import os
import time
from pathlib import Path
import psycopg2
from neo4j import GraphDatabase
import pandas as pd

# Configuration from environment variables
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "app"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
    "database": os.getenv("POSTGRES_DB", "shop")
}

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


def wait_for_postgres(max_retries=30, delay=2):
    """Wait for PostgreSQL to be ready."""
    print("⏳ Waiting for PostgreSQL...")
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            conn.close()
            print("PostgreSQL is ready!")
            return True
        except psycopg2.OperationalError:
            if i < max_retries - 1:
                time.sleep(delay)
            else:
                raise Exception("PostgreSQL not available after max retries")
    return False


def wait_for_neo4j(max_retries=30, delay=2):
    """Wait for Neo4j to be ready."""
    print("Waiting for Neo4j...")
    for i in range(max_retries):
        try:
            print('1')
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            print('2')
            with driver.session() as session:
                print('3')
                session.run("RETURN 1")
                print('4')
            driver.close()
            print("Neo4j is ready!")
            return True
        except Exception as e:
            if i < max_retries - 1:
                time.sleep(delay)
            else:
                raise Exception(f"Neo4j not available after max retries: {e}")
    return False


def run_cypher(driver, query, params=None):
    """Execute single cypher query."""
    with driver.session() as session:
        result = session.run(query, params or {})
        return result.consume()


def run_cypher_file(driver, filepath):
    """Execute multiple cypher statements from a file."""
    print(f"Running Cypher file: {filepath}")
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Split by semicolon and filter empty statements
    statements = [s.strip() for s in content.split(';') if s.strip()]
    
    for stmt in statements:
        try:
            run_cypher(driver, stmt)
            print(f"Executed: {stmt[:50]}...")
        except Exception as e:
            print(f"Warning executing statement: {e}")


def chunk(df, size=500):
    """Split DataFrame into chunks for batch processing."""
    for i in range(0, len(df), size):
        yield df.iloc[i:i + size]


def etl():
    """Main etl function to load data from postgre to neo4j"""
    # Dependencies
    wait_for_postgres()
    wait_for_neo4j()

    # Path to Cypher schema file
    queries_path = Path(__file__).with_name("queries.cypher")

    # Connect to db
    print("Connecting to databases...")
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        # 1. Setup Neo4j schema (constraints, indexes)
        if queries_path.exists():
            run_cypher_file(neo4j_driver, queries_path)
        else:
            print("queries.cypher not found, skipping schema setup")

        # 2. Extract data from PostgreSQL
        print("\nExtracting data from PostgreSQL...")
        
        categories_df = pd.read_sql("SELECT * FROM categories", pg_conn)
        print(f"  - Categories: {len(categories_df)} rows")
        
        products_df = pd.read_sql("SELECT * FROM products", pg_conn)
        print(f"  - Products: {len(products_df)} rows")
        
        customers_df = pd.read_sql("SELECT * FROM customers", pg_conn)
        print(f"  - Customers: {len(customers_df)} rows")
        
        orders_df = pd.read_sql("SELECT * FROM orders", pg_conn)
        print(f"  - Orders: {len(orders_df)} rows")
        
        order_items_df = pd.read_sql("SELECT * FROM order_items", pg_conn)
        print(f"  - Order Items: {len(order_items_df)} rows")
        
        events_df = pd.read_sql("SELECT * FROM events", pg_conn)
        print(f"  - Events: {len(events_df)} rows")

        # 3. Load data into Neo4j
        print("\nLoading data into Neo4j...")
        
        # Load Categories
        print("Loading Categories...")
        for _, row in categories_df.iterrows():
            run_cypher(neo4j_driver, """
                MERGE (c:Category {id: $id})
                SET c.name = $name
            """, {"id": row['id'], "name": row['name']})
        
        # Load Products with relationships to Categories
        print("Loading Products...")
        for _, row in products_df.iterrows():
            run_cypher(neo4j_driver, """
                MERGE (p:Product {id: $id})
                SET p.name = $name, p.price = $price
                WITH p
                MATCH (c:Category {id: $category_id})
                MERGE (p)-[:IN_CATEGORY]->(c)
            """, {
                "id": row['id'],
                "name": row['name'],
                "price": float(row['price']),
                "category_id": row['category_id']
            })
        
        # Load Customers
        print("Loading Customers...")
        for _, row in customers_df.iterrows():
            run_cypher(neo4j_driver, """
                MERGE (c:Customer {id: $id})
                SET c.name = $name, c.join_date = date($join_date)
            """, {
                "id": row['id'],
                "name": row['name'],
                "join_date": str(row['join_date'])
            })
        
        # Load Orders with relationships to Customers
        print("Loading Orders...")
        for _, row in orders_df.iterrows():
            # ensure proper date format (since we had issues beforehand)
            ts_iso = row['ts'].isoformat()
            run_cypher(neo4j_driver, """
                MERGE (o:Order {id: $id})
                SET o.ts = datetime($ts)
                WITH o
                MATCH (c:Customer {id: $customer_id})
                MERGE (c)-[:PLACED]->(o)
            """, {
                "id": row['id'],
                "customer_id": row['customer_id'],
                "ts": ts_iso
            })
        
        # Load Order Items (Order-Product relationships)
        print("Loading Order Items...")
        for _, row in order_items_df.iterrows():
            run_cypher(neo4j_driver, """
                MATCH (o:Order {id: $order_id})
                MATCH (p:Product {id: $product_id})
                MERGE (o)-[r:CONTAINS]->(p)
                SET r.quantity = $quantity
            """, {
                "order_id": row['order_id'],
                "product_id": row['product_id'],
                "quantity": int(row['quantity'])
            })
        
        # Load Events (Customer-Product interactions)
        print("Loading Events...")
        for _, row in events_df.iterrows():
            event_type = row['event_type'].upper()  # VIEW, CLICK, ADD_TO_CART
            rel_type = event_type  # good enough

            ts_iso = row['ts'].isoformat()  # 👈 same fix here

            run_cypher(neo4j_driver, f"""
                MATCH (c:Customer {{id: $customer_id}})
                MATCH (p:Product {{id: $product_id}})
                MERGE (c)-[r:{rel_type}]->(p)
                SET r.ts = datetime($ts)
            """, {
                "customer_id": row['customer_id'],
                "product_id": row['product_id'],
                "ts": ts_iso
            })

        print("\nETL done !")

    finally:
        pg_conn.close()
        neo4j_driver.close()


if __name__ == "__main__":
    etl()
