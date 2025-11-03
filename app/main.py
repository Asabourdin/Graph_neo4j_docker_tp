import os
from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
from typing import List, Dict, Any
import psycopg2

app = FastAPI(title="E-Commerce Recommendation API", version="1.0.0")

# Configuration
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "app"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
    "database": os.getenv("POSTGRES_DB", "shop")
}

# Neo4j driver
neo4j_driver = None

def get_neo4j_driver():
    global neo4j_driver
    if neo4j_driver is None:
        neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return neo4j_driver


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        # Check Neo4j
        driver = get_neo4j_driver()
        with driver.session() as session:
            session.run("RETURN 1")
        
        # Check PostgreSQL
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.close()
        
        return {"ok": True, "status": "healthy"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/stats")
def get_stats():
    """Get database statistics."""
    driver = get_neo4j_driver()
    
    with driver.session() as session:
        customers = session.run("MATCH (c:Customer) RETURN count(c) AS count").single()["count"]
        products = session.run("MATCH (p:Product) RETURN count(p) AS count").single()["count"]
        orders = session.run("MATCH (o:Order) RETURN count(o) AS count").single()["count"]
        categories = session.run("MATCH (c:Category) RETURN count(c) AS count").single()["count"]
        
        return {
            "customers": customers,
            "products": products,
            "orders": orders,
            "categories": categories
        }


@app.get("/products")
def list_products():
    """List all products with their categories."""
    driver = get_neo4j_driver()
    
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Product)-[:IN_CATEGORY]->(c:Category)
            RETURN p.id AS id, p.name AS name, p.price AS price, 
                   c.name AS category
            ORDER BY p.name
        """)
        
        products = []
        for record in result:
            products.append({
                "id": record["id"],
                "name": record["name"],
                "price": float(record["price"]),
                "category": record["category"]
            })
        
        return {"products": products}


@app.get("/customers/{customer_id}/orders")
def customer_orders(customer_id: str):
    """Get all orders for a customer."""
    driver = get_neo4j_driver()
    
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Customer {id: $customer_id})-[:PLACED]->(o:Order)-[r:CONTAINS]->(p:Product)
            RETURN o.id AS order_id, o.ts AS order_date, 
                   p.id AS product_id, p.name AS product_name, 
                   r.quantity AS quantity, p.price AS price
            ORDER BY o.ts DESC
        """, {"customer_id": customer_id})
        
        orders = {}
        for record in result:
            order_id = record["order_id"]
            if order_id not in orders:
                orders[order_id] = {
                    "order_id": order_id,
                    "order_date": str(record["order_date"]),
                    "items": []
                }
            
            orders[order_id]["items"].append({
                "product_id": record["product_id"],
                "product_name": record["product_name"],
                "quantity": record["quantity"],
                "price": float(record["price"])
            })
        
        return {"orders": list(orders.values())}


@app.get("/recommendations/collaborative/{customer_id}")
def collaborative_filtering(customer_id: str, limit: int = 5):
    """
    Collaborative filtering: recommend products bought by similar customers.
    
    Strategy: Find customers who bought similar products, then recommend 
    products they bought that this customer hasn't purchased yet.
    """
    driver = get_neo4j_driver()
    
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Customer {id: $customer_id})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
            WITH c, collect(p) AS customer_products
            
            MATCH (other:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
            WHERE other <> c AND p IN customer_products
            WITH c, other, count(p) AS common_products, customer_products
            ORDER BY common_products DESC
            LIMIT 10
            
            MATCH (other)-[:PLACED]->(:Order)-[:CONTAINS]->(rec:Product)
            WHERE NOT rec IN customer_products
            WITH rec, count(DISTINCT other) AS score
            ORDER BY score DESC
            LIMIT $limit
            
            MATCH (rec)-[:IN_CATEGORY]->(cat:Category)
            RETURN rec.id AS product_id, rec.name AS product_name, 
                   rec.price AS price, cat.name AS category, score
        """, {"customer_id": customer_id, "limit": limit})
        
        recommendations = []
        for record in result:
            recommendations.append({
                "product_id": record["product_id"],
                "product_name": record["product_name"],
                "price": float(record["price"]),
                "category": record["category"],
                "score": record["score"]
            })
        
        return {"customer_id": customer_id, "recommendations": recommendations}


@app.get("/recommendations/content/{customer_id}")
def content_based(customer_id: str, limit: int = 5):
    """
    Content-based filtering: recommend products from categories the customer likes.
    
    Strategy: Find categories of products the customer has bought or viewed,
    then recommend other products from those categories.
    """
    driver = get_neo4j_driver()
    
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Customer {id: $customer_id})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)-[:IN_CATEGORY]->(cat:Category)
            WITH c, collect(DISTINCT cat) AS liked_categories
            
            MATCH (rec:Product)-[:IN_CATEGORY]->(cat:Category)
            WHERE cat IN liked_categories
            AND NOT EXISTS {
                MATCH (c)-[:PLACED]->(:Order)-[:CONTAINS]->(rec)
            }
            WITH rec, cat, count(*) AS relevance
            ORDER BY relevance DESC
            LIMIT $limit
            
            RETURN rec.id AS product_id, rec.name AS product_name, 
                   rec.price AS price, cat.name AS category
        """, {"customer_id": customer_id, "limit": limit})
        
        recommendations = []
        for record in result:
            recommendations.append({
                "product_id": record["product_id"],
                "product_name": record["product_name"],
                "price": float(record["price"]),
                "category": record["category"]
            })
        
        return {"customer_id": customer_id, "recommendations": recommendations}


@app.get("/recommendations/popular")
def popular_products(limit: int = 5):
    """
    Popularity-based recommendations: most frequently purchased products.
    """
    driver = get_neo4j_driver()
    
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Product)<-[r:CONTAINS]-(:Order)
            WITH p, sum(r.quantity) AS total_sold
            ORDER BY total_sold DESC
            LIMIT $limit
            
            MATCH (p)-[:IN_CATEGORY]->(cat:Category)
            RETURN p.id AS product_id, p.name AS product_name, 
                   p.price AS price, cat.name AS category, total_sold
        """, {"limit": limit})
        
        recommendations = []
        for record in result:
            recommendations.append({
                "product_id": record["product_id"],
                "product_name": record["product_name"],
                "price": float(record["price"]),
                "category": record["category"],
                "total_sold": record["total_sold"]
            })
        
        return {"recommendations": recommendations}


@app.get("/recommendations/frequently-bought-together/{product_id}")
def frequently_bought_together(product_id: str, limit: int = 5):
    """
    Co-occurrence recommendations: products frequently bought together.
    
    Strategy: Find products that appear in the same orders as the given product.
    """
    driver = get_neo4j_driver()
    
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Product {id: $product_id})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(rec:Product)
            WHERE rec <> p
            WITH rec, count(DISTINCT o) AS co_occurrences
            ORDER BY co_occurrences DESC
            LIMIT $limit
            
            MATCH (rec)-[:IN_CATEGORY]->(cat:Category)
            RETURN rec.id AS product_id, rec.name AS product_name, 
                   rec.price AS price, cat.name AS category, co_occurrences
        """, {"product_id": product_id, "limit": limit})
        
        recommendations = []
        for record in result:
            recommendations.append({
                "product_id": record["product_id"],
                "product_name": record["product_name"],
                "price": float(record["price"]),
                "category": record["category"],
                "co_occurrences": record["co_occurrences"]
            })
        
        return {"product_id": product_id, "recommendations": recommendations}


@app.on_event("shutdown")
def shutdown_event():
    """Close Neo4j driver on shutdown."""
    if neo4j_driver:
        neo4j_driver.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)