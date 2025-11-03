#!/bin/bash

# Install dependencies
pip install --no-cache-dir -r requirements.txt

# Start FastAPI server with auto-reload
uvicorn main:app --host 0.0.0.0 --port 8000 --reload