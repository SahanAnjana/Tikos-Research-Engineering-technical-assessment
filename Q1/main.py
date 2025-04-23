from fastapi import FastAPI, HTTPException, Depends, Query, Path, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from sqlalchemy import create_engine, text, MetaData, Table, select, inspect
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel, Field
import pandas as pd
import os
import json
import logging
from typing import Dict, List, Optional, Any
from data_processor import (
    setup_logger, get_env_var, get_db_engine, health_check,
    DATABASE_URL, ApiConfig, process_apis
)
import asyncio
import uvicorn
import time
from datetime import datetime
from prometheus_client import Counter, Histogram

logger = setup_logger('api_server', 'logs/api_server.log')

REQUEST_COUNT = Counter('api_requests_total', 'Total number of API requests', ['endpoint', 'method', 'status'])
REQUEST_LATENCY = Histogram('api_request_latency_seconds', 'API request latency', ['endpoint'])

app = FastAPI(
    title="Data Processing API",
    description="API for accessing processed data from various sources",
    version="1.0.0"
)

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

API_KEY = get_env_var("API_KEY", "")

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_env_var("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    
    endpoint = request.url.path
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        
        REQUEST_COUNT.labels(
            endpoint=endpoint,
            method=request.method,
            status=response.status_code
        ).inc()
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(process_time)
        
        return response
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"Request error: {str(e)}")
        
        REQUEST_COUNT.labels(
            endpoint=endpoint,
            method=request.method,
            status=500
        ).inc()
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(process_time)
        
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )

async def verify_api_key(api_key: str = Depends(api_key_header)):
    if not API_KEY or api_key == API_KEY:
        return api_key
    raise HTTPException(status_code=401, detail="Invalid API key")

@app.get("/", tags=["Info"])
async def root():
    return {
        "name": "Data Processing API",
        "version": "1.0.0",
        "status": "active",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health", tags=["Health"])
async def check_health():
    """Health check endpoint."""
    return health_check()

@app.get("/tables", dependencies=[Depends(verify_api_key)], tags=["Data"])
async def list_tables():
    try:
        with get_db_engine() as engine:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            result = {}
            for table in tables:
                try:
                    with engine.connect() as conn:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                        result[table] = {
                            "row_count": count,
                            "columns": [col["name"] for col in inspector.get_columns(table)]
                        }
                except SQLAlchemyError as e:
                    logger.error(f"Error getting info for table {table}: {str(e)}")
                    result[table] = {"error": str(e)}
            
            return {"tables": result}
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/data/{table_name}", dependencies=[Depends(verify_api_key)], tags=["Data"])
async def get_table_data(
    table_name: str = Path(..., description="Name of the table to query"),
    limit: int = Query(100, description="Number of records to return", ge=1, le=1000),
    offset: int = Query(0, description="Number of records to skip", ge=0),
    sort_by: Optional[str] = Query(None, description="Column to sort by"),
    sort_order: str = Query("asc", description="Sort order (asc or desc)")
):
    try:
        with get_db_engine() as engine:
            inspector = inspect(engine)
            
            if table_name not in inspector.get_table_names():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
            
            valid_columns = [col["name"] for col in inspector.get_columns(table_name)]
            if sort_by and sort_by not in valid_columns:
                raise HTTPException(status_code=400, detail=f"Sort column '{sort_by}' not found")

            query = f"SELECT * FROM {table_name}"
            if sort_by:
                sort_direction = "ASC" if sort_order.lower() == "asc" else "DESC"
                query += f" ORDER BY {sort_by} {sort_direction}"
                
            query += f" LIMIT {limit} OFFSET {offset}"
            
            with engine.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                
                total_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                
            column_names = result.keys()
            data = []
            for row in rows:
                item = dict(zip(column_names, row))
                
                for key, value in item.items():
                    if isinstance(value, str) and (
                        (value.startswith('{') and value.endswith('}')) or 
                        (value.startswith('[') and value.endswith(']'))
                    ):
                        try:
                            item[key] = json.loads(value)
                        except json.JSONDecodeError:
                            pass
                            
                data.append(item)
            
            return {
                "data": data,
                "metadata": {
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "sort_by": sort_by,
                    "sort_order": sort_order
                }
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data for table {table_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

class ProcessRequest(BaseModel):
    apis: List[Dict[str, Any]] = Field(..., description="List of API configurations to process")

@app.post("/process", dependencies=[Depends(verify_api_key)], tags=["Processing"])
async def trigger_processing(request: ProcessRequest):
    try:
        api_configs = []
        for api_dict in request.apis:
            try:
                api_configs.append(ApiConfig(**api_dict))
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid API configuration: {str(e)}")
        
        task = asyncio.create_task(process_apis(api_configs))
        
        return {
            "status": "processing_started",
            "message": f"Processing started for {len(api_configs)} APIs",
            "apis": [api.label for api in api_configs]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering processing: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")

