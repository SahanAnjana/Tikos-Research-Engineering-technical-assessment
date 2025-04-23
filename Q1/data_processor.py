import aiohttp
import asyncio
import pandas as pd
import json
import time
import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Any, Optional, Tuple, Union
import backoff
import concurrent.futures
from functools import partial
import traceback
import contextlib
from prometheus_client import Counter, Histogram, start_http_server
import socket

# Metrics for monitoring
FETCH_COUNT = Counter('api_fetch_total', 'Total number of API fetch operations', ['api', 'status'])
TRANSFORM_TIME = Histogram('transform_processing_seconds', 'Time spent transforming data', ['api'])
STORAGE_TIME = Histogram('storage_processing_seconds', 'Time spent storing data', ['api', 'status'])
ROWS_PROCESSED = Counter('rows_processed_total', 'Total number of rows processed', ['api'])

def setup_logger(name, log_file, level=logging.INFO):
    """Configure a logger with file and console handlers."""
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Ensure the log directory exists
    os.makedirs(os.path.dirname(log_file) if os.path.dirname(log_file) else '.', exist_ok=True)
    
    handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates on reloads
    if logger.hasHandlers():
        logger.handlers.clear()
        
    logger.addHandler(handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger('data_processor', 'logs/data_processor.log')

# Load environment variables safely
load_dotenv()

def get_env_var(var_name, default=None, required=False):
    """Get environment variable with validation."""
    value = os.getenv(var_name, default)
    if required and not value:
        raise EnvironmentError(f"Required environment variable '{var_name}' is missing")
    return value

MYSQL_HOST = get_env_var("MYSQL_HOST", "localhost")
MYSQL_USERNAME = get_env_var("MYSQL_USERNAME", required=True)
MYSQL_PASSWORD = get_env_var("MYSQL_PASSWORD", "")
MYSQL_DB = get_env_var("MYSQL_DB", "data_processing")
MYSQL_PORT = get_env_var("MYSQL_PORT", "3306")
API_RATE_LIMIT = int(get_env_var("API_RATE_LIMIT", "10"))  # Requests per second
WORKER_THREADS = int(get_env_var("WORKER_THREADS", "4"))
METRICS_PORT = int(get_env_var("METRICS_PORT", "8000"))

# Database connection string
DATABASE_URL = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

# Rate limiter semaphore
api_rate_limiter = None

@contextlib.contextmanager
def get_db_engine():
    """Context manager for database engine to ensure proper cleanup."""
    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=10,
        max_overflow=20,
        connect_args={"connect_timeout": 30}
    )
    try:
        yield engine
    finally:
        engine.dispose()

def initialize_database():
    """Create the database if it doesn't exist."""
    try:
        base_url = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/"
        base_engine = create_engine(base_url)
        
        with base_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}"))
            logger.info(f"Database {MYSQL_DB} initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise
    finally:
        if 'base_engine' in locals():
            base_engine.dispose()

class ApiConfig:
    """Configuration for API endpoints."""
    def __init__(self, url: str, api_type: str, query: Optional[str] = None, 
                 label: Optional[str] = None, params: Optional[Dict] = None,
                 headers: Optional[Dict] = None, table_name: Optional[str] = None,
                 retry_attempts: int = 3, timeout: int = 30):
        self.url = url
        self.type = api_type  # REST or GraphQL
        self.query = query    # GraphQL query
        self.params = params or {}
        self.headers = headers or {}
        self.label = label or url.split('//')[-1].split('/')[0]
        self.table_name = table_name or self.label.lower().replace('.', '_').replace('-', '_')
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        
        # Validate inputs
        if not url:
            raise ValueError("URL is required for ApiConfig")
        if api_type not in ["REST", "GraphQL"]:
            raise ValueError("api_type must be 'REST' or 'GraphQL'")
        if api_type == "GraphQL" and not query:
            raise ValueError("Query is required for GraphQL API")

    def __str__(self):
        return f"ApiConfig({self.label}, {self.type})"

async def init_rate_limiter(limit_per_second):
    """Initialize a rate limiter based on configured limits."""
    global api_rate_limiter
    api_rate_limiter = asyncio.Semaphore(limit_per_second)

@backoff.on_exception(
    backoff.expo,
    (aiohttp.ClientError, asyncio.TimeoutError),
    max_tries=3,
    jitter=backoff.full_jitter
)
async def fetch_data(session: aiohttp.ClientSession, api: ApiConfig) -> Tuple[ApiConfig, Any]:
    """Fetch data from API with rate limiting and retries."""
    global api_rate_limiter
    start_time = time.time()
    logger.info(f"Fetching data from {api.url}")
    
    try:
        # Use rate limiter if available
        if api_rate_limiter:
            async with api_rate_limiter:
                return await _do_fetch(session, api)
        else:
            return await _do_fetch(session, api)
    except Exception as e:
        FETCH_COUNT.labels(api=api.label, status='error').inc()
        logger.error(f"Failed to fetch data from {api.url}: {str(e)}")
        return api, None

async def _do_fetch(session, api):
    """Internal function to perform the actual API fetch."""
    start_time = time.time()
    
    if api.type == "REST":
        async with session.get(
            api.url, 
            params=api.params,
            headers=api.headers,
            timeout=api.timeout
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"API error {response.status} from {api.url}: {error_text}")
                FETCH_COUNT.labels(api=api.label, status='error').inc()
                return api, None
            
            data = await response.json()
            logger.info(f"Successfully fetched data from {api.url} in {time.time() - start_time:.2f}s")
            FETCH_COUNT.labels(api=api.label, status='success').inc()
            return api, data
            
    elif api.type == "GraphQL":
        async with session.post(
            api.url, 
            json={"query": api.query},
            headers=api.headers,
            timeout=api.timeout
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"GraphQL API error {response.status} from {api.url}: {error_text}")
                FETCH_COUNT.labels(api=api.label, status='error').inc()
                return api, None
            
            data = await response.json()
            logger.info(f"Successfully fetched GraphQL data from {api.url} in {time.time() - start_time:.2f}s")
            FETCH_COUNT.labels(api=api.label, status='success').inc()
            return api, data

def transform_data(raw_data: Any, api_config: ApiConfig) -> List[Dict]:
    """Transform raw API data into a consistent format."""
    with TRANSFORM_TIME.labels(api=api_config.label).time():
        if not raw_data:
            logger.warning(f"No data to transform for {api_config.url}")
            return []
        
        try:
            # Special handling for GraphQL responses
            if api_config.type == "GraphQL":
                if 'data' in raw_data:
                    if 'characters' in raw_data.get('data', {}):
                        raw_data = raw_data['data']['characters']['results']
                    elif 'countries' in raw_data.get('data', {}):
                        raw_data = raw_data['data']['countries']
                    else:
                        first_key = next(iter(raw_data['data']), None)
                        if first_key:
                            raw_data = raw_data['data'][first_key]
            
            # Ensure we have a list of data
            if not isinstance(raw_data, (list, dict)):
                logger.error(f"Unexpected data format for {api_config.url}: {type(raw_data)}")
                return []
            
            # Extract nested results if needed
            if isinstance(raw_data, dict):
                for key in ['results', 'items', 'data']:
                    if key in raw_data and isinstance(raw_data[key], list):
                        raw_data = raw_data[key]
                        break
                else:
                    # If no recognized list field, treat the dict as a single item
                    raw_data = [raw_data]
            
            # Use pandas for normalization
            df = pd.json_normalize(raw_data)
            
            # Replace NaN with None for DB compatibility
            df = df.where(pd.notnull(df), None)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"Error transforming data for {api_config.url}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []

def store_data(data: List[Dict], api_config: ApiConfig, engine) -> bool:
    """Store transformed data to database with chunking for large datasets."""
    with STORAGE_TIME.labels(api=api_config.label, status='processing').time():
        if not data:
            logger.info(f"Skipping storage for empty table: {api_config.table_name}")
            return False
        
        try:
            df = pd.DataFrame(data)
            
            # Convert complex types to JSON strings
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)
            
            inspector = inspect(engine)
            
            # Use replace for first run, append for subsequent chunks
            if_exists = 'replace' if api_config.table_name in inspector.get_table_names() else 'append'
            
            # Process in chunks to handle large datasets
            chunk_size = 1000
            total_rows = len(df)
            
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                chunk.to_sql(
                    name=api_config.table_name,
                    con=engine,
                    if_exists='append' if i > 0 else if_exists,
                    index=False,
                    method='multi'
                )
                logger.info(f"Stored chunk {i//chunk_size + 1}/{(total_rows-1)//chunk_size + 1} " +
                           f"({len(chunk)} rows) in table '{api_config.table_name}'")
            
            # Update metrics
            ROWS_PROCESSED.labels(api=api_config.label).inc(total_rows)
            STORAGE_TIME.labels(api=api_config.label, status='success').observe(0)
            return True
        except Exception as e:
            logger.error(f"Storage failed for {api_config.table_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            STORAGE_TIME.labels(api=api_config.label, status='error').observe(0)
            return False

def process_data_in_thread_pool(api_config: ApiConfig, raw_data: Any) -> int:
    """Process data in a thread pool for parallel execution."""
    logger.info(f"Processing data for {api_config.table_name}")
    
    try:
        transformed_data = transform_data(raw_data, api_config)
        if transformed_data:
            with get_db_engine() as engine:
                success = store_data(transformed_data, api_config, engine)
                if success:
                    return len(transformed_data)
        return 0
    except Exception as e:
        logger.error(f"Error in data processing thread for {api_config.table_name}: {str(e)}")
        return 0

async def process_apis(apis: List[ApiConfig]) -> Dict[str, int]:
    """Process a list of APIs, fetching and storing their data."""
    logger.info(f"Starting data processing for {len(apis)} APIs")
    
    # Initialize the database if needed
    initialize_database()
    
    # Initialize rate limiter
    await init_rate_limiter(API_RATE_LIMIT)
    
    # Configure HTTP client
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    
    # Track results for reporting
    results = {}
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # Fetch all APIs in parallel
        tasks = [fetch_data(session, api) for api in apis]
        api_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process the results in a thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
            futures = []
            
            for result in api_results:
                if isinstance(result, Exception):
                    logger.error(f"API fetch failed with exception: {str(result)}")
                    continue
                    
                api_config, raw_data = result
                if raw_data:
                    future = executor.submit(
                        process_data_in_thread_pool,
                        api_config,
                        raw_data
                    )
                    futures.append((api_config, future))
            
            # Collect results as they complete
            for api_config, future in futures:
                try:
                    row_count = future.result()
                    results[api_config.label] = row_count
                except Exception as e:
                    logger.error(f"Processing failed for {api_config.label}: {str(e)}")
                    results[api_config.label] = 0
            
            total_rows = sum(results.values())
            logger.info(f"Completed processing {total_rows} rows across all APIs")
    
    return results

def start_metrics_server():
    """Start a Prometheus metrics server."""
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Metrics server started on port {METRICS_PORT}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {str(e)}")

def health_check() -> Dict[str, Any]:
    """Check system health status."""
    health_status = {
        "status": "healthy",
        "timestamp": time.time(),
        "hostname": socket.gethostname(),
        "checks": {}
    }
    
    # Check database connectivity
    try:
        with get_db_engine() as engine:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                health_status["checks"]["database"] = "connected" if result else "error"
    except Exception as e:
        health_status["checks"]["database"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    return health_status

if __name__ == "__main__":
    # Example usage when run directly
    start_metrics_server()
    
    apis = [
        ApiConfig(
            url="https://jsonplaceholder.typicode.com/posts",
            api_type="REST",
            label="posts"
        ),
        ApiConfig(
            url="https://api.openbrewerydb.org/v1/breweries",
            api_type="REST",
            label="breweries"
        ),
        ApiConfig(
            url="https://rickandmortyapi.com/graphql",
            api_type="GraphQL",
            query="""query {
                characters(page: 1, filter: { name: "rick" }) {
                    info {
                    count
                    }
                    results {
                    name
                    status
                    species
                    }
                }
                }""",
            label="rick_and_morty_characters"
        ),
        ApiConfig(
            url="https://countries.trevorblades.com/graphql",
            api_type="GraphQL",
            query="query { countries { name capital currency } }",
            label="countries"
        )
    ]
    
    asyncio.run(process_apis(apis))