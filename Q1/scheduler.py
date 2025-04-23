import schedule
import time
import asyncio
import datetime
import os
import signal
import json
import logging
from logging.handlers import RotatingFileHandler
from data_processor import ApiConfig, process_apis, setup_logger

logger = setup_logger('scheduler', 'scheduler.log')

running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received, stopping scheduler...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def load_api_configs():
    try:
        if os.path.exists('api_config.json'):
            with open('api_config.json', 'r') as f:
                configs = json.load(f)
                return [ApiConfig(**config) for config in configs]
        else:
            return [
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
    except Exception as e:
        logger.error(f"Error loading API configurations: {str(e)}")
        return []

def run_data_processing_job():
    logger.info("Starting scheduled data processing job")
    apis = load_api_configs()
    
    if not apis:
        logger.error("No API configurations found, skipping job")
        return
    
    try:
        asyncio.run(process_apis(apis))
        logger.info("Data processing job completed successfully")
    except Exception as e:
        logger.error(f"Data processing job failed: {str(e)}")

def schedule_jobs(interval_minutes=60):
    logger.info(f"Scheduling data processing job to run every {interval_minutes} minutes")
    schedule.every(interval_minutes).minutes.do(run_data_processing_job)
    
    logger.info("Running initial data processing job")
    run_data_processing_job()

def run_scheduler():
    global running
    
    logger.info("Starting scheduler service")
    
    schedule_jobs()
    
    while running:
        schedule.run_pending()
        time.sleep(1)
    
    logger.info("Scheduler service shut down")

if __name__ == "__main__":
    run_scheduler()