"""
Celery application configuration.
"""

import os
from celery import Celery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create Celery app
app = Celery('finance_data_pipeline')

# Configure Celery
app.conf.update(
    broker_url=os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0'),
    result_backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/0'),
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task routing for parallel processing
    task_routes={
        # Discovery tasks go to dedicated discovery worker
        'app.tasks.discover_and_download_ticker': {'queue': 'discovery'},
        'app.tasks.discover_universe': {'queue': 'discovery'},
        
        # Download tasks are distributed across 5 download workers
        'app.tasks.download_single_timeframe': {'queue': 'download'},
        'app.tasks.backfill_symbol': {'queue': 'download'},
        
        # Control tasks
        'app.tasks.bulk_enqueue_backfills': {'queue': 'default'},
    },
    
    # Worker configuration
    worker_max_tasks_per_child=100,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    
    # Task result expiry
    result_expires=3600,
    
    # Retry configuration
    task_default_retry_delay=60,
    task_max_retries=3,
)

# Auto-discover tasks
app.autodiscover_tasks(['app'])
