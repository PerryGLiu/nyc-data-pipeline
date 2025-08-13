from src.data_ingestion import MTADataFetcher
from src.data_processing import MTADataProcessor
from src.data_storage import DataStorage
from src.utils import create_spark_session, get_logger
import schedule
import time

logger = get_logger(__name__)

def run_pipeline():
    """Main pipeline execution function"""
    try:
        # Initialize components
        spark = create_spark_session()
        fetcher = MTADataFetcher()
        processor = MTADataProcessor(spark)
        storage = DataStorage(spark)
        
        # Fetch data
        logger.info("Fetching data from MTA API...")
        feeds_data = fetcher.fetch_all_feeds()
        
        if not feeds_data:
            logger.warning("No data fetched from any feed")
            return
            
        # Process data
        logger.info("Processing data...")
        processed_df = processor.process_all_feeds(feeds_data)
        
        # Store data
        logger.info("Storing data...")
        storage.save_as_parquet(processed_df, partition_by=["feed_type", "route_id"])
        # Alternatively: storage.save_as_delta(processed_df, partition_by=["feed_type", "route_id"])
        
        logger.info("Pipeline execution completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    # Run immediately
    run_pipeline()
    
    # Schedule to run periodically
    schedule.every(5).minutes.do(run_pipeline)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
