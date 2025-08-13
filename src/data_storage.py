from pyspark.sql import DataFrame, SparkSession
from typing import Optional
from .utils import get_logger

logger = get_logger(__name__)

class DataStorage:
    def __init__(self, spark: SparkSession, storage_path: str = "data/nyc_metro"):
        self.spark = spark
        self.storage_path = storage_path
        
    def save_as_parquet(self, df: DataFrame, mode: str = "append", partition_by: Optional[list] = None):
        """Save DataFrame as Parquet files"""
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
            
        writer.parquet(self.storage_path)
        logger.info(f"Data saved to {self.storage_path} as Parquet")
        
    def save_as_delta(self, df: DataFrame, mode: str = "append", partition_by: Optional[list] = None):
        """Save DataFrame as Delta Lake format"""
        try:
            writer = df.write.format("delta").mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
                
            writer.save(self.storage_path)
            logger.info(f"Data saved to {self.storage_path} as Delta")
        except Exception as e:
            logger.error(f"Error saving Delta table: {e}")
            raise
            
    def save_to_database(self, df: DataFrame, table_name: str, mode: str = "append"):
        """Save DataFrame to a JDBC database"""
        # This would require additional JDBC configuration
        pass
