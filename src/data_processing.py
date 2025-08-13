from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from typing import Dict, Any
from .utils import get_logger

logger = get_logger(__name__)

class MTADataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def get_schema(self, feed_type: str) -> StructType:
        """Define schema for different feed types"""
        if feed_type == "subway":
            return StructType([
                StructField("trip_id", StringType(), True),
                StructField("route_id", StringType(), True),
                StructField("vehicle_id", StringType(), True),
                StructField("current_stop_sequence", IntegerType(), True),
                StructField("current_status", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("stop_id", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("bearing", DoubleType(), True),
                StructField("speed", DoubleType(), True)
            ])
        else:
            # Default schema for other feed types
            return StructType([
                StructField("id", StringType(), True),
                StructField("data", StringType(), True),
                StructField("timestamp", TimestampType(), True)
            ])
    
    def process_feed(self, feed_data: Dict[str, Any], feed_name: str) -> DataFrame:
        """Process a single feed's data"""
        schema = self.get_schema(feed_name)
        
        # Convert to Spark DataFrame
        df = self.spark.createDataFrame(
            [feed_data],
            schema=StructType([
                StructField("data", StringType(), True),
                StructField("timestamp", StringType(), True)
            ])
        )
        
        # Parse JSON data
        df = df.withColumn("parsed_data", from_json(col("data"), schema))
        
        # Explode the data
        df = df.select(
            col("timestamp"),
            col("parsed_data.*")
        )
        
        # Add feed type as a column
        df = df.withColumn("feed_type", lit(feed_name))
        
        logger.info(f"Processed {df.count()} records from {feed_name} feed")
        return df
    
    def process_all_feeds(self, all_feeds_data: Dict[str, Any]) -> DataFrame:
        """Process all feeds and union them"""
        dfs = []
        for feed_name, feed_data in all_feeds_data.items():
            try:
                df = self.process_feed(feed_data['data'], feed_name)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error processing feed {feed_name}: {e}")
                continue
        
        if not dfs:
            raise ValueError("No feeds were processed successfully")
        
        # Union all DataFrames
        result_df = dfs[0]
        for df in dfs[1:]:
            result_df = result_df.unionByName(df, allowMissingColumns=True)
        
        return result_df
