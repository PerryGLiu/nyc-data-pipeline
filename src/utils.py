import yaml
import os
from dotenv import load_dotenv
import logging
from typing import Dict, Any

def load_config(config_path: str) -> Dict[str, Any]:
    """Load YAML configuration file"""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Load environment variables
    load_dotenv()
    
    # Replace any ${VAR} with environment variables
    def replace_env_vars(data):
        if isinstance(data, dict):
            return {k: replace_env_vars(v) for k, v in data.items()}
        elif isinstance(data, str) and data.startswith('${') and data.endswith('}'):
            return os.getenv(data[2:-1], data)
        return data
    
    return replace_env_vars(config)

def get_logger(name: str) -> logging.Logger:
    """Get a configured logger"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(name)

def create_spark_session(config_path: str = "config/spark_config.yaml") -> 'SparkSession':
    """Create and configure a Spark session"""
    config = load_config(config_path)['spark']
    
    spark_builder = SparkSession.builder \
        .appName(config['app_name']) \
        .master(config['master'])
    
    for key, value in config['config'].items():
        spark_builder.config(key, value)
    
    return spark_builder.getOrCreate()
