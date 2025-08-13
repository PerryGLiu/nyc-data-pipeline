import requests
import json
from datetime import datetime
import pytz
from typing import Dict, Any
from .utils import load_config, get_logger

logger = get_logger(__name__)

class MTADataFetcher:
    def __init__(self, config_path: str = "config/api_config.yaml"):
        self.config = load_config(config_path)['mta_api']
        self.base_url = self.config['base_url']
        self.api_key = self.config['api_key']
        self.feeds = self.config['feeds']
        
    def fetch_feed(self, feed_id: str) -> Dict[str, Any]:
        """Fetch data from a specific MTA feed"""
        url = f"{self.base_url}{feed_id}"
        headers = {"x-api-key": self.api_key}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching feed {feed_id}: {e}")
            raise

    def fetch_all_feeds(self) -> Dict[str, Any]:
        """Fetch data from all configured feeds"""
        result = {}
        for feed in self.feeds:
            try:
                data = self.fetch_feed(feed['id'])
                result[feed['name']] = {
                    'data': data,
                    'timestamp': datetime.now(pytz.timezone('America/New_York')).isoformat()
                }
                logger.info(f"Successfully fetched feed: {feed['name']}")
            except Exception as e:
                logger.error(f"Failed to fetch feed {feed['name']}: {e}")
                continue
        return result
