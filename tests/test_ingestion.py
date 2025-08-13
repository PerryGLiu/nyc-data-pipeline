import pytest
from src.data_ingestion import MTADataFetcher
from src.utils import get_logger

logger = get_logger(__name__)

@pytest.fixture
def fetcher():
    return MTADataFetcher()

def test_fetch_all_feeds(fetcher):
    # This is a basic test - in reality you'd mock the API calls
    try:
        result = fetcher.fetch_all_feeds()
        assert isinstance(result, dict)
        logger.info("Fetch test passed")
    except Exception as e:
        logger.error(f"Fetch test failed: {e}")
        pytest.fail(f"Fetch test failed: {e}")
