#TODO cleanup later, some of these are not used/shouldn't be used
USERNAME:str = ""
PASSWORD: str = ""
THROTTLE: float = 1
RETRIES: int = 2
BACKOFF_FACTOR = 2
MAX_PAGES = 50
CHUNK_SIZE = 10
REQUIRED_KEYS_CONFIG = ['base-url', 'endpoint']
SUPPORTED_PARTITION_STRATEGIES = ['','page','param']