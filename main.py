from api_ingestion import config_loader
from datetime import datetime,timedelta

if __name__ == "__main__":
    conf = config_loader.ConfigLoader('config/config.json')
    print(conf.load_app_context())
