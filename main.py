from api_ingestion import config_loader
from api_ingestion.ingestion_builder import ApiIngestion

if __name__ == "__main__":
    conf = config_loader.ConfigLoader('config/config.json')
    print(conf.load_app_context())