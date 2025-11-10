from typing import Any

from api_ingestion.connector_wrapper import fetch_from_rest
from connector import MyRestDataSource
from config_loader import ConfigLoader
from datetime import datetime, timedelta
class Runner:
    def __init__(self,spark:Any, config_path:str):
        self.spark = spark
        self.spark.dataSource.register(MyRestDataSource)
        conf = ConfigLoader(config_path)
        self.app_context = conf.load_app_context()

    def run_api_ingestion(self):
        #for now just skip this
        if self.app_context.history_load.enabled:
            self._generate_history_load(self.app_context.history_load.start_date,1)
        else:
            fetch_from_rest(self.spark,self.app_context).show()

    def _generate_history_load(self,start_dt: str,windowing_days: int):
        start_dt = datetime.strptime(start_dt, "%Y-%m-%d")
        end_dt = datetime.now()
        while start_dt <= end_dt:
            yield start_dt, min(end_dt, start_dt + timedelta(days=windowing_days - 1))
            start_dt += timedelta(days=windowing_days)