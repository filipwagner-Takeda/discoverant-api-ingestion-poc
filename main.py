import json

from api_ingestion import config_loader

if __name__ == "__main__":
    conf = config_loader.ConfigLoader('config/config.json')
    params = {"AGId": "78f0bd38-39ff-4098-8316-dc160a2a3541"}
    param = json.dumps(params)
    print(json.loads(param))
    print(params.keys())
    print(conf.parse_params())