import json
import logging
import boto3
import requests

PAYLOAD_TEMPLATE = {
    "serviceLocation": "USA - Boston - Remote",
    "impact": "2",
    "urgency": "1",
    "category": "Application/Software",
    "subcategory": "Error/Issue",
    "consumerName": "AWS Lambda"
}

SHORT_DESC = "{event_source_hr} failed {resource_name}"
DESCRIPTION = (
    "\n\nSpark API Connector has encountered a failure.\n"
    "Error message: {error_message}"
)

class SecretsManager:
    def __init__(self, region: str = 'us-east-1'):
        self.client = boto3.client('secretsmanager', region_name=region)

    def get_secret(self, secret_name: str) -> dict:
        response = self.client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])

class IncidentCreator:
    def __init__(self, secret_name: str, region: str = 'us-east-1'):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.secret = SecretsManager(region).get_secret(secret_name)

    def generate_payload(self, event_source_hr: str, resource_name: str, error_message: str) -> dict:
        payload = PAYLOAD_TEMPLATE.copy()
        payload.update({
            "shortDescription": SHORT_DESC.format(event_source_hr=event_source_hr, resource_name=resource_name),
            "description": DESCRIPTION.format(error_message=error_message),
            "requestedFor": resource_name
        })
        return payload

    def create_incident(self, url: str, event_source_hr: str, resource_name: str, error_message: str) -> str:
        payload = self.generate_payload(event_source_hr, resource_name, error_message)
        headers = {
            "client_id": self.secret["client_id"],
            "client_secret": self.secret["client_secret"],
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        return response.json().get("IncidentNumber", "UNKNOWN")