import json
import boto3
import logging
from decimal import Decimal

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DynamoDB
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = 'TrafficAggregated'
table = dynamodb.Table(TABLE_NAME)

def decimal_to_native(obj):
    if isinstance(obj, list):
        return [decimal_to_native(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

def equals(a, b):
    """Comparaison robuste (ignore espaces, casse, nulls)"""
    if not a or not b:
        return False
    return str(a).strip().lower() == str(b).strip().lower()

def lambda_handler(event, context):
    try:
        logger.info("EVENT RAW: %s", json.dumps(event))

        params = event.get('queryStringParameters') or {}
        date = params.get('date')
        location_name = params.get('location_name')

        logger.info(f"Received: date={date}, location_name={location_name}")

        # Lecture brute de la table
        response = table.scan()
        items = response.get('Items', [])
        total_before = len(items)

        # Filtrage Python basé sur ton schéma
        if date:
            items = [i for i in items if equals(i.get('Date'), date)]
        if location_name:
            items = [i for i in items if equals(i.get('Location_Name'), location_name)]

        items_native = decimal_to_native(items)
        total_after = len(items_native)

        debug = {
            "received_params": params,
            "total_items_in_table": total_before,
            "total_after_filter": total_after,
            "sample_items": items_native[:3]
        }

        logger.info(json.dumps(debug, ensure_ascii=False))

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"debug": debug, "items": items_native}, ensure_ascii=False)
        }

    except Exception as e:
        logger.exception("Erreur Lambda vélo")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
