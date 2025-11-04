import json
import boto3
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = 'stats-jours-trafic'
table = dynamodb.Table(TABLE_NAME)

def decimal_to_native(obj):
    if isinstance(obj, list):
        return [decimal_to_native(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

def lambda_handler(event, context):
    try:
        logger.info("Event: %s", json.dumps(event))

        params = event.get('queryStringParameters') or {}
        date = params.get('date')
        departement = params.get('departement')
        niveau_congestion = params.get('niveau_congestion')
        nom_rue = params.get('nom_rue')

        response = table.scan()
        items = response.get('Items', [])
        total_before = len(items)

        # 🛠 Filtrage PROPRE et FIABLE
        def equals(a, b):
            if not a or not b:
                return False
            return str(a).strip().lower() == str(b).strip().lower()

        if date:
            items = [i for i in items if equals(i.get('date'), date)]
        if departement:
            items = [i for i in items if equals(i.get('departement'), departement)]
        if niveau_congestion:
            items = [i for i in items if equals(i.get('niveau_congestion'), niveau_congestion)]
        if nom_rue:
            items = [i for i in items if equals(i.get('nom_rue'), nom_rue)]

        items_native = decimal_to_native(items)
        total_after = len(items_native)

        debug = {
            "received_query_params": params,
            "total_items_in_table": total_before,
            "total_items_after_filter": total_after,
            "sample_items_after_filter": items_native[:3]
        }

        logger.info("DEBUG: %s", json.dumps(debug, ensure_ascii=False))

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"debug": debug, "items": items_native}, ensure_ascii=False)
        }

    except Exception as e:
        logger.exception("Erreur Lambda")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
