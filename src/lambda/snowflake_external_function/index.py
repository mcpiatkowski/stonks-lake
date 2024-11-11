import logging
import json
import requests
import boto3
from botocore.exceptions import ClientError

log = logging.getLogger("StonksApi")
log.setLevel(logging.INFO)


def get_secret():
    ssm = boto3.client('ssm')
    try:
        parameter = ssm.get_parameter(
            Name='/financial-modeling-prep/api-key',  # Parameter name in SSM
            WithDecryption=True
        )
        return parameter['Parameter']['Value']
    except ClientError as e:
        print(f"Error retrieving API key: {e}")
        raise


def handler(event, context):
    status_code = 200
    array_of_rows_to_return = []
    BASE_URL = "https://financialmodelingprep.com/api/v3/quote-short"

    log.info(f"Event: {event}")

    try:
        # Get API key from Parameter Store
        API_KEY = get_secret()

        event_body = event["body"]
        payload = json.loads(event_body)
        rows = payload["data"]

        for row in rows:
            log.info(f"Row: {row}")
            row_number = row[0]
            ticker_symbol = row[1]

            url = f"{BASE_URL}/{ticker_symbol}?apikey={API_KEY}"
            response = requests.get(url)

            if response.status_code == 200:
                stock_data = response.json()
                if stock_data:
                    current_price = stock_data[0]["price"]
                    output_value = [ticker_symbol, current_price]
                else:
                    output_value = [ticker_symbol, "No data found"]
            else:
                output_value = [ticker_symbol, f"API Error: {response.status_code}"]

            row_to_return = [row_number, output_value]
            array_of_rows_to_return.append(row_to_return)

        json_compatible_string_to_return = json.dumps({"data": array_of_rows_to_return})

    except ClientError as e:
        status_code = 500
        json_compatible_string_to_return = json.dumps({"error": f"Secret retrieval failed: {str(e)}"})
    except requests.RequestException as e:
        status_code = 500
        json_compatible_string_to_return = json.dumps({"error": f"API request failed: {str(e)}"})
    except Exception as err:
        status_code = 400
        json_compatible_string_to_return = event_body

    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return
    }
