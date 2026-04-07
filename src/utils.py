import requests
import json
import os
import pandas as pd

from zoneinfo import ZoneInfo
from datetime import datetime as dt
import dateutil.parser as du
import time

import src.queries as queries

from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

merchant = os.getenv("MERCHANT")

# Get Shopify credentials
def get_credentials(client_id,secret):

    # print(merchant)
    r = requests.post(
        f"https://{merchant}.myshopify.com/admin/oauth/access_token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": secret,
        }
    )

    return r.json()['access_token']


# Make bulk operation query request
def bulk_query_request(query,token):
    r = requests.post(
        f"https://{merchant}.myshopify.com/admin/api/2026-01/graphql.json",
        headers={
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": token,
        },
        json={"query": query}
    )

    return r.json()

# Make bulk operation status request
def status_update(status_query,token):
    r = requests.post(
        f"https://{merchant}.myshopify.com/admin/api/2026-01/graphql.json",
        headers={
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": token,
        },
        json={"query": status_query}
    )

    response = r.json()
    print(response)
    return response

def poll_for_result(token,interval_seconds=60, max_attempts=10):
    for attempt in range(1, max_attempts + 1):
        print(f"Attempt {attempt}/{max_attempts}...")
        
        bulk_status_response = status_update(queries.status,token)
        
        if (bulk_status_response['data']['currentBulkOperation']['status'] == 'COMPLETED' and bulk_status_response['data']['currentBulkOperation']['errorCode'] is None):
            print("Done! File now available.")

            url_results = bulk_status_response['data']['currentBulkOperation']['url']
            result = requests.get(url_results)
            contents = result.content
            my_json = contents.decode('utf8')
            orders = [json.loads(line) for line in my_json.strip().split('\n') if line.strip()]
            return orders

        print(f"Status: {bulk_status_response['data']['currentBulkOperation']['status']}. Retrying in {interval_seconds}s...")
        time.sleep(interval_seconds)

    raise TimeoutError("Job did not complete within the allowed attempts.")

