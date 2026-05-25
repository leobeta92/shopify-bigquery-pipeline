# Import Libraries
import os
import pandas as pd
import requests
import json

# Custom Functions
import src.utils as utils
import src.df_functions_orders_addresses as dffx
import src.queries_orders_addresses as queries
import src.gcloud as gcloud

# Datetime Packages
from zoneinfo import ZoneInfo
from datetime import datetime as dt
import dateutil.parser as du
import time

# Services Libraries
from google.oauth2 import service_account
from google.cloud import bigquery

# Load Shopify Secrets
SHOPIFY_CLIENT_ID = os.getenv("SHOPIFY_CLIENT_ID")
SHOPIFY_SECRET = os.getenv("SHOPIFY_SECRET")

# Load Google Cloud services account and BigQuery Client
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cloud_python_private_key.json'
# client = bigquery.Client()

credentials_json = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')

if credentials_json:
    # Running in GitHub Actions
    credentials_dict = json.loads(credentials_json)
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    client = bigquery.Client(credentials=credentials, project=credentials_dict['project_id'])
else:
    # Running locally
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cloud_python_private_key.json'
    client = bigquery.Client()


ORDERS_ADDRESSES = os.getenv('ORDERS_ADDRESSES')

# Get Shopify Token
SHOPIFY_ACCESS_TOKEN = utils.get_credentials(SHOPIFY_CLIENT_ID,SHOPIFY_SECRET)

# Query Shopify API for last week's data (This will run weekly)
bulk_query_response = utils.bulk_query_request(queries.last_week,SHOPIFY_ACCESS_TOKEN)

# Wait for the result to finish and then get the file.
orders = utils.poll_for_result(SHOPIFY_ACCESS_TOKEN)

# Create a dataFrame and add data to it.
orders_df = dffx.create_order_address_info_df(orders)

table_id = ORDERS_ADDRESSES

gcloud.bigquery_write_table_append(client, orders_df, table_id,'orders_addresses')