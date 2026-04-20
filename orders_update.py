# Import Libraries
import os
import pandas as pd
import requests
import json

# Custom Functions
import src.utils as utils
import src.df_functions as dffx
import src.queries as queries
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
# if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cloud_python_private_key.json'
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

ORDERS_UPDATE = os.getenv('ORDERS_UPDATE')

# Get Shopify Token
SHOPIFY_ACCESS_TOKEN = utils.get_credentials(SHOPIFY_CLIENT_ID,SHOPIFY_SECRET)

# Query Shopify API for yesterday's data
bulk_query_response = utils.bulk_query_request(queries.last_30_days,SHOPIFY_ACCESS_TOKEN)

# Wait for the result to finish and then get the file.
orders = utils.poll_for_result(SHOPIFY_ACCESS_TOKEN)

# Create a dataFrame and add data to it.
orders_df_complete = dffx.add_data_to_df(orders)

# Process data prior to moving to BigQuery
new_df = dffx.process_data(orders_df_complete)

table_id = ORDERS_UPDATE

gcloud.bigquery_write_table_truncate(client, new_df, table_id)

print('Query of Orders to Modify: ',queries.orders_to_modify)
print('Query to Upsert: ',queries.upsert_orders)
gcloud.upsert_orders(client,queries.orders_to_modify,queries.upsert_orders)