# Import Libraries
import os
import pandas as pd
import requests
import json

# Custom Functions
import src.utils as utils
import src.df_functions_collections as cdffx
import src.queries_collections as c_queries
import src.gcloud as gcloud

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


COLLECTIONS = os.getenv('COLLECTIONS')
PRODUCTS_COLLECTIONS = os.getenv('PRODUCTS_COLLECTIONS')

# Get Shopify Token
SHOPIFY_ACCESS_TOKEN = utils.get_credentials(SHOPIFY_CLIENT_ID,SHOPIFY_SECRET)

# Query Shopify API for yesterday's product line items sales data
bulk_query_response = utils.bulk_query_request(c_queries.collections_query,SHOPIFY_ACCESS_TOKEN)

# Wait for the result to finish and then get the file.
collections_response = utils.poll_for_result(SHOPIFY_ACCESS_TOKEN)

# Create a dataFrame and add data to it (for collections)
split_response = cdffx.response_split(collections_response)
collections_information = split_response['collections_information']
products_collections_information = split_response['products_collections']

collections_df = cdffx.add_collections_to_df(collections_information)
products_collections_df = cdffx.add_products_collections_data_to_df(products_collections_information)

collections_table_id = COLLECTIONS
products_collections_table_id = PRODUCTS_COLLECTIONS

gcloud.bigquery_write_table_truncate(client, collections_df, collections_table_id,'collections')
gcloud.bigquery_write_table_truncate(client, products_collections_df, products_collections_table_id,'products_collections')
