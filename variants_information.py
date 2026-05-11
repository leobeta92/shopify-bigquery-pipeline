# Import Libraries
import os
import pandas as pd
import requests
import json

# Custom Functions
import src.utils as utils
import src.df_functions_pr_vr as pv_dffx
import src.queries_products_variants as pv_queries
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


VARIANTS = os.getenv('VARIANTS_INFORMATION')

# Get Shopify Token
SHOPIFY_ACCESS_TOKEN = utils.get_credentials(SHOPIFY_CLIENT_ID,SHOPIFY_SECRET)

# Query Shopify API for yesterday's product line items sales data
bulk_query_response = utils.bulk_query_request(pv_queries.variants_query,SHOPIFY_ACCESS_TOKEN)

# Wait for the result to finish and then get the file.
variants_response = utils.poll_for_result(SHOPIFY_ACCESS_TOKEN)

# Create a dataFrame and add data to it (for variants)

variants_df = pv_dffx.create_variants_info_df(variants_response)
variants_df_processed = pv_dffx.process_variant_data(variants_df)

variants_table_id = VARIANTS

gcloud.bigquery_write_table_truncate(client, variants_df_processed, variants_table_id,'variants_info')
