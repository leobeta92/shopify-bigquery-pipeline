import requests
import json
import os
import pandas as pd

from zoneinfo import ZoneInfo
from datetime import datetime as dt
import dateutil.parser as du
import time

merchant = os.getenv("MERCHANT")

# Get Shopify credentials
def get_credentials(client_id,secret):

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

    return r.json()

def modify_columns(df):

    # Column Definitions
    b_num_columns = ['cartDiscountAmountSet','currentCartDiscountAmountSet','currentShippingPriceSet','currentSubtotalPriceSet','currentTotalDiscountsSet','currentTotalPriceSet','currentTotalTaxSet','originalTotalPriceSet','subtotalPriceSet','totalDiscountsSet','totalPriceSet']
    b_date_cols = ['closedAt','createdAt','cancelledAt']
    b_dict_columns = ["cancellation", "channelInformation","currentTaxLines","discountCodes","paymentGatewayNames","refunds","transactions"] 

    # For dict/list items
    for col in b_dict_columns:

        if col == "refunds":
            df[col] = df[col].apply(lambda x: sum(float(d['totalRefundedSet']['shopMoney']['amount']) for d in x) if x else 0)   
        elif col == "currentTaxLines":
            df[col] = df[col].apply(lambda x: json.dumps([float(d['priceSet']['shopMoney']['amount']) for d in x]) if x else None)
        else:    
            df[col] = df[col].apply(lambda x: json.dumps(x) if (isinstance(x, dict) or isinstance(x, list)) else None)


    # Columns that need to be numbers
    # Columns with strings that need to be converted to float

    for col in b_num_columns:
        if col == "cartDiscountAmountSet":
            df[col] = df[col].apply(lambda x: float(x['shopMoney']['amount']) if x else 0)    
        else:
            df[col] = df[col].apply(lambda x: float(x['shopMoney']['amount']))

    # Date Columns
    for col in b_date_cols:
        col_utc = col + '_utc'

        if col == 'cancelledAt' or col == 'closedAt':
            df[col_utc] = df[col].apply(lambda x: pd.to_datetime(x) if x is not None else None)
        else:
            df[col_utc] = df[col].apply(lambda x: pd.to_datetime(x))
    
    return df