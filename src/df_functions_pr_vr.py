import pandas as pd
import json
import datetime

def columns_for_products_info_df():
    columns = ['id','title','createdAt','isGiftCard','status','onlineStoreUrl','isClearance','publishedAt']
    
    return columns

def columns_for_variants_info_df():
    columns = ['variantId','sku','productId','price','availableForSale','compareAtPrice','createdAt','inventoryQuantity','updatedAt']
    
    return columns

def create_products_info_df(products):

    # Resetting data frame
    products_df = pd.DataFrame(columns= columns_for_products_info_df())

    # Resetting objects
    id = []
    title = []
    createdAt = []
    isGiftCard = []
    status = []
    onlineStoreUrl = []
    isClearance = []
    publishedAt = []

    # Looping through data
    # for i, response in enumerate(products):
    for response in products:
        
        id.append(response['id'])
        title.append(response['title'])
        createdAt.append(response['createdAt'])
        isGiftCard.append(response['isGiftCard'])
        status.append(response['status'])
        onlineStoreUrl.append(response['onlineStoreUrl'])
        if response['metafield'] is not None:
            isClearance.append(response['metafield']['value'])
        else:
            isClearance.append('N/A')
        publishedAt.append(response['publishedAt'])
        
    
    products_df['id'] = id
    products_df['title'] = title
    products_df['createdAt'] = createdAt
    products_df['isGiftCard'] = isGiftCard
    products_df['status'] = status
    products_df['onlineStoreUrl'] = onlineStoreUrl
    products_df['isClearance'] = isClearance
    products_df['publishedAt'] = publishedAt
    
    return products_df

def create_variants_info_df(variants):

    # Resetting data frame
    variants_df = pd.DataFrame(columns= columns_for_variants_info_df())

    # Resetting objects
    variantId = []
    sku = []
    productId = []
    price = []
    unitCost = []
    availableForSale = []
    compareAtPrice = []
    createdAt = []
    inventoryQuantity = []
    updatedAt = []

    # Looping through data
    # for i, response in enumerate(variants):
    for response in variants:
        
        # print(response)
        variantId.append(response['id'])
        if response['sku'] is not None:
            sku.append(response['sku'])
        else:
            sku.append('Not Assigned')
        productId.append(response['product']['id'])
        price.append(response['price'])
        if response['inventoryItem']['unitCost'] is not None:
            unitCost.append(response['inventoryItem']['unitCost']['amount'])
        else:
            unitCost.append(0)
        availableForSale.append(response['availableForSale'])
        compareAtPrice.append(response['compareAtPrice'])
        createdAt.append(response['createdAt'])
        inventoryQuantity.append(response['inventoryQuantity'])
        updatedAt.append(response['updatedAt'])
       

    variants_df['variantId'] = variantId
    variants_df['sku'] = sku
    variants_df['productId'] = productId
    variants_df['price'] = price
    variants_df['unitCost'] = unitCost
    variants_df['availableForSale'] = availableForSale
    variants_df['compareAtPrice'] = compareAtPrice
    variants_df['createdAt'] = createdAt
    variants_df['inventoryQuantity'] = inventoryQuantity
    variants_df['updatedAt'] = updatedAt
    
    
    return variants_df


def process_product_data(df):

    date_cols = ['createdAt','publishedAt']

    for col in date_cols:
        col_utc = col + '_utc'

        df[col_utc] = df[col].apply(lambda x: pd.to_datetime(x) if x is not None else None)
    
    return df

def process_variant_data(df):

    num_columns = ['price','compareAtPrice','unitCost']
    date_cols = ['createdAt','updatedAt']

    for col in num_columns:
        df[col] = df[col].apply(lambda x: float(x) if x is not None else 0)

    for col in date_cols:
        col_utc = col + '_utc'
        df[col_utc] = df[col].apply(lambda x: pd.to_datetime(x) if x is not None else None)
        
    return df