import pandas as pd
import json
import datetime

# Creating the columns for the necessary dataframes.

# Columns for collections.
def columns_for_collections_df():
    columns = ['id','collectionId']
    return columns

# Columns for products and collections df
def columns_for_products_collections_df():
    columns = ['id','title','productsInCollection']
    return columns    

# Creating the dataframes to process data and load into BigQuery.

# For collections
def create_collections_df():
    collections_df_complete = pd.DataFrame(columns= columns_for_collections_df())

    return collections_df_complete    

# For products and collections
def create_products_collections_df():
    products_collections_df_complete = pd.DataFrame(columns= columns_for_products_collections_df())

    return products_collections_df_complete  

# COLLECTIONS

# Call collections object and then split the response into a collections and a products_collections dict object.
def response_split(collections):
    
    products_table = []
    collection_table = []

    for item in collections:
        if '__parentId' in item.keys():
            products_table.append(item)
        else:
            collection_table.append(item)
    
    return {'products_collections': products_table,
             'collections_information': collection_table}

def add_products_collections_data_to_df(products_collections):

    # Resetting data frame
    products_collections_df = pd.DataFrame(columns= columns_for_products_collections_df())

    # Resetting objects
    id = []
    collectionId = []

    # Looping through data
    # for i, response in enumerate(orders):
    for response in products_collections:
        
        # print(response)
        id.append(response['id'])
        collectionId.append(response['__parentId'])
        

    products_collections_df['id'] = id
    products_collections_df['collectionId'] = collectionId
    
    return products_collections_df

# Creating collections dataframe
def add_collections_to_df(collections):
    collections_df = pd.DataFrame(columns= columns_for_collections_df())
    
    id = []
    title = []
    products_in_collection = []
    
    for response in collections:
        
        # print(response)
        id.append(response['id'])
        title.append(response['title'])
        products_in_collection.append(response['productsCount']['count'])

    collections_df['id'] = id
    collections_df['title'] = title
    collections_df['productsInCollection'] = products_in_collection

    return collections_df