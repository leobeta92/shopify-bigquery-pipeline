import pandas as pd
import json
import datetime

# In case import has orders that contain previous BigCommerce naming:
def filter_orders(orders):
    return [o for o in orders if not o['name'].startswith('1200')]


# Creating the columns for the necessary dataframes.

# For orders df
def columns_for_df():
    columns=['id','name','email','createdAt','cancellation','cancelledAt','cancelReason','cartDiscountAmountSet','channelInformation','closed','closedAt','currentCartDiscountAmountSet','currentShippingPriceSet','currentSubtotalLineItemsQuantity','currentSubtotalPriceSet','currentTaxLines','currentTotalAdditionalFeesSet','currentTotalDiscountsSet','currentTotalPriceSet','currentTotalTaxSet','discountCode','discountCodes','displayFinancialStatus','displayFulfillmentStatus','fullyPaid','netPaymentSet','originalTotalPriceSet','paymentGatewayNames','refunds','registeredSourceUrl','returnStatus','sourceName','subtotalLineItemsQuantity','subtotalPriceSet','totalDiscountsSet','totalReceivedSet','totalRefundedSet','totalRefundedShippingSet','totalShippingPriceSet','totalTaxSet','totalPriceSet','transactions','shippingDiscount','shippingAfterDiscount','originalShippingPrice']
    return columns

# For product line items df
def columns_for_products_df():
    columns = ['id','orderId','productId','variantId','quantity','refundableQuantity','originalTotalSet','originalUnitPriceSet','discountedUnitPriceSet','discountedTotalSet','discountedUnitPriceAfterAllDiscountsSet','isGiftCard','taxLines','unitCost']

    return columns

# Creating the dataframes to process data and load into BigQuery.
# Create orders dataframe.
def create_df():

    orders_df_complete = pd.DataFrame(columns= columns_for_df())

    return orders_df_complete

# For products
def create_products_df():

    products_df_complete = pd.DataFrame(columns= columns_for_products_df())

    return products_df_complete

# ORDERS

def add_data_to_df(orders):

    # Resetting data frame
    orders_df = create_df()

    # Resetting objects
    id = []
    name = []
    email = []
    createdAt = []
    cancellation = []
    cancelledAt = []
    cancelReason = []
    cartDiscountAmountSet = []
    channelInformation = []
    closed = []
    closedAt = []
    currentCartDiscountAmountSet = []
    currentShippingPriceSet = []
    currentSubtotalLineItemsQuantity = []
    currentSubtotalPriceSet = []
    currentTaxLines = []
    currentTotalAdditionalFeesSet = []
    currentTotalDiscountsSet = []
    currentTotalPriceSet = []
    currentTotalTaxSet = []
    discountCode = []
    discountCodes = []
    displayFinancialStatus = []
    displayFulfillmentStatus = []
    fullyPaid = []
    netPaymentSet = []
    originalTotalPriceSet = []
    paymentGatewayNames = []
    refunds = []
    registeredSourceUrl = []
    returnStatus = []
    sourceName = []
    subtotalLineItemsQuantity = []
    subtotalPriceSet = []
    totalDiscountsSet = []
    totalReceivedSet = []
    totalRefundedSet = []
    totalRefundedShippingSet = []
    totalShippingPriceSet = []
    totalTaxSet = []
    totalPriceSet = []
    transactions = []
    shippingDiscount = [] 
    shippingAfterDiscount = [] 
    originalShippingPrice = []

    # Looping through data
    for response in orders:
        
        id.append(response['id'])
        name.append(response['name'])
        email.append(response['email'])
        createdAt.append(response['createdAt'])
        cancellation.append(response['cancellation'])
        cancelledAt.append(response['cancelledAt'])
        cancelReason.append(response['cancelReason'])
        cartDiscountAmountSet.append(response['cartDiscountAmountSet'])
        channelInformation.append(response['channelInformation'])
        closed.append(response['closed'])
        closedAt.append(response['closedAt'])
        currentCartDiscountAmountSet.append(response['currentCartDiscountAmountSet'])
        currentShippingPriceSet.append(response['currentShippingPriceSet'])
        currentSubtotalLineItemsQuantity.append(response['currentSubtotalLineItemsQuantity'])
        currentSubtotalPriceSet.append(response['currentSubtotalPriceSet'])
        currentTaxLines.append(response['currentTaxLines'])
        currentTotalAdditionalFeesSet.append(response['currentTotalAdditionalFeesSet'])
        currentTotalDiscountsSet.append(response['currentTotalDiscountsSet'])
        currentTotalPriceSet.append(response['currentTotalPriceSet'])
        currentTotalTaxSet.append(response['currentTotalTaxSet'])
        discountCode.append(response['discountCode'])
        discountCodes.append(response['discountCodes'])
        displayFinancialStatus.append(response['displayFinancialStatus'])
        displayFulfillmentStatus.append(response['displayFulfillmentStatus'])
        fullyPaid.append(response['fullyPaid'])
        netPaymentSet.append(response['netPaymentSet'])
        originalTotalPriceSet.append(response['originalTotalPriceSet'])
        paymentGatewayNames.append(response['paymentGatewayNames'])
        refunds.append(response['refunds'])
        registeredSourceUrl.append(response['registeredSourceUrl'])
        returnStatus.append(response['returnStatus'])
        sourceName.append(response['sourceName'])
        subtotalLineItemsQuantity.append(response['subtotalLineItemsQuantity'])
        subtotalPriceSet.append(response['subtotalPriceSet'])

        totalDiscountsSet.append(response['totalDiscountsSet'])
        totalReceivedSet.append(response['totalReceivedSet'])
        totalRefundedSet.append(response['totalRefundedSet'])
        totalRefundedShippingSet.append(response['totalRefundedShippingSet'])
        totalShippingPriceSet.append(response['totalShippingPriceSet'])
        totalTaxSet.append(response['totalTaxSet'])
        
        totalPriceSet.append(response['totalPriceSet'])
        transactions.append(response['transactions'])
        if response['shippingLine'] is not None:
            if response['shippingLine']['discountAllocations'] is not None:
                shippingDiscount.append(response['shippingLine']['discountAllocations']) 
            else:
                shippingDiscount.append([])
            shippingAfterDiscount.append(response['shippingLine']['discountedPriceSet']['shopMoney']['amount']) 
            originalShippingPrice.append(response['shippingLine']['originalPriceSet']['shopMoney']['amount'])
        else:
            shippingDiscount.append([])
            shippingAfterDiscount.append(0) 
            originalShippingPrice.append(0)

    # Populate new table
    orders_df['id'] = id
    orders_df['name'] = name
    orders_df['email'] = email
    orders_df['createdAt'] = createdAt
    orders_df['cancellation'] = cancellation
    orders_df['cancelledAt'] = cancelledAt
    orders_df['cancelReason'] = cancelReason
    orders_df['cartDiscountAmountSet'] = cartDiscountAmountSet
    orders_df['channelInformation'] = channelInformation
    orders_df['closed'] = closed
    orders_df['closedAt'] = closedAt
    orders_df['currentCartDiscountAmountSet'] = currentCartDiscountAmountSet
    orders_df['currentShippingPriceSet'] = currentShippingPriceSet    
    orders_df['currentSubtotalLineItemsQuantity'] = currentSubtotalLineItemsQuantity
    orders_df['currentSubtotalPriceSet'] = currentSubtotalPriceSet
    orders_df['currentTaxLines'] = currentTaxLines
    orders_df['currentTotalAdditionalFeesSet'] = currentTotalAdditionalFeesSet
    orders_df['currentTotalDiscountsSet'] = currentTotalDiscountsSet    
    orders_df['currentTotalPriceSet'] = currentTotalPriceSet
    orders_df['currentTotalTaxSet'] = currentTotalTaxSet
    orders_df['discountCode'] = discountCode
    orders_df['discountCodes'] = discountCodes
    orders_df['displayFinancialStatus'] = displayFinancialStatus    
    orders_df['displayFulfillmentStatus'] = displayFulfillmentStatus
    orders_df['fullyPaid'] = fullyPaid
    orders_df['netPaymentSet'] = netPaymentSet
    orders_df['originalTotalPriceSet'] = originalTotalPriceSet
    orders_df['paymentGatewayNames'] = paymentGatewayNames
    orders_df['refunds'] = refunds   
    orders_df['registeredSourceUrl'] = registeredSourceUrl
    orders_df['returnStatus'] = returnStatus
    orders_df['sourceName'] = sourceName
    orders_df['subtotalLineItemsQuantity'] = subtotalLineItemsQuantity
    orders_df['subtotalPriceSet'] = subtotalPriceSet   
    orders_df['totalDiscountsSet'] = totalDiscountsSet
    orders_df['totalReceivedSet'] = totalReceivedSet
    orders_df['totalRefundedSet'] = totalRefundedSet
    orders_df['totalRefundedShippingSet'] = totalRefundedShippingSet
    orders_df['totalShippingPriceSet'] = totalShippingPriceSet
    orders_df['totalTaxSet'] = totalTaxSet
    orders_df['totalPriceSet'] = totalPriceSet  
    orders_df['transactions'] = transactions 
     
    orders_df['shippingDiscount'] =  shippingDiscount
    orders_df['shippingAfterDiscount'] = shippingAfterDiscount
    orders_df['originalShippingPrice'] = originalShippingPrice

    return orders_df

def process_data(df):

    # Column Definitions
    b_num_columns = ['cartDiscountAmountSet','currentCartDiscountAmountSet','currentShippingPriceSet','currentSubtotalPriceSet','currentTotalDiscountsSet','currentTotalPriceSet','currentTotalTaxSet','netPaymentSet','originalTotalPriceSet','subtotalPriceSet','totalDiscountsSet','totalPriceSet','totalReceivedSet','totalRefundedSet','totalRefundedShippingSet','totalShippingPriceSet','totalTaxSet','shippingAfterDiscount','originalShippingPrice']
    b_date_cols = ['closedAt','createdAt','cancelledAt']
    b_dict_columns = ["cancellation", "channelInformation","currentTaxLines","discountCodes","paymentGatewayNames","refunds","transactions","shippingDiscount"] 

    # For dict/list items
    for col in b_dict_columns:

        if col == "refunds":
            df[col] = df[col].apply(lambda x: sum(float(d['totalRefundedSet']['shopMoney']['amount']) for d in x) if x else 0)
        elif col == "shippingDiscount":
            df[col] = df[col].apply(lambda x: sum([float(d['allocatedAmountSet']['shopMoney']['amount']) for d in x]))           
        elif col == "currentTaxLines":
            df[col] = df[col].apply(lambda x: json.dumps([float(d['priceSet']['shopMoney']['amount']) for d in x]) if x else None)
        else:    
            df[col] = df[col].apply(lambda x: json.dumps(x) if (isinstance(x, dict) or isinstance(x, list)) else None)


    # Columns that need to be numbers
    # Columns with strings that need to be converted to float

    for col in b_num_columns:
        if col == "cartDiscountAmountSet":
            df[col] = df[col].apply(lambda x: float(x['shopMoney']['amount']) if x else 0) 
        elif col == 'shippingAfterDiscount' or col == 'originalShippingPrice':
            df[col] = df[col].apply(lambda x: float(x))
        else:
            df[col] = df[col].apply(lambda x: float(x['shopMoney']['amount']))

    # Date Columns
    for col in b_date_cols:
        col_utc = col + '_utc'

        if col == 'cancelledAt' or col == 'closedAt':
            df[col_utc] = df[col].apply(lambda x: pd.to_datetime(x) if x is not None else None)
        else:
            df[col_utc] = df[col].apply(lambda x: pd.to_datetime(x))
    
    df['updatedAt'] = datetime.datetime.now()

    return df

# PRODUCTS LINE ITEMS

def response_to_products(orders):
    
    products_table = []

    for item in orders:
        if '__parentId' in item.keys() and item['product'] is not None:
            products_table.append(item)
    
    return products_table

def add_product_data_to_df(orders):

    # Resetting data frame
    line_items_df = create_products_df()

    # Resetting objects
    id = []
    orderId = []
    productId = []
    variantId = []
    quantity = []
    refundableQuantity = []
    isGiftCard = []
    originalTotalSet = []
    originalUnitPriceSet = []
    discountedTotalSet = []
    discountedUnitPriceSet = []
    discountedUnitPriceAfterAllDiscountsSet = []
    isGiftCard = []
    taxLines = []
    unitCost = []

    # Looping through data
    for response in orders:
        
        # print(response)
        id.append(response['id'])
        orderId.append(response['__parentId'])
        productId.append(response['product']['id'])
        if response['variant'] is not None:
            variantId.append(response['variant']['id'])
        else:
            variantId.append('N/A')
        quantity.append(response['quantity'])
        refundableQuantity.append(response['refundableQuantity'])
        originalTotalSet.append(response['originalTotalSet']['shopMoney']['amount'])
        originalUnitPriceSet.append(response['originalUnitPriceSet']['shopMoney']['amount'])
        discountedTotalSet.append(response['discountedTotalSet']['shopMoney']['amount'])
        discountedUnitPriceSet.append(response['discountedUnitPriceSet']['shopMoney']['amount'])
        discountedUnitPriceAfterAllDiscountsSet.append(response['discountedUnitPriceAfterAllDiscountsSet']['shopMoney']['amount'])
        isGiftCard.append(response['isGiftCard'])
        taxLines.append(response['taxLines'])
        if response['variant'] is not None:
        # print(i,"-",response['variant']['inventoryItem']['unitCost']['amount'])
            if response['variant']['inventoryItem']['unitCost'] is not None:
                unitCost.append(response['variant']['inventoryItem']['unitCost']['amount'])
            else:
                unitCost.append(0)
        else:
            unitCost.append(0)     

    line_items_df['id'] = id
    line_items_df['orderId'] = orderId
    line_items_df['productId'] = productId
    line_items_df['variantId'] = variantId
    line_items_df['quantity'] = quantity
    line_items_df['refundableQuantity'] = refundableQuantity
    line_items_df['originalTotalSet'] = originalTotalSet
    line_items_df['originalUnitPriceSet'] = originalUnitPriceSet
    line_items_df['discountedTotalSet'] = discountedTotalSet
    line_items_df['discountedUnitPriceSet'] = discountedUnitPriceSet
    line_items_df['discountedUnitPriceAfterAllDiscountsSet'] = discountedUnitPriceAfterAllDiscountsSet
    line_items_df['isGiftCard'] = isGiftCard
    line_items_df['taxLines'] = taxLines
    line_items_df['unitCost'] = unitCost

    return line_items_df

def process_product_data(df):

    num_columns = ['originalTotalSet','originalUnitPriceSet','discountedUnitPriceSet','discountedTotalSet','discountedUnitPriceAfterAllDiscountsSet','unitCost']    

    df['totalTax'] = df['taxLines'].apply(lambda x: sum(float(item['priceSet']['shopMoney']['amount']) for item in x) if x else 0)
    df['taxLines'] = df['taxLines'].apply(lambda x: json.dumps(x) if (isinstance(x, dict) or isinstance(x, list)) else None)

        # Columns that need to be numbers
        # Columns with strings that need to be converted to float

    for col in num_columns:

        df[col] = df[col].apply(lambda x: float(x))
    
    return df
