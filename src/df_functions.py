import pandas as pd
import json
import datetime

def columns_for_df():
    columns=['id','name','email','createdAt','cancellation','cancelledAt','cancelReason','cartDiscountAmountSet','channelInformation','closed','closedAt','currentCartDiscountAmountSet','currentShippingPriceSet','currentSubtotalLineItemsQuantity','currentSubtotalPriceSet','currentTaxLines','currentTotalAdditionalFeesSet','currentTotalDiscountsSet','currentTotalPriceSet','currentTotalTaxSet','discountCode','discountCodes','displayFinancialStatus','displayFulfillmentStatus','fullyPaid','originalTotalPriceSet','paymentGatewayNames','refunds','registeredSourceUrl','returnStatus','sourceName','subtotalLineItemsQuantity','subtotalPriceSet','totalDiscountsSet','totalPriceSet','transactions'
]
    return columns

def create_df():

    orders_df_complete = pd.DataFrame(columns= columns_for_df())

    return orders_df_complete
    

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
    originalTotalPriceSet = []
    paymentGatewayNames = []
    refunds = []
    registeredSourceUrl = []
    returnStatus = []
    sourceName = []
    subtotalLineItemsQuantity = []
    subtotalPriceSet = []
    totalDiscountsSet = []
    totalPriceSet = []
    transactions = []

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
        originalTotalPriceSet.append(response['originalTotalPriceSet'])
        paymentGatewayNames.append(response['paymentGatewayNames'])
        refunds.append(response['refunds'])
        registeredSourceUrl.append(response['registeredSourceUrl'])
        returnStatus.append(response['returnStatus'])
        sourceName.append(response['sourceName'])
        subtotalLineItemsQuantity.append(response['subtotalLineItemsQuantity'])
        subtotalPriceSet.append(response['subtotalPriceSet'])
        totalDiscountsSet.append(response['totalDiscountsSet'])
        totalPriceSet.append(response['totalPriceSet'])
        transactions.append(response['transactions'])
        
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
    orders_df['originalTotalPriceSet'] = originalTotalPriceSet
    orders_df['paymentGatewayNames'] = paymentGatewayNames
    orders_df['refunds'] = refunds   
    orders_df['registeredSourceUrl'] = registeredSourceUrl
    orders_df['returnStatus'] = returnStatus
    orders_df['sourceName'] = sourceName
    orders_df['subtotalLineItemsQuantity'] = subtotalLineItemsQuantity
    orders_df['subtotalPriceSet'] = subtotalPriceSet   
    orders_df['totalDiscountsSet'] = totalDiscountsSet
    orders_df['totalPriceSet'] = totalPriceSet  
    orders_df['transactions'] = transactions  

    return orders_df

def process_data(df):

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
    
    df['updatedAt'] = datetime.datetime.now()

    return df
