from google.cloud import bigquery

def bigquery_write_table_truncate(client, df, table_id):

    job_config = bigquery.LoadJobConfig(
    schema = [
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("email", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("createdAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cancellation", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cancelledAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cancelReason", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cartDiscountAmountSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("channelInformation", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("closed", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("closedAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("currentCartDiscountAmountSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentShippingPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentSubtotalLineItemsQuantity", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("currentSubtotalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentTaxLines", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("currentTotalAdditionalFeesSet", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("currentTotalDiscountsSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentTotalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentTotalTaxSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("discountCode", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("discountCodes", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("displayFinancialStatus", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("displayFulfillmentStatus", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("fullyPaid", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("originalTotalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("paymentGatewayNames", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("refunds", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("registeredSourceUrl", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("returnStatus", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("sourceName", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("subtotalLineItemsQuantity", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("subtotalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("totalDiscountsSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("totalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("transactions", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("closedAt_utc", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("createdAt_utc", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("cancelledAt_utc", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("updatedAt", bigquery.enums.SqlTypeNames.DATETIME),
    ],
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(df,table_id,job_config= job_config)
    job.result()

    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def bigquery_write_table_append(client, df, table_id):

    job_config = bigquery.LoadJobConfig(
    schema = [
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("email", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("createdAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cancellation", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cancelledAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cancelReason", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cartDiscountAmountSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("channelInformation", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("closed", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("closedAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("currentCartDiscountAmountSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentShippingPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentSubtotalLineItemsQuantity", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("currentSubtotalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentTaxLines", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("currentTotalAdditionalFeesSet", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("currentTotalDiscountsSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentTotalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("currentTotalTaxSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("discountCode", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("discountCodes", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("displayFinancialStatus", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("displayFulfillmentStatus", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("fullyPaid", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("originalTotalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("paymentGatewayNames", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("refunds", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("registeredSourceUrl", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("returnStatus", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("sourceName", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("subtotalLineItemsQuantity", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("subtotalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("totalDiscountsSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("totalPriceSet", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("transactions", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("closedAt_utc", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("createdAt_utc", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("cancelledAt_utc", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("updatedAt", bigquery.enums.SqlTypeNames.DATETIME),
    ],
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(df,table_id,job_config= job_config)
    job.result()

    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def upsert_orders(client, query_orders_to_change, query_upsert):

    print('Starting upsert...')
    query_job_orders_to_change = client.query(query_orders_to_change)  
    query_job_orders_to_change.result()
    
    df = query_job_orders_to_change.to_dataframe()
    print(
    "{} rows to be updated".format(
        df.shape[0]
    )
    )

    if df.shape[0] > 0:
        query_job_upsert = client.query(query_upsert)  
        query_job_upsert.result()
        print('Orders updated!')
    else:
        print('Nothing to modify! All current.')