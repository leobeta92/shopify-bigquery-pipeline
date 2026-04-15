import datetime as dt
import os

# For moving time-based queries.
today = dt.date.today()
today_string = dt.date.strftime(dt.date.today(),'%Y-%m-%d')
today_minus_30 = dt.date.strftime(today - dt.timedelta(days = 30),'%Y-%m-%d')

from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

# Importing table strings
ORDERS = os.getenv('ORDERS')
ORDERS_UPDATE = os.getenv('ORDERS_UPDATE')

# Yesterday query

yesterday = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
orders(query: "created_at:yesterday") {
edges {
node {
id
name
email
createdAt
cancellation {
    staffNote
    } # closes cancellation
cancelledAt
cancelReason
cartDiscountAmountSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes cartDiscountAmountSet
channelInformation {
    displayName
    } # closes channelInformation
closed
closedAt
currentCartDiscountAmountSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentCartDiscountAmountSet
currentShippingPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentShippingPriceSet
currentSubtotalLineItemsQuantity
currentSubtotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentSubtotalPriceSet
currentTaxLines {
    priceSet {
        shopMoney {
            amount
            } # closes shopMoney
        } # closes priceSet
    } # closes currentTaxLines 
currentTotalAdditionalFeesSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalAdditionalFeesSet
currentTotalDiscountsSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalDiscountsSet
currentTotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalPriceSet
currentTotalTaxSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalTaxSet
discountCode
discountCodes
displayFinancialStatus
displayFulfillmentStatus
fullyPaid
originalTotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalTaxSet
paymentGatewayNames
refunds {
    totalRefundedSet {
        shopMoney {
        amount
        } # closes shopMoney
    } # closes totalRefundedSet
    } # closes refunds
registeredSourceUrl
returnStatus
sourceName
subtotalLineItemsQuantity
subtotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
        } # closes subtotalPriceSet
totalDiscountsSet {
    shopMoney {
        amount
        } # closes shopMoney
        } # closes totalDiscountsSet
totalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
        } # closes totalPriceSet
transactions {
    device {
        id
        } # closes device
    } # closes transactions

} # Closing node
} # Closing edges
} # Closing orders
} # Closing query bracket
    \"""
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
    """

status = """
query {
  currentBulkOperation {
    id
    status
    errorCode
    createdAt
    completedAt
    objectCount
    fileSize
    url
    partialDataUrl
  }
}
"""

backfill = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
orders(query: "created_at:>=2026-02-23 created_at:<=2026-04-06") {
edges {
node {
id
name
email
createdAt
cancellation {
    staffNote
    } # closes cancellation
cancelledAt
cancelReason
cartDiscountAmountSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes cartDiscountAmountSet
channelInformation {
    displayName
    } # closes channelInformation
closed
closedAt
currentCartDiscountAmountSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentCartDiscountAmountSet
currentShippingPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentShippingPriceSet
currentSubtotalLineItemsQuantity
currentSubtotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentSubtotalPriceSet
currentTaxLines {
    priceSet {
        shopMoney {
            amount
            } # closes shopMoney
        } # closes priceSet
    } # closes currentTaxLines 
currentTotalAdditionalFeesSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalAdditionalFeesSet
currentTotalDiscountsSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalDiscountsSet
currentTotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalPriceSet
currentTotalTaxSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalTaxSet
discountCode
discountCodes
displayFinancialStatus
displayFulfillmentStatus
fullyPaid
originalTotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalTaxSet
paymentGatewayNames
refunds {
    totalRefundedSet {
        shopMoney {
        amount
        } # closes shopMoney
    } # closes totalRefundedSet
    } # closes refunds
registeredSourceUrl
returnStatus
sourceName
subtotalLineItemsQuantity
subtotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
        } # closes subtotalPriceSet
totalDiscountsSet {
    shopMoney {
        amount
        } # closes shopMoney
        } # closes totalDiscountsSet
totalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
        } # closes totalPriceSet
transactions {
    device {
        id
        } # closes device
    } # closes transactions

} # Closing node
} # Closing edges
} # Closing orders
} # Closing query bracket
    \"""
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
    """

# Need to do some string replacing to get a dynamic date into a """block""".
last_30_days_pre = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
orders(query: "created_at:>={string_date} created_at:<={today_string}") {
edges {
node {
id
name
email
createdAt
cancellation {
    staffNote
    } # closes cancellation
cancelledAt
cancelReason
cartDiscountAmountSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes cartDiscountAmountSet
channelInformation {
    displayName
    } # closes channelInformation
closed
closedAt
currentCartDiscountAmountSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentCartDiscountAmountSet
currentShippingPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentShippingPriceSet
currentSubtotalLineItemsQuantity
currentSubtotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentSubtotalPriceSet
currentTaxLines {
    priceSet {
        shopMoney {
            amount
            } # closes shopMoney
        } # closes priceSet
    } # closes currentTaxLines 
currentTotalAdditionalFeesSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalAdditionalFeesSet
currentTotalDiscountsSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalDiscountsSet
currentTotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalPriceSet
currentTotalTaxSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalTaxSet
discountCode
discountCodes
displayFinancialStatus
displayFulfillmentStatus
fullyPaid
originalTotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
    } # closes currentTotalTaxSet
paymentGatewayNames
refunds {
    totalRefundedSet {
        shopMoney {
        amount
        } # closes shopMoney
    } # closes totalRefundedSet
    } # closes refunds
registeredSourceUrl
returnStatus
sourceName
subtotalLineItemsQuantity
subtotalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
        } # closes subtotalPriceSet
totalDiscountsSet {
    shopMoney {
        amount
        } # closes shopMoney
        } # closes totalDiscountsSet
totalPriceSet {
    shopMoney {
        amount
        } # closes shopMoney
        } # closes totalPriceSet
transactions {
    device {
        id
        } # closes device
    } # closes transactions

} # Closing node
} # Closing edges
} # Closing orders
} # Closing query bracket
    \"""
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
    """

last_30_days = (last_30_days_pre.replace("{string_date}", today_minus_30)).replace("{today_string}",today_string)

# How many orders have changed their status (refunded, fulfilled, etc.) since the order was first placed?
orders_to_modify = (
    f'''
    SELECT 
  so.returnStatus as so_returnStatus,
  so.fullyPaid as so_fullyPaid,
  so.displayFinancialStatus as so_displayFinancialStatus,
  so.displayFulfillmentStatus as so_displayFulfillmentStatus,
  so.cancelReason as so_cancelReason,
  so.cancelledAt_utc as so_cancelledAt,
  so.closed as so_closed,

  sou.returnStatus as sou_returnStatus,
  sou.fullyPaid as sou_fullyPaid,
  sou.displayFinancialStatus as sou_displayFinancialStatus,
  sou.displayFulfillmentStatus as sou_displayFulfillmentStatus,
  sou.cancelReason as sou_cancelReason,
  sou.cancelledAt_utc as sou_cancelledAt,
  sou.closed as sou_closed,
  sou.closedAt as sou_closedAt,
  so.closedAt as so_closedAt

FROM {ORDERS} so
INNER JOIN {ORDERS_UPDATE} sou
  on so.id = sou.id
where 
  so.returnStatus != sou.returnStatus OR
  so.fullyPaid != sou.fullyPaid OR
  so.displayFulfillmentStatus != sou.displayFulfillmentStatus OR
  so.displayFinancialStatus != sou.displayFinancialStatus OR
  so.cancelReason != sou.cancelReason OR
  so.closed != sou.closed    

    '''
)

# Upsert orders that have changed status since they were originally placed.
upsert_orders = f'''
    MERGE `{ORDERS}` as so
    USING
    `{ORDERS_UPDATE}` as sou
    on so.id = sou.id

    WHEN MATCHED AND 
    so.returnStatus != sou.returnStatus OR
    so.fullyPaid != sou.fullyPaid OR
    so.displayFulfillmentStatus != sou.displayFulfillmentStatus OR
    so.displayFinancialStatus != sou.displayFinancialStatus OR
    so.cancelReason != sou.cancelReason OR
    so.cancellation != sou.cancellation OR
    so.closed != sou.closed
    THEN 
    UPDATE SET
        so.createdAt = sou.createdAt,
        so.cancellation = sou.cancellation,
        so.cancelledAt = sou.cancelledAt,
        so.cancelReason = sou.cancelReason,
        so.cartDiscountAmountSet = sou.cartDiscountAmountSet,
        so.channelInformation = sou.channelInformation,
        so.closed = sou.closed,
        so.closedAt = sou.closedAt,
        so.currentCartDiscountAmountSet = sou.currentCartDiscountAmountSet,
        so.currentShippingPriceSet = sou.currentShippingPriceSet,
        so.currentSubtotalLineItemsQuantity = sou.currentSubtotalLineItemsQuantity,
        so.currentSubtotalPriceSet = sou.currentSubtotalPriceSet,
        so.currentTaxLines = sou.currentTaxLines,
        so.currentTotalAdditionalFeesSet = sou.currentTotalAdditionalFeesSet,
        so.currentTotalDiscountsSet = sou.currentTotalDiscountsSet,
        so.currentTotalPriceSet = sou.currentTotalPriceSet,
        so.currentTotalTaxSet = sou.currentTotalTaxSet,
        so.discountCode = sou.discountCode,
        so.discountCodes = sou.discountCodes,
        so.displayFinancialStatus = sou.displayFinancialStatus,
        so.displayFulfillmentStatus = sou.displayFulfillmentStatus,
        so.fullyPaid = sou.fullyPaid,
        so.originalTotalPriceSet = sou.originalTotalPriceSet,
        so.paymentGatewayNames = sou.paymentGatewayNames,
        so.refunds = sou.refunds,
        so.registeredSourceUrl = sou.registeredSourceUrl,
        so.returnStatus = sou.returnStatus,
        so.sourceName = sou.sourceName,
        so.subtotalLineItemsQuantity = sou.subtotalLineItemsQuantity,
        so.subtotalPriceSet = sou.subtotalPriceSet,
        so.totalDiscountsSet = sou.totalDiscountsSet,
        so.totalPriceSet = sou.totalPriceSet,
        so.transactions = sou.transactions,
        so.closedAt_utc = sou.closedAt_utc,
        so.createdAt_utc = sou.createdAt_utc,
        so.cancelledAt_utc = sou.cancelledAt_utc,
        so.updatedAt = sou.updatedAt

'''