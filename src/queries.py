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
