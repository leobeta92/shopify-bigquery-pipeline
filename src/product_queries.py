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
# ORDERS = os.getenv('ORDERS')
# ORDERS_UPDATE = os.getenv('ORDERS_UPDATE')

# PRODUCTS

yesterday_products = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
orders(query: "created_at:yesterday") {
edges {
node {
id
lineItems {
  edges {
    node {
      discountedTotalSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # closing discountedTotalSet
      discountedUnitPriceSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # closing discountedUnitPriceSet
      discountedUnitPriceAfterAllDiscountsSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # closing discountedUnitPriceAfterAllDiscountsSet
      id
      isGiftCard
      name
      originalTotalSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # originalTotalSet
      product {
        createdAt
        id
        isGiftCard
        status
        title
        totalInventory
      } # closing product
      quantity
      refundableQuantity
      sku
      originalUnitPriceSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # originalUnitPriceSet
      taxLines {
        priceSet {
          shopMoney {
            amount
          } # closing shopMoney        
        } # closing priceSet
      } # closing taxLines
            variant {
        inventoryItem {
          unitCost {
            amount
          } # closing unitCost
        } # closing inventoryItem
      } # closing variant
      totalDiscountSet {
          shopMoney {
            amount
          } # closing shopMoney         
      } # closing totalDiscountSet
    } # Closing node (lineItems)
  } # Closing edges (lineItems)

} # Closing lineItems

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

backfill_products = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
orders(query: "created_at:>=2026-02-23 created_at:<=2026-05-01") {
edges {
node {
id
lineItems {
  edges {
    node {
      discountedTotalSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # closing discountedTotalSet
      discountedUnitPriceSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # closing discountedUnitPriceSet
      discountedUnitPriceAfterAllDiscountsSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # closing discountedUnitPriceAfterAllDiscountsSet
      id
      isGiftCard
      name
      originalTotalSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # originalTotalSet
      product {
        createdAt
        id
        isGiftCard
        status
        title
        totalInventory
      } # closing product
      quantity
      refundableQuantity
      sku
      originalUnitPriceSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # originalUnitPriceSet
      taxLines {
        priceSet {
          shopMoney {
            amount
          } # closing shopMoney        
        } # closing priceSet
      } # closing taxLines
            variant {
        inventoryItem {
          unitCost {
            amount
          } # closing unitCost
        } # closing inventoryItem
      } # closing variant
      totalDiscountSet {
          shopMoney {
            amount
          } # closing shopMoney         
      } # closing totalDiscountSet
    } # Closing node (lineItems)
  } # Closing edges (lineItems)

} # Closing lineItems

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


# NEED TO MODIFY THIS AND SET UP QUERIES TO MODIFY DF FOR PRODUCTS.
last_30_days_pre_products =  """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
orders(query: "created_at:>={string_date} created_at:<={today_string}") {
edges {
node {
id
lineItems {
  edges {
    node {
      discountedTotalSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # closing discountedTotalSet
      discountedUnitPriceSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # closing discountedUnitPriceSet
      discountedUnitPriceAfterAllDiscountsSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # closing discountedUnitPriceAfterAllDiscountsSet
      id
      isGiftCard
      name
      originalTotalSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # originalTotalSet
      product {
        createdAt
        id
        isGiftCard
        status
        title
        totalInventory
      } # closing product
      quantity
      refundableQuantity
      sku
      originalUnitPriceSet {
        shopMoney {
          amount
        } # closing shopMoney
      } # originalUnitPriceSet
      taxLines {
        priceSet {
          shopMoney {
            amount
          } # closing shopMoney        
        } # closing priceSet
      } # closing taxLines
      totalDiscountSet {
          shopMoney {
            amount
          } # closing shopMoney         
      } # closing totalDiscountSet
            variant {
        inventoryItem {
          unitCost {
            amount
          } # closing unitCost
        } # closing inventoryItem
      } # closing variant
    } # Closing node (lineItems)
  } # Closing edges (lineItems)

} # Closing lineItems

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

last_30_days = (last_30_days_pre_products.replace("{string_date}", today_minus_30)).replace("{today_string}",today_string)