# Products Query

products_query = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
products {
edges {
node {
    id
    title
    createdAt
    handle
    isGiftCard
    status
    onlineStoreUrl
    metafield(key:\"clearance\" namespace:\"big\") {
      value
    } # closing metafield
    publishedAt
        } # Closing node
    } # Closing edges

} # Closing products
} # Closing query bracket
    \"""
  ) {
    bulkOperation {
      id
      status
    } # close bulkOperation
    userErrors {
      field
      message
    } # close userErrors
  } # close bulkOperationOpener 
} # close mutation
    """

# Variants Query

variants_query = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
productVariants {
    edges {
        node {
            id
            sku
            product {
              id
            } # closing product
            price
            inventoryItem {
              unitCost {
                amount
              } # closing unitCost
            } # closing inventoryItem
            availableForSale
            compareAtPrice
            createdAt
            defaultCursor
            inventoryQuantity
            title
            unitPrice {
                amount
            } # closing unitPrice
            updatedAt
        } # Closing node
    } # Closing edges

} # Closing products
} # Closing query bracket
    \"""
  ) {
    bulkOperation {
      id
      status
    } # close bulkOperation
    userErrors {
      field
      message
    } # close userErrors
  } # close bulkOperationOpener 
} # close mutation
    """