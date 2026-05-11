collections_query = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
collections {
edges {
node {
    id
    title
    products {
      edges {
        node {
          id
        } # closing edges
      } # closing node
    } # closing products
    productsCount {
      count
    } # productsCount
        } # Closing node
    } # Closing edges

} # Closing collections
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