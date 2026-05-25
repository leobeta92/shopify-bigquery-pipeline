import datetime as dt

# For moving time-based queries.
today = dt.date.today()
today_string = dt.date.strftime(dt.date.today(),'%Y-%m-%d')
today_minus_30 = dt.date.strftime(today - dt.timedelta(days = 30),'%Y-%m-%d')
today_minus_15 = dt.date.strftime(today - dt.timedelta(days = 15),'%Y-%m-%d')
today_minus_7 = dt.date.strftime(today - dt.timedelta(days = 7),'%Y-%m-%d')

# ORDERS
# Yesterday query for orders

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
billingAddress {
    address1
    address2
    city
    company
    coordinatesValidated
    country
    countryCodeV2
    firstName
    formattedArea
    id
    lastName
    latitude
    longitude
    name
    phone
    province
    provinceCode
    timeZone
    validationResultSummary
    zip
} # closing billingAddress
billingAddressMatchesShippingAddress
shippingAddress {
    address1
    address2
    city
    company
    coordinatesValidated
    country
    countryCodeV2
    firstName
    formattedArea
    id
    lastName
    latitude
    longitude
    name
    phone
    province
    provinceCode
    timeZone
    validationResultSummary
    zip
} # closing shippingAddress

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



backfill_addresses = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
orders(query: "created_at:>=2026-02-23 created_at:<=2026-05-20") {
edges {
node {
id
name
email
createdAt
billingAddress {
    address1
    address2
    city
    company
    coordinatesValidated
    country
    countryCodeV2
    firstName
    formattedArea
    id
    lastName
    latitude
    longitude
    name
    phone
    province
    provinceCode
    timeZone
    validationResultSummary
    zip
} # closing billingAddress
billingAddressMatchesShippingAddress
shippingAddress {
    address1
    address2
    city
    company
    coordinatesValidated
    country
    countryCodeV2
    firstName
    formattedArea
    id
    lastName
    latitude
    longitude
    name
    phone
    province
    provinceCode
    timeZone
    validationResultSummary
    zip
} # closing shippingAddress

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
last_week_pre = """
        mutation {
  bulkOperationRunQuery(
   query: \"""
    query { 
orders(query: "created_at:>={string_date} created_at:<=yesterday") {
edges {
node {
id
name
email
createdAt
billingAddress {
    address1
    address2
    city
    company
    coordinatesValidated
    country
    countryCodeV2
    firstName
    formattedArea
    id
    lastName
    latitude
    longitude
    name
    phone
    province
    provinceCode
    timeZone
    validationResultSummary
    zip
} # closing billingAddress
billingAddressMatchesShippingAddress
shippingAddress {
    address1
    address2
    city
    company
    coordinatesValidated
    country
    countryCodeV2
    firstName
    formattedArea
    id
    lastName
    latitude
    longitude
    name
    phone
    province
    provinceCode
    timeZone
    validationResultSummary
    zip
} # closing shippingAddress

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

last_week = last_week_pre.replace("{string_date}", today_minus_7)


