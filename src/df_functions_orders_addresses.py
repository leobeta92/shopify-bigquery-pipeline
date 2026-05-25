import pandas as pd
import json
import datetime

def columns_for_addresses_info_df():
    columns = [
            'id',
            'name',
            'email',
            'createdAt',
            'billing_address1',
            'billing_address2',
            'billing_city',
            'billing_company',
            'billing_coordinatesValidated',
            'billing_country',
            'billing_countryCodeV2',
            'billing_firstName',
            'billing_formattedArea',
            'billing_id',
            'billing_lastName',
            'billing_latitude',
            'billing_longitude',
            'billing_name',
            'billing_phone',
            'billing_province',
            'billing_provinceCode',
            'billing_timeZone',
            'billing_validationResultSummary',
            'billing_zip',
            'billingAddressMatchesShippingAddress',
            'shipping_address1',
            'shipping_address2',
            'shipping_city',
            'shipping_company',
            'shipping_coordinatesValidated',
            'shipping_country',
            'shipping_countryCodeV2',
            'shipping_firstName',
            'shipping_formattedArea',
            'shipping_id',
            'shipping_lastName',
            'shipping_latitude',
            'shipping_longitude',
            'shipping_name',
            'shipping_phone',
            'shipping_province',
            'shipping_provinceCode',
            'shipping_timeZone',
            'shipping_validationResultSummary',
            'shipping_zip',
        ]
    return columns

def create_order_address_info_df(orders):

    # Resetting data frame
    orders_df = pd.DataFrame(columns= columns_for_addresses_info_df())

    # Resetting objects
    id = []
    name = []
    email = []
    createdAt = []
    billing_address1 = []
    billing_address2 = []
    billing_city = []
    billing_company = []
    billing_coordinatesValidated = []
    billing_country = []
    billing_countryCodeV2 = []
    billing_firstName = []
    billing_formattedArea = []
    billing_id = []
    billing_lastName = []
    billing_latitude = []
    billing_longitude = []
    billing_name = []
    billing_phone = []
    billing_province = []
    billing_provinceCode = []
    billing_timeZone = []
    billing_validationResultSummary = []
    billing_zip = []
    billingAddressMatchesShippingAddress = []
    shipping_address1 = []
    shipping_address2 = []
    shipping_city = []
    shipping_company = []
    shipping_coordinatesValidated = []
    shipping_country = []
    shipping_countryCodeV2 = []
    shipping_firstName = []
    shipping_formattedArea = []
    shipping_id = []
    shipping_lastName = []
    shipping_latitude = []
    shipping_longitude = []
    shipping_name = []
    shipping_phone = []
    shipping_province = []
    shipping_provinceCode = []
    shipping_timeZone = []
    shipping_validationResultSummary = []
    shipping_zip = []


 # Looping through data
    for response in orders:
        
        id.append(response['id'])
        name.append(response['name'])
        email.append(response['email'])
        createdAt.append(response['createdAt'])
        billing_address1.append(response['billingAddress']['address1'])
        billing_address2.append(response['billingAddress']['address2'])
        billing_city.append(response['billingAddress']['city'])
        billing_company.append(response['billingAddress']['company'])
        billing_coordinatesValidated.append(response['billingAddress']['coordinatesValidated'])
        billing_country.append(response['billingAddress']['country'])
        billing_countryCodeV2.append(response['billingAddress']['countryCodeV2'])
        billing_firstName.append(response['billingAddress']['firstName'])
        billing_formattedArea.append(response['billingAddress']['formattedArea'])
        billing_id.append(response['billingAddress']['id'])
        billing_lastName.append(response['billingAddress']['lastName'])
        billing_latitude.append(response['billingAddress']['latitude'])
        billing_longitude.append(response['billingAddress']['longitude'])
        billing_name.append(response['billingAddress']['name'])
        billing_phone.append(response['billingAddress']['phone'])
        billing_province.append(response['billingAddress']['province'])
        billing_provinceCode.append(response['billingAddress']['provinceCode'])
        billing_timeZone.append(response['billingAddress']['timeZone'])
        billing_validationResultSummary.append(response['billingAddress']['validationResultSummary'])
        billing_zip.append(response['billingAddress']['zip'])
        billingAddressMatchesShippingAddress.append(response['billingAddressMatchesShippingAddress'])
        if response['shippingAddress'] is not None:
            shipping_address1.append(response['shippingAddress']['address1'])
            shipping_address2.append(response['shippingAddress']['address2'])
            shipping_city.append(response['shippingAddress']['city'])
            shipping_company.append(response['shippingAddress']['company'])
            shipping_coordinatesValidated.append(response['shippingAddress']['coordinatesValidated'])
            shipping_country.append(response['shippingAddress']['country'])
            shipping_countryCodeV2.append(response['shippingAddress']['countryCodeV2'])
            shipping_firstName.append(response['shippingAddress']['firstName'])
            shipping_formattedArea.append(response['shippingAddress']['formattedArea'])
            shipping_id.append(response['shippingAddress']['id'])
            shipping_lastName.append(response['shippingAddress']['lastName'])
            shipping_latitude.append(response['shippingAddress']['latitude'])
            shipping_longitude.append(response['shippingAddress']['longitude'])
            shipping_name.append(response['shippingAddress']['name'])
            shipping_phone.append(response['shippingAddress']['phone'])
            shipping_province.append(response['shippingAddress']['province'])
            shipping_provinceCode.append(response['shippingAddress']['provinceCode'])
            shipping_timeZone.append(response['shippingAddress']['timeZone'])
            shipping_validationResultSummary.append(response['shippingAddress']['validationResultSummary'])
            shipping_zip.append(response['shippingAddress']['zip'])

        else:
            shipping_address1.append("Not Provided")
            shipping_address2.append("Not Provided")
            shipping_city.append("Not Provided")
            shipping_company.append("Not Provided")
            shipping_coordinatesValidated.append(False)
            shipping_country.append("Not Provided")
            shipping_countryCodeV2.append("Not Provided")
            shipping_firstName.append("Not Provided")
            shipping_formattedArea.append("Not Provided")
            shipping_id.append("Not Provided")
            shipping_lastName.append("Not Provided")
            shipping_latitude.append(float("nan"))
            shipping_longitude.append(float("nan"))
            shipping_name.append("Not Provided")
            shipping_phone.append("Not Provided")
            shipping_province.append("Not Provided")
            shipping_provinceCode.append("Not Provided")
            shipping_timeZone.append("Not Provided")
            shipping_validationResultSummary.append("Not Provided")
            shipping_zip.append("Not Provided")
    
        
    # Populate new table
    orders_df['id'] = id
    orders_df['name'] = name
    orders_df['email'] = email
    orders_df['createdAt'] = createdAt
    orders_df['billing_address1'] = billing_address1
    orders_df['billing_address2'] = billing_address2
    orders_df['billing_city'] = billing_city
    orders_df['billing_company'] = billing_company
    orders_df['billing_coordinatesValidated'] = billing_coordinatesValidated
    orders_df['billing_country'] = billing_country
    orders_df['billing_countryCodeV2'] = billing_countryCodeV2
    orders_df['billing_firstName'] = billing_firstName
    orders_df['billing_formattedArea'] = billing_formattedArea
    orders_df['billing_id'] = billing_id
    orders_df['billing_lastName'] = billing_lastName
    orders_df['billing_latitude'] = billing_latitude
    orders_df['billing_longitude'] = billing_longitude
    orders_df['billing_name'] = billing_name
    orders_df['billing_phone'] = billing_phone
    orders_df['billing_province'] = billing_province
    orders_df['billing_provinceCode'] = billing_provinceCode
    orders_df['billing_timeZone'] = billing_timeZone
    orders_df['billing_validationResultSummary'] = billing_validationResultSummary
    orders_df['billing_zip'] = billing_zip
    orders_df['billingAddressMatchesShippingAddress'] = billingAddressMatchesShippingAddress
    orders_df['shipping_address1'] = shipping_address1
    orders_df['shipping_address2'] = shipping_address2
    orders_df['shipping_city'] = shipping_city
    orders_df['shipping_company'] = shipping_company
    orders_df['shipping_coordinatesValidated'] = shipping_coordinatesValidated
    orders_df['shipping_country'] = shipping_country
    orders_df['shipping_countryCodeV2'] = shipping_countryCodeV2
    orders_df['shipping_firstName'] = shipping_firstName
    orders_df['shipping_formattedArea'] = shipping_formattedArea
    orders_df['shipping_id'] = shipping_id
    orders_df['shipping_lastName'] = shipping_lastName
    orders_df['shipping_latitude'] = shipping_latitude
    orders_df['shipping_longitude'] = shipping_longitude
    orders_df['shipping_name'] = shipping_name
    orders_df['shipping_phone'] = shipping_phone
    orders_df['shipping_province'] = shipping_province
    orders_df['shipping_provinceCode'] = shipping_provinceCode
    orders_df['shipping_timeZone'] = shipping_timeZone
    orders_df['shipping_validationResultSummary'] = shipping_validationResultSummary
    orders_df['shipping_zip'] = shipping_zip

    return orders_df