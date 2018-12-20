from google.cloud import bigquery
import pandas as pd

import seaborn as sns

import operator


def get_products_by_customer_fast(products_df_customers, customer_id):
    return list(set(products_df_customers.at[customer_id, 'product_id']))


def get_products_by_purchase_fast(products_df_purchase, purchase_id):
    return list(set(products_df_purchase.at[purchase_id, 'product_id']))


def get_purchases_by_customer_fast(products_df_customers, customer_id):
    return list(set(products_df_customers.at[customer_id, 'purchase_id']))


def get_previous_purchases_fast(customer, current_purchase_date):
    purchases_given_customer = list(set(products_df_customers.at[customer, 'purchase_id']))
    previous_purchases_fast = list()
    for purchase in purchases_given_customer:

        pure_date = products_df_purchases.at[purchase, 'day']

        if isinstance(pure_date, pd._libs.tslib.Timestamp):
            date = pure_date
        else:
            date = list(set(pure_date))[0]

        if date < current_purchase_date:
            previous_purchases_fast.append(purchase)

    return previous_purchases_fast


def get_days_from_last_purchase_fast(products_df_purchases, current_purchase_date, previous_purchases):
    previous_purchases_date = list()
    for purchase in previous_purchases:

        pure_date = products_df_purchases.at[purchase, 'day']

        if isinstance(pure_date, pd._libs.tslib.Timestamp):
            date = pure_date
        else:
            date = list(set(pure_date))[0]

        previous_purchases_date.append(date)

    return (current_purchase_date - max(previous_purchases_date))


def get_days_from_last_product_purchase_fast(product_id, previous_purchases, current_purchase_date,
                                             products_df_purchase):
    deltas = list()
    for purchase in previous_purchases:
        product_in_this_purchase = get_products_by_purchase_fast(products_df_purchase, purchase)

        pure_date = products_df_purchases.at[purchase, 'day']

        if isinstance(pure_date, pd._libs.tslib.Timestamp):
            date = pure_date
        else:
            date = list(set(pure_date))[0]

        if product_id in product_in_this_purchase:
            deltas.append(current_purchase_date - date)

    if len(deltas) == 0:
        return None
    else:
        return min(deltas)


def get_number_of_previous_product_purchases_fast(product_id, previous_purchases, products_df_purchase):
    number_of_previous_product_purchases = 0
    for purchase in previous_purchases:
        product_in_this_purchase = get_products_by_purchase_fast(products_df_purchase, purchase)
        if product_id in product_in_this_purchase:
            number_of_previous_product_purchases += 1

    return number_of_previous_product_purchases


def get_product_quantity_last_purchase_fast(product_id, previous_purchases, current_purchase_date,
                                            products_df_purchase, products_df_multi):
    purchases_with_dates = dict()
    for purchase in previous_purchases:

        pure_date = products_df_purchases.at[purchase, 'day']

        if isinstance(pure_date, pd._libs.tslib.Timestamp):
            date = pure_date
        else:
            date = list(set(pure_date))[0]

        purchases_with_dates[purchase] = date

    purch = max(purchases_with_dates.items(), key=operator.itemgetter(1))[0]

    product_in_this_purchase = get_products_by_purchase_fast(products_df_purchase, purch)

    try:
        quantity = products_df_multi.loc[purch].at[product_id, 'quantity']
    except KeyError:
        quantity = 0

    return quantity


def get_prediction_fast(products_df_purchase, product_id, purchase_id):
    purchase_products = get_products_by_purchase_fast(products_df_purchase, purchase_id)

    if product_id in purchase_products:
        return 1
    else:
        return 0

sns.set(color_codes=True)

client = bigquery.Client.from_service_account_json('/Users/jose/service_account.json')

products_query = """select * from shop.products"""
products_df = client.query(products_query).to_dataframe()
print(products_df.__len__())

products_df.head(2)

products_df_purchases = products_df.copy(deep=True)
products_df_customers = products_df.copy(deep=True)
products_df_products = products_df.copy(deep=True)
products_df_multi = products_df.copy(deep=True)

products_df_purchases.set_index(['purchase_id'], inplace=True)
products_df_purchases.head(2)

products_df_customers.set_index(['customer_id'], inplace=True)
products_df_customers.head(2)

products_df_products.set_index(['product_id'], inplace=True)
products_df_products.head(2)

products_df_multi.set_index(['purchase_id', 'product_id'], inplace=True)
products_df_multi.head(2)

products_df_purchases.sort_index(inplace=True)
products_df_customers.sort_index(inplace=True)
products_df_products.sort_index(inplace=True)
products_df_multi.sort_index(inplace=True)

dataset_df = pd.DataFrame(columns=['customer', 'purchase', 'product', 'number_of_previous_purchases',
                                   'number_of_previous_product_purchases', 'purchase_probability',
                                   'days_from_last_purchase', 'days_from_last_product_purchase',
                                   'product_quantity', 'prediction'])
data = []

try:
    customers = products_df['customer_id'].unique().tolist()
    for customer_id in customers:
        purchases = get_purchases_by_customer_fast(products_df_customers, customer_id)

        for purchase in purchases:

            pure_date = products_df_purchases.at[purchase, 'day']

            if isinstance(pure_date, pd._libs.tslib.Timestamp):
                current_purchase_date = pure_date
            else:
                current_purchase_date = list(set(pure_date))[0]

            previous_purchases = get_previous_purchases_fast(customer_id, current_purchase_date)

            number_of_previous_purchases = len(previous_purchases)

            # If it is the first purchase go out
            if number_of_previous_purchases == 0:
                continue

            days_from_last_purchase = float(get_days_from_last_purchase_fast(products_df_purchases, current_purchase_date,
                                                                         previous_purchases).days)

            products = get_products_by_customer_fast(products_df_customers, customer_id)

            for product in products:
                days_from_last_product_purchase = get_days_from_last_product_purchase_fast(product,
                                                                                       previous_purchases,
                                                                                       current_purchase_date,
                                                                                       products_df_purchases)
                # If it is the first product purchase go out
                if days_from_last_product_purchase == None:
                    continue

                days_from_last_product_purchase = float(days_from_last_product_purchase.days)

                product_quantity = get_product_quantity_last_purchase_fast(product,
                                                                       previous_purchases,
                                                                       current_purchase_date,
                                                                       products_df_purchases,
                                                                       products_df_multi)

                number_of_previous_product_purchases = get_number_of_previous_product_purchases_fast(product,
                                                                                                 previous_purchases,
                                                                                                 products_df_purchases)

                purchase_probability = round(
                    float(number_of_previous_product_purchases) / float(number_of_previous_purchases), 2)

                prediction = get_prediction_fast(products_df_purchases, product, purchase)

                try:
                    data.append({'customer': str(customer_id), 'purchase': str(purchase), 'product': str(product),
                             'number_of_previous_purchases': float(number_of_previous_purchases),
                             'number_of_previous_product_purchases': float(number_of_previous_product_purchases),
                             'purchase_probability': float(purchase_probability),
                             'days_from_last_purchase': float(days_from_last_purchase),
                             'days_from_last_product_purchase': float(days_from_last_product_purchase),
                             'product_quantity': float(product_quantity),
                             'prediction': int(prediction)})
                except TypeError:
                    print('ERROR!!')
                    print(customer_id)
                    print(purchase)
                    print(product)
                    print(number_of_previous_purchases)
                    print(number_of_previous_product_purchases)
                    print(purchase_probability)
                    print(days_from_last_purchase)
                    print(days_from_last_product_purchase)
                    print(product_quantity)
                    print(prediction)
                    continue

except KeyError:
    print(KeyError)

dataset_df = dataset_df.append(data, ignore_index=True)
print(dataset_df.__len__())
# print(dataset_df.head(5))
# print(dataset_df.dtypes)
# dataset_df.to_gbq('shop.purchases_data_set', 'prod-mercadona',
#               chunksize=10000, if_exists='append', verbose=False)

file_name = '/Users/jose/my_essentials_data_set.csv'
dataset_df.to_csv(file_name, sep=';')
print('done!')
