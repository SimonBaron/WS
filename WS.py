"""Some code to prove that I can. Also, I'm going to learn Pandas."""

import pandas as pd
import numpy as np
import os


def array_uniques(array):
    """For aggregating columns by number of unique entries"""
    return len(np.unique(array))


def read_and_merge(directory):
    """Read in fact_order.csv and fact_orderline.csv
    and merge them for use in subsequent analyses.
    """
    data = pd.read_csv(directory + "fact_order.csv", parse_dates=['ts_placed'])
    data2 = pd.read_csv(directory + "fact_orderline.csv")
    data['month_placed'] = [m.month for m in data['ts_placed']]
    data2['goods_value'] = data2['current_quantity'] * data2['item_price_incVAT_per_unit']
    tojoin = data2.groupby('order_idno', as_index=False).agg(
        {'original_quantity': sum,
         'current_quantity': sum,
         'goods_value': sum})
    merged = pd.concat([data, tojoin], axis=1, join='inner')
    merged = merged.loc[:, ~merged.columns.duplicated()]
    # you learn new things everyday.
    return merged


def format_data(data):
    """Format raw data as suggested in q1 of the assignment
    ie group by website, order type and month of order, displaying
    various metric appropriately with sensible naming schemes.
    """
    formatted = raw_data[["order_idno",
                          "order_type",
                          "sitename",
                          "customer_idno",
                          "order_total_incVAT",
                          "month_placed",
                          "current_quantity",
                          "goods_value",
                          'original_order_value_incVAT']].groupby(
                              ['sitename',
                               'order_type',
                               'month_placed']).agg(
                                   {'order_idno': np.size,
                                    'customer_idno': array_uniques,
                                    'order_total_incVAT': sum,
                                    'original_order_value_incVAT': sum,
                                    'current_quantity': np.mean,
                                    'goods_value': sum})

    # rename columns

    formatted = formatted.rename(index=str, columns={
        "order_idno": "number_of_orders",
        "order_total_incVAT": "current_revenue",
        "original_order_value_incVAT": "original_revenue",
        "current_quantity": "average_basket_size",
        "customer_idno": "number_of_customers"})

    return formatted


def refunds(data, grouping='month_placed'):
    """Takes raw data from read_and_merge, analyses refunds
    grouping variable switches between refunds by month and refunds
    by sitename, but could be any column of raw data.
    """

    refund_data = data.loc[:,[
        "sitename",
        "order_type",
        "order_total_incVAT",
        "goods_value",
        "month_placed"]]
    refund_data["refunds"] = refund_data["order_total_incVAT"] - refund_data["goods_value"]
    refund_data = refund_data.groupby(grouping).agg({"refunds": sum,
                                             "order_total_incVAT": sum})
    refund_data["refund_value_as_percent"] = 100 * refund_data["refunds"] /refund_data["order_total_incVAT"]
    return refund_data

def refunds_by_classes(directory, class_id='class1'):
    """Read in product info for analysis"""
    orders = pd.read_csv(directory + "fact_orderline.csv")
    properties = pd.read_csv(directory + "product_properties.csv")
    properties_values = pd.read_csv(directory + "product_properties_values.csv")

    orders["refund_value"] = (orders["original_quantity"] - orders["current_quantity"]) * orders["item_price_incVAT_per_unit"]

    orders["sale_value"] = orders["item_price_incVAT_per_unit"] * orders["current_quantity"]     

    grouped = orders.groupby("product_idno").agg({"refund_value": sum, "sale_value": sum, "order_idno": np.size})

    class_index = properties[properties["property"] == class_id]["property_id"].values[0]

    class_required = properties_values[properties_values["property_id"] == class_index]
    
    classes = pd.concat([class_required, grouped], axis=1, join='inner')
    classes = classes.groupby("property_value").agg({"sale_value": sum, "refund_value": sum, "order_idno": sum})

    classes["refund_as_percentage"] = 100 * classes["refund_value"] / classes["sale_value"]

    return classes

def load_customer_activity(data):
    """Looking at raw data as provided in read_and_merge.
    Studies customer ordering patterns.
    """
    data["cumulative"] = 1
    data["recency"] = None

    for customer in np.unique(data["customer_idno"]):
        customer_map = data["customer_idno"] == customer
        if sum(customer_map) > 1:
            data.loc[customer_map, "cumulative"] = range(1, len(data[customer_map]) + 1)
            data.loc[customer_map, "recency"] = [None] + [np.timedelta64(data.loc[customer_map, "ts_placed"].values[i+1] - data.loc[customer_map, "ts_placed"].values[i], 'm') for i in range(sum(customer_map) - 1)]
    return data



def repeat_customers(data):
    """From customer activity data, analyse percentage of customers
    that order multiple times."""
    repeats = data[data["cumulative"] == 2]   # singly count repeats
    repeats = repeats.groupby("sitename", as_index=False).agg({"customer_idno": array_uniques})
    repeats["percentage_returning"] = 0
    for site in repeats["sitename"]:
        repeats.loc[repeats["sitename"] == site, "percentage_returning"] = 100 * repeats.loc[repeats["sitename"] == site, "customer_idno"] / float(sum((data["cumulative"] == 1) & (data["sitename"] == site)))
    return repeats

def repeat_in_30(data):
    """From customer activity data, analyse percentage of customers
    who re-order within 30 days"""

    repeats = data.loc[(data["cumulative"] == 2) & (data["recency"] < np.timedelta64(30, 'D')),:]
    repeats = repeats.groupby("sitename", as_index=False).agg({"customer_idno": array_uniques})
    repeats["percentage_returning_in30"] = 0
    for site in repeats["sitename"]:
        repeats.loc[repeats["sitename"] == site, "percentage_returning_in30"] = 100 * repeats.loc[repeats["sitename"] == site, "customer_idno"] / float(sum((data["cumulative"] == 1) & (data["sitename"] == site)))
    return repeats

pd.set_option('precision', 2)
d = os.getcwd()  # if you have the data in this directory
d = "/home/simon/Documents/WStest/"  # if you have the data elsewhere


## for Q1: ##
#raw_data = read_and_merge(d)
#formatted_data = format_data(raw_data)
#print formatted_data
#postage_table = (formatted_data["current_revenue"] - formatted_data["goods_value"]).to_frame("postage")
#print postage_table

## for Q2/3: ##

#raw_data = read_and_merge(d)
#print refunds(raw_data, ["sitename", "order_type"])#.to_html(float_format='{:,.2f}'.format)

## for Q4/5 ##
#print refunds_by_classes(d, class_id="class1")
#data = refunds_by_classes(d, class_id="supplier_id")

## customer behaviour ##

#raw_data = read_and_merge(d)
#data_customer = load_customer_activity(raw_data) # this is the one that takes time

#repeats = repeat_customers(data_customer)
#repeats_30 = repeat_in_30(data_customer)


