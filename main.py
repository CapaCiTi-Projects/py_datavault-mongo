from bson.codec_options import CodecOptions, TypeCodec, TypeRegistry
from bson.decimal128 import Decimal128
from dbmanager import DBManager, MySQLManager, MongoManager
from decimal import Decimal
from pymongo import MongoClient
from pymongo.results import InsertManyResult, UpdateResult

import atexit
import datetime as dtime
import pandas as pd
import pprint
import re
import urllib.parse


int_pattern = re.compile("^(\d+)$")


class DecimalCodec(TypeCodec):
    python_type = Decimal
    bson_type = Decimal128

    def transform_python(self, value):
        return Decimal128(value)

    def transform_bson(self, value):
        return value.to_decimal()


class DateCodec(TypeCodec):
    python_type = dtime.date
    bson_type = dtime.datetime

    def transform_python(self, value):
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    def transform_bson(self, value):
        return dtime.datetime.strptime("%Y-%m-%dT%H:%M:%S.%f%z").date()


def setup():
    # Mongo Initialiser
    Mongo_init()
    MySQL_init()


def MySQL_init():
    # MySQL Connection Initialise
    man = MySQLManager(passwd="2ZombiesEatBrains?", db="practice")
    man.setup_db()
    DBManager.store_data("mysql", man)


def Mongo_init():
    decimal_c = DecimalCodec()
    date_c = DateCodec()
    type_registry = TypeRegistry([decimal_c, date_c])
    codec_options = CodecOptions(type_registry=type_registry)

    man = MongoManager(codec_options=codec_options, user="admin08345",
                       host="cluster0.xjgrr.mongodb.net", passwd="GWSgnYU4pu7zs2S", db="DataTracker")
    DBManager.store_data("mongo", man)


def perform_operations():
    is_running = True
    functions = (
        transfer_products,
        create_top_3,
        drop_brands,
        update_product,
        create_worst_5_brands,
        quit_program
    )

    print("Welcome to DataVault Inc.")

    while is_running:
        print("What would you like to do today?")
        for idx, func in enumerate(functions):
            print(f"[{idx}]: {prettify_func_name(func.__name__)}")
        idx = get_int(input("Enter the action index: "))
        if idx is None:
            print("Invalid index selected")
            continue

        res = functions[idx]()
        if res[0] < 0:
            print(res[1])
            break
        else:
            print(res[1])
        print("")


def quit_program():
    return -1, "Quitting Program..."


def transfer_products():
    mysql = DBManager.retrieve_data("mysql")
    mongo = DBManager.retrieve_data("mongo")

    # Firstly, get MySQL for conversion.
    products_df = mysql.get_dbdata("products")
    categories_df = mysql.get_dbdata("categories")
    product_sales_df = mysql.get_dbdata("product_sales")

    products_df.rename(columns={"id_category": "category"}, inplace=True)
    for _, row in categories_df.iterrows():
        mask = products_df.loc[:, "category"] == row["id_category"]
        products_df.loc[mask, "category"] = row["title"]

    products_df.rename(columns={"id_product": "_id"}, inplace=True)
    products_data = products_df.to_dict(orient="records")
    for p in products_data:
        sales = product_sales_df.loc[product_sales_df.loc[:,
                                                          "id_product"] == p["_id"]].copy()
        sales.drop(columns=["id_sale", "id_product"], inplace=True)
        p["sales"] = sales.to_dict(orient="records")

        if p["brand"] is None:
            del p["brand"]

    # Insert data from MySQL into Mongo.
    db_datatracker = mongo.get_database()
    coll_products = db_datatracker["products"]

    coll_products.drop()
    insert_res = coll_products.insert_many(products_data)
    if isinstance(insert_res, InsertManyResult) and (len(insert_res.inserted_ids) > 0):
        # Insert Successful
        return 1, "Products Successfully Transferred."
    else:
        return 0, "Error Transferring Products"


def create_top_3():
    mysql = DBManager.retrieve_data("mysql")
    mongo = DBManager.retrieve_data("mongo")

    # c = ["id_product",
    #      "YEAR(period) as `sold_year`, AVG(sold) as `sold_average`"]
    # w = {"YEAR(period)": 2020}
    # g = "id_product"
    # o = "id_product DESC"
    # top3 = mysql.select(tablename="product_sales", columns=c,
    #                     where=w, groupby=g, order=o, limit=3)

    db_datatracker = mongo.get_database()
    coll_products = db_datatracker["products"]

    top3 = coll_products.aggregate([
        {"$match": {"sales.0": {"$exists": True}}},
        {"$unwind": "$sales"},
        {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}, "totalSales": {
            "$sum": "$sales.sold"}}},
        {"$replaceRoot": {
            "newRoot": {"$mergeObjects": ["$doc", {"totalSales": "$totalSales"}]}}},
        {"$unset": "sales"},
        {"$sort": {"totalSales": -1}},
        {"$limit": 3}
    ])

    data_top3 = list(top3)
    pos = 0
    for item in data_top3:
        item["position"] = pos
        pos += 1
    del pos

    coll_top_products = db_datatracker["top_products"]
    coll_top_products.drop()
    if (len(data_top3) > 0):
        insert_res = coll_top_products.insert_many(data_top3)
        if isinstance(insert_res, InsertManyResult) and (len(insert_res.inserted_ids) > 0):
            # Insert Successful
            return 1, "Top 3 Successfully Generate and Saved."
        else:
            return 0, "Error Generating Top 3 Products in Mongo."
    else:
        return 0, "Error Generating Top 3 Products. No Data was Found."


def drop_brands():
    mongo = DBManager.retrieve_data("mongo")

    db_datatracker = mongo.get_database()
    coll_top_products = db_datatracker["top_products"]

    data_top_products = coll_top_products.find({})
    drop_count = 0

    for item in data_top_products:
        if drop_count >= 2:
            break
        res_update = coll_top_products.update_one(
            {"_id": item["_id"]}, {"$unset": {"brand": ""}})
        if res_update.matched_count >= 1:
            drop_count += 1
    return 1, "Brands were Successfully Dropped and Stored."


def update_product():
    mongo = DBManager.retrieve_data("mongo")

    db_datatracker = mongo.get_database()
    coll_top_products = db_datatracker["top_products"]
    coll_products = db_datatracker["products"]

    res_update = coll_top_products.find_one_and_update(
        {}, {"$inc": {"totalSales": 250}, "$set": {"brand": "Shield"}})

    if res_update is None:
        return 0, "No products updated as no products with brands were found."

    # coll_products.find_one_and_update(res_update, {"brand": "Orange Juice"})
    return 1, "Succesfully update 1 document in the `top_products` collection."


def create_worst_5_brands():
    mongo = DBManager.retrieve_data("mongo")

    db_datatracker = mongo.get_database()
    coll_products = db_datatracker["products"]

    worst5 = coll_products.aggregate([
        {"$match": {"sales.0": {"$exists": True}, "brand": {"$exists": True}}},
        {"$unwind": "$sales"},
        {"$group": {"_id": "$brand", "totalSales": {"$sum": "$sales.sold"}}},
        {"$sort": {"totalSales": 1, "brand": 1}},
        {"$limit": 5}
    ])

    print("")
    print("The following are the 5 worst brands in the store.")

    for level, brand in enumerate(worst5):
        print(f"{level+1} -> {brand['_id']}")

    return 1, ""


def prettify_func_name(name):
    return " ".join(
        [s.capitalize() for s in name.split("_")]
    )


def get_int(val):
    match = int_pattern.search(val)
    return int(match.group(0)) if match is not None else None


if __name__ == "__main__":
    setup()
    perform_operations()
