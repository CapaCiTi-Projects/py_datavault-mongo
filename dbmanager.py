from bson.codec_options import CodecOptions
from pymongo import MongoClient

import mysql.connector
import mysql.connector.errors
import numpy as np
import pandas as pd
import tkinter.messagebox as tkMessageBox
import tkinter.simpledialog as tkSimpleDialog
import urllib.parse


class DBColumn(object):
    def __init__(self, name, dtype="VARCHAR(45)", allow_nulls=True, auto_increment=False, default=None):
        self.name = name
        self.type = dtype
        self.allow_nulls = allow_nulls
        self.auto_increment = auto_increment
        self.default = default

    def __repr__(self):
        self.__str__()

    def __str__(self):
        out = ""

        out += "`" + self.name + "`"
        out += " " + self.type
        if not self.allow_nulls:
            out += " NOT NULL"
        if self.auto_increment:
            out += " AUTO_INCREMENT"
        if self.default is not None:
            out += " DEFAULT " + str(self.default)

        return out

    def can_self_generate(self):
        return self.allow_nulls or self.auto_increment or self.default is not None

    def get_name(self):
        return self.name


class DBManager:
    _config = {}
    _tables = {}
    _data_store = {}

    def __init__(self, tables=None, **conf):
        if tables is not None:
            self._tables = tables

        for key in conf:
            self.updateconfig_safe(key, conf[key])

    def open_connection(self):
        pass

    def get_table(self, tablename, cols_as_dict=False):
        for t in self._tables:
            if t["table"] == tablename:
                out = t.copy()
                out["fields"] = self.get_table_cols_dict(t)
                return out
        return None

    def does_table_exist(self, tablename):
        return self.get_table(tablename) is not None

    def get_table_cols(self, tablename):
        columns = [tuple(map(lambda x: x.get_name(), t["fields"]))
                   for t in self._tables if t["table"] == tablename]

        assert(len(columns) > 0),\
            f"`{tablename}` does not exist in known tables."
        return columns[0]

    def get_table_cols_full(self, table):
        if isinstance(table, str):
            columns = list(t["fields"]
                           for t in self._tables if t["table"] == table)

            assert(len(columns) > 0),\
                f"{table} is not a known table, please pass `{table}` directly or add it to the `tables` dictionary."
        elif isinstance(table, dict):
            columns = table["fields"]
        else:
            raise TypeError(
                "`table` parameter expects string or dictionary as type."
            )

        return columns

    def get_table_cols_dict(self, table):
        t = self.get_table_cols_full(table)
        return {col.get_name(): col for col in t}

    def updateconfig_safe(self, key, data):
        if key in self._config:
            self._config[key] = data

    def updateconfig(self, key, data):
        if key in self._config:
            self._config[key] = data
        else:
            raise KeyError(f"{key} does not exist in `tables`.")

    def getconfig(self, key):
        if key in self._config:
            return self._config[key]
        raise KeyError(f"{key} does not exist in `config`")

    def isconfigset(self, key):
        if key not in self._config:
            return False

        config = self._config[key]
        if isinstance(config, str) and config:
            return True
        elif isinstance(config, (list, dict)) and len(config) > 0:
            return True
        elif config is not None:
            return True
        return False

    @staticmethod
    def store_data(key, data, allow_overwrite=True):
        if allow_overwrite:
            DBManager._data_store[key] = data
        else:
            if key not in DBManager._data_store:
                DBManager._data_store[key] = data

    @staticmethod
    def retrieve_data(key):
        if key in DBManager._data_store:
            return DBManager._data_store[key]
        raise KeyError(f"{key} does not exist in `data store`")

    @staticmethod
    def isdataset(key):
        return key in DBManager._data_store


class MySQLManager(DBManager):
    _config = {
        "host": "localhost",
        "user": "root",
        "passwd": "",
        "db": "",
        "table": ""
    }

    # Store information about the tables for the database through a list of dictionaries
    # Fields Include:
    # table string The name of the table,
    # primary string The primray key of the table,
    # foreign tuple The foreign key data for the table,
    # fields tuple The fields for the table, represented using DBColumn
    _tables = [
        {
            "table": "products",  # name of the table
            "primary": "id_product",  # primary key of the table
            "foreign": (  # table foreign key
                "id_category",  # Key in current table
                "categories(id_category)"  # Reference to key in other table
            ),
            "fields": (  # the columns of the table
                DBColumn("id_product", dtype="INT",
                         allow_nulls=False, auto_increment=True),
                DBColumn("id_category", dtype="INT", allow_nulls=False),
                DBColumn("name", allow_nulls=False),
                DBColumn("brand"),
                DBColumn("stock_available", dtype="INT",
                         allow_nulls=False, default=0),
                DBColumn("selling_price", dtype="DECIMAL(13,2)",
                         allow_nulls=False, default=0.00)
            )
        },
        {
            "table": "categories",
            "primary": "id_category",
            "fields": (
                DBColumn("id_category", dtype="INT",
                         allow_nulls=False, auto_increment=True),
                DBColumn("title", allow_nulls=False)
            )
        },
        {
            "table": "product_sales",
            "primary": "id_sale",
            "foreign": (
                "id_product",
                "products(id_product)"
            ),
            "fields": (
                DBColumn("id_sale", dtype="INT",
                         allow_nulls=False, auto_increment=True),
                DBColumn("id_product", dtype="INT", allow_nulls=False),
                DBColumn("period", dtype="DATE", allow_nulls=False),
                DBColumn("sold", dtype="INT", allow_nulls=False)
            )
        }
    ]

    def __init__(self, tables=None, **conf):
        super().__init__(tables, **conf)
        self.setup_db()

    def open_connection(self, ignore_db=False):
        if self.isconfigset("passwd"):
            passwd = self.getconfig("passwd")
        if self.isconfigset("db") and not ignore_db:
            db = self.getconfig("db")

        connect_args = {}
        connect_args["host"] = self.getconfig("host")
        connect_args["user"] = self.getconfig("user")
        connect_args["passwd"] = passwd
        connect_args["database"] = db

        try:
            con = mysql.connector.connect(**connect_args)
        except mysql.connector.errors.InterfaceError:
            tkMessageBox.showerror(title="Connection Failed",
                                   message="Couldn't connect to the MySQL server, please check if it is running and available.")
            return
        return con

    def setup_db(self):
        if (not self.isconfigset("db")) and (not self._tables):
            return

        with self.open_connection() as con:
            cursor = con.cursor(prepared=True)

            for table in self._tables:
                sql_createtable = f"""CREATE TABLE IF NOT EXISTS `{table["table"]}` (
                    {",".join(str(field) for field in table["fields"])},
                    PRIMARY KEY ({table["primary"]})
                )"""

                cursor.execute(sql_createtable)

            for table in self._tables:
                if "foreign" not in table:
                    continue

                sql_alter = f"""ALTER TABLE `{table["table"]}`
                    ADD FOREIGN KEY ({table["foreign"][0]})
                    REFERENCES {table["foreign"][1]}
                """

                cursor.execute(sql_alter)

    def select(self, tablename: str = "", columns: list = [], where: dict = {}, groupby: str = "", order: str = "", limit: int = 5):
        with self.open_connection() as con:
            cursor = con.cursor()
            cols = ",".join(columns)
            where_clause = " AND ".join([f"{k} = {where[k]}" for k in where])

            stmt=""
            stmt += f"SELECT {cols}" + "\n"
            stmt += f"FROM {tablename}" + "\n"
            if len(where) > 0:
                stmt += f"WHERE {where_clause}" + "\n"
            if groupby:
                stmt += f"GROUP BY {groupby}" + "\n"
            if order:
                stmt += f"ORDER BY {order}" + "\n"
            stmt += f"LIMIT {limit}" + "\n"

            cursor.execute(stmt)
            return cursor.fetchall()

    def get_dbdata(self, table: str = None) -> pd.DataFrame:
        if self.isconfigset("table"):
            tablename=self.getconfig("table")
        tablename=table or tablename

        if not self.does_table_exist(tablename):
            return

        data=[]
        with self.open_connection() as con:
            cursor=con.cursor()
            cols=self.get_table_cols(tablename)

            cursor.execute(f"""
                SELECT {",".join(cols)} FROM {tablename}
            """)
            data=cursor.fetchall()

        data_df=pd.DataFrame(data, columns = cols)
        return data_df

    def add_df_to_db(self, df, table: str = "", suppress: str = ""):
        if (not table) and (not self.isconfigset("table")):
            raise LookupError(
                "There is no table available for CRUD operations."
            )
        else:
            db_table=table if table else self.getconfig("table")

        if not self.does_table_exist(db_table):
            raise LookupError(
                "The specified table is not specified in the `table` dict or does not exist."
            )

        with self.open_connection() as con:
            cursor=con.cursor()

            left_df=self.get_dbdata(table = db_table)
            out_df=left_df.merge(df, how = "outer", indicator = "shared")

            df_insert=out_df.loc[out_df.loc["shared"] == "right_only"].copy()
            df_delete=out_df.loc[out_df.loc["shared"] == "left_only"].copy()

            out_df=out_df.drop(columns = ["shared"])
            df_insert=df_insert.drop(columns = ["shared"])
            df_delete=df_insert.drop(columns = ["shared"])

            if len(out_df) == 0:
                tkMessageBox.showinfo(title = "DataBase Update Complete",
                                      message = "Nothing was added to the DB as no changes were detected between the different datasets.")
                return

            current_table=self.get_table(db_table, cols_as_dict = True)
            table_cols=current_table["fields"]
            table_pk=current_table["primary"]

            cols_insert="`,`".join([str(i)
                                      for i in df_insert.columns.tolist()])
            sql_delete=f"""
                DELETE FROM `{db_table}` WHERE {table_pk}=%s
            """

            if len(df_insert) > 0:
                for _, row in df_insert.iterrows():
                    for index, value in row.iteritems():
                        if pd.isna(value) and table_cols[index].can_self_generate():
                            row=row.drop(index = [index])
                            continue
                        if type(value) == np.int64:
                            row.loc[index]=int(value)

                    cols_insert="`,`".join([str(i)
                                              for i in row.index.tolist()])
                    sql_insert=f"""
                        INSERT INTO `{db_table}` (`{cols_insert}`)
                        VALUES ({"%s," * (len(row.index)-1)}%s)
                    """

                    cursor.execute(sql_insert, tuple(row))
            if len(df_delete) > 0:
                # data_delete = df_delete.to_dict(orient="records")
                # cursor.executemany(sql_delete, data_delete)
                cursor.executemany(sql_delete, df_delete)

            try:
                con.commit()

                if (suppress == "success") or (suppress == "all"):
                    tkMessageBox.showinfo(title = "Save Successful",
                                          message = "Save Completed Successfully!")
            except mysql.connector.ProgrammingError:
                con.rollback()

                if (suppress == "error") or (suppress == "all"):
                    tkMessageBox.showinfo(title = "Save Failed",
                                          message = "There was a problem with the supplied SQL statement.")
            except Exception as err:
                con.rollback()

                if (suppress == "error") or (suppress == "all"):
                    tkMessageBox.showinfo(title = "Save Successful",
                                          message = err)


class MongoManager(DBManager):
    _config={
        "host": "",
        "user": "",
        "passwd": "",
        "db": "",
        "codec_options": None
    }

    def __init__(self, tables=None, **conf):
        super().__init__(tables=tables, **conf)

    def open_connection(self):
        """Open a connection to the database using the data store in DBManager._config.
        Retrieve using the static method, getconfig()."""

        # pword = urllib.parse.quote("GWSgnYU4pu7zs2S")
        # dbname = urllib.parse.quote("DataTracker")
        # con = MongoClient(
        #     f"mongodb+srv://admin08345:{pword}@cluster0.xjgrr.mongodb.net/{dbname}?retryWrites=true&w=majority")
        try:
            host = urllib.parse.quote(self.getconfig("host"))
            user = urllib.parse.quote(self.getconfig("user"))
            dbname = urllib.parse.quote(self.getconfig("db"))
            passwd = urllib.parse.quote(self.getconfig("passwd"))
        except KeyError:
            raise ValueError(
                "Please Ensure that all of the configuration settings needed to open a connection are available."
                "These required options are: `host`, `user`, `dbname`, `passwd`.")

        # try:
        con=MongoClient(
            f"mongodb+srv://{user}:{passwd}@{host}/{dbname}?retryWrites=true&w=majority")
        # except :
        #     tkMessageBox.showerror(title="Connection Failed",
        #                            message="Couldn't connect to the MySQL server, please check if it is running and available.")
        #     return

        return con

    def get_database(self, db=None):
        if db == None:
            db = self.getconfig("db")

        with self.open_connection() as client:
            if self.isconfigset("codec_options"):
                codec_options = self.getconfig("codec_options")
                return client.get_database(db, codec_options=codec_options)
            return client.get_database(db)


if __name__ == "__main__":
    pass
