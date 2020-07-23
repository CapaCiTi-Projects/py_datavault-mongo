import mysql.connector
import mysql.connector.errors
import numpy as np
import pandas as pd
import tkinter.messagebox as tkMessageBox
import tkinter.simpledialog as tkSimpleDialog


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


class DBManager(object):
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
        }
    ]

    # Used for storing data, accessible via `store_data` & `retrieve_data`.
    _data_store = {}

    @staticmethod
    def init(data=None, tables=None, **conf):
        """Initialise the DbManager data dictionaries, ie '_data', '_conf' & '_tables'."""
        for key in data:
            DBManager.store_data(key, data[key])

        if tables is not None:
            DBManager._tables = tables

        for key in conf:
            DBManager.updateconfig_safe(key, conf[key])

        DBManager.setup_db()

    @staticmethod
    def open_connection(ignore_db=False):
        """Open a connection to the database using the data store in DBManager._config.
        Retrieve using the static method, getconfig()."""

        connect_args = {}
        connect_args["host"] = DBManager.getconfig("host")
        connect_args["user"] = DBManager.getconfig("user")

        passwd = ""
        if DBManager.isconfigset("passwd"):
            passwd = DBManager.getconfig("passwd")
        connect_args["passwd"] = passwd

        if DBManager.isconfigset("db") and not ignore_db:
            connect_args["database"] = DBManager.getconfig("db")

        try:
            con = mysql.connector.connect(**connect_args)
        except mysql.connector.errors.InterfaceError:
            tkMessageBox.showerror(title="Connection Failed",
                                   message="Couldn't connect to the MySQL server, please check if it is running and available.")
            return

        return con

    @staticmethod
    def setup_db():
        """Setup the DataBase by creating all of the tables stored in the `_tables` dict."""
        if (not DBManager.isconfigset("db")) or (not DBManager._tables):
            return

        with DBManager.open_connection() as con:
            cursor = con.cursor(prepared=True)

            # Create all of the tables.
            for table in DBManager._tables:
                sql_createtable = f"""CREATE TABLE IF NOT EXISTS `{table["table"]}` (
                    {",".join(str(field) for field in table["fields"])},
                    PRIMARY KEY ({table["primary"]})
                )"""

                cursor.execute(sql_createtable)

            # Add foreign keys to the tables.
            for table in DBManager._tables:
                if "foreign" not in table:
                    continue

                sql_alter = f"""ALTER TABLE `{table["table"]}`
                    ADD FOREIGN KEY ({table["foreign"][0]})
                    REFERENCES {table["foreign"][1]}
                """

                cursor.execute(sql_alter)

    @staticmethod
    def get_table(tablename, cols_as_dict=False):
        for t in DBManager._tables:
            if t["table"] == tablename:
                out = t.copy()
                out["fields"] = DBManager.get_table_cols_dict(t)
                return out
        return None

    @staticmethod
    def does_table_exist(tablename):
        return DBManager.get_table(tablename) is not None

    @staticmethod
    def get_table_cols(tablename):
        """Get a tuple with the names of all of the columns in the specified table."""
        columns = [tuple(map(lambda x: x.get_name(), t["fields"]))
                   for t in DBManager._tables if t["table"] == tablename]

        assert(len(columns) >
               0), f"`{tablename}` does not exist in known tables."
        return columns[0]

    @staticmethod
    def get_table_cols_full(table):
        """Get the full `DBColumn` tuple at table."""
        if isinstance(table, str):
            columns = list(t["fields"]
                           for t in DBManager._tables if t["table"] == table)

            assert(len(columns) >
                   0), f"`{table}` is not a known table, please pass `{table}` directly or add it to the `tables` dictionary."
        elif isinstance(table, dict):
            columns = table["fields"]
        else:
            raise TypeError(
                "`table` parameter expects string or dictionary as type.")

        return columns

    @staticmethod
    def get_table_cols_dict(table):
        t = DBManager.get_table_cols_full(table)
        return {col.get_name(): col for col in t}

    @staticmethod
    def updateconfig_safe(key, data):
        """This method is the same as updateconf(), but is considered safe as
        it doesn't overwrite any config data if it already has a value."""
        if key in DBManager._config:
            if not DBManager.isconfigset(key):
                DBManager._config[key] = data
        else:
            raise KeyError(f"{key} does not exists in DBManager.tables")

    @ staticmethod
    def updateconfig(key, data):
        """Update data in the `_conf` dictionary"""
        if key in DBManager._config:
            DBManager._config[key] = data
            raise KeyError(f"{key} does not exist in `tables`")

    @ staticmethod
    def getconfig(key):
        """Get the data at key in the `_conf` dict"""
        if key in DBManager._config:
            return DBManager._config[key]
        raise KeyError(f"{key} does not exist in `config`")

    @ staticmethod
    def isconfigset(key):
        """Check to see if key exists in _conf and if a value is set."""
        return (key in DBManager._config) and (DBManager._config[key])

    @ staticmethod
    def store_data(key, data, allow_overwrite=True):
        """Method to store data in the `_data` dict. Can be used for any data."""
        if allow_overwrite:
            DBManager._data_store[key] = data
        else:
            if key not in DBManager._data_store:
                DBManager._data_store[key] = data

    @ staticmethod
    def retrieve_data(key):
        """Method to retrieve data from the `_data` dict using key."""
        if key in DBManager._data_store:
            return DBManager._data_store[key]
        raise KeyError(f"{key} does not exist in `data store`")

    @staticmethod
    def isdataset(key):
        return key in DBManager._data_store

    @staticmethod
    def add_to_table(table, *data):
        pass

    @staticmethod
    def get_dbdata(table: str = None) -> pd.DataFrame:
        """ Method to get the data from the database and return it as a tuple consisting
        of a list of the names of the columns and a list of the actualy data in tuple format."""
        if DBManager.isconfigset("table"):
            tablename = DBManager.getconfig("table")
        tablename = table or tablename

        if not DBManager.does_table_exist(tablename):
            return

        with DBManager.open_connection() as con:
            cursor = con.cursor()

            cols = DBManager.get_table_cols(tablename)

            cursor.execute(
                f"SELECT {','.join(cols)} FROM {tablename}")
            data = cursor.fetchall()

        data_df = pd.DataFrame(data, columns=cols)

        return data_df

    @staticmethod
    def add_df_to_db(df, table: str = "", suppress=""):
        if (not table) and (not DBManager.isconfigset("table")):
            raise LookupError(
                "There is no table specified to use for CRUD operations.")
        else:
            db_table = table if table else DBManager.getconfig("table")

        if not DBManager.does_table_exist(db_table):
            raise LookupError(
                "The specified table is not specified in `table` dict or does not exist.")

        with DBManager.open_connection() as con:
            cursor = con.cursor()

            # Firstly, get original dataframe, using get_db_data()
            left_df = DBManager.get_dbdata(table=db_table)

            # Then, compare the the two and only take the ones that have differences
            out_df = left_df.merge(df, how="outer", indicator="shared")

            df_insert = out_df[out_df["shared"] == "right_only"].copy()
            df_delete = out_df[out_df["shared"] == "left_only"].copy()

            out_df = out_df.drop(["shared"], axis=1)
            df_insert = df_insert.drop(["shared"], axis=1)
            df_delete = df_delete.drop(["shared"], axis=1)

            if len(out_df) == 0:
                tkMessageBox.showinfo(title="DataBase Update Complete",
                                      message="Nothing was added to the DB as no changes were detected between the different datasets.")
                return

            current_table = DBManager.get_table(db_table, cols_as_dict=True)
            table_cols = current_table["fields"]
            table_pk = current_table["primary"]

            cols_insert = "`,`".join([str(i)
                                      for i in df_insert.columns.tolist()])
            sql_delete = f"DELETE FROM `{db_table}` WHERE {table_pk}=%s"

            # try:
            if len(df_insert) > 0:
                for _, row in df_insert.iterrows():
                    for index, value in row.iteritems():
                        if pd.isna(value) and table_cols[index].can_self_generate():
                            row = row.drop(index=[index])
                            continue
                        if type(value) == np.int64:
                            row.loc[index] = int(value)
                    # if not row.loc[table_pk] or pd.isna(row.loc[table_pk]):
                    #     row = row.drop(index=[table_pk])
                    # print(row)

                    cols_insert = "`,`".join([str(i)
                                              for i in row.index.tolist()])

                    sql_insert = (f"INSERT INTO `{db_table}` (`" + cols_insert +
                                  "`) VALUES (" + "%s," * (len(row.index)-1) + "%s)")
                    print(tuple(row))
                    cursor.execute(sql_insert, tuple(row))
            if len(df_delete) > 0:
                cursor.executemany(sql_delete, df_delete)
            con.commit()

            if (suppress == "success") or (suppress == "all"):
                tkMessageBox.showinfo(title="Save Successful",
                                      message="Save Completed Successfully!")
            # except Exception as err:
            #     con.rollback()

            #     if (suppress == "error") or (suppress == "all"):
            #         tkMessageBox.showerror(title="Save Failed",
            #                                message=f"The data was not saved to the DB.\n{err}")


if __name__ == "__main__":
    import test
