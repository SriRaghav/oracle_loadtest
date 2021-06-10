import cx_Oracle
import names
import random
import sys
import json
import argparse
from queue import Queue


class OracleLoadTest:

    file_path = "/home/oracle_loadtest/oracle_cred.txt"

    def __init__(self):
        self.user = None
        self.password = None
        self.hostname = None
        self.service_id = None
        self.port_number = None
        self.schema_name = None
        self.table_name = None
        self.operation = None
        self.num_rows = 0
        self.is_mockup = False
        self.columns = []

    def read_creds_from_file(self):

        with open(self.file_path, "r") as reader:
            oracle_cred_json = json.loads(reader.read())

        self.user = oracle_cred_json["user"]
        self.password = oracle_cred_json["password"]
        self.hostname = oracle_cred_json["hostname"]
        self.service_id = oracle_cred_json["service_id"]
        self.port_number = oracle_cred_json["port_number"]

    @staticmethod
    def generate_namelist(number_records):
        name_list = []

        gender = ["male", "female"]
        for i in range(0, number_records):
            name_list.append(names.get_full_name(gender=random.choice(gender)))

        return name_list

    @staticmethod
    def generate_numlist(number_records, limit_min, limit_max):
        return random.sample(range(limit_min, limit_max), number_records)

    def insert(self, table_name, col_names, values):

        value_string = [":"+ str(i) for i in range(col_names)]
        print(value_string)

        try:
            connection_obj = cx_Oracle.connect(self.user, self.password, self.hostname + '/' + self.service_id)

            cursor_obj = connection_obj.cursor()
            query_builder = "insert into " + table_name + " (" + col_names + ") values (" + ",".join(value_string) + ")"
            cursor_obj.executemany(query_builder, values)

            connection_obj.commit()

            print("\nTable - %s & # INSERTED records - %s  " % (table_name, str(cursor_obj.rowcount)))

        except cx_Oracle.DatabaseError as e:
            print("Oracle DB Error!", e)

        finally:
            if cursor_obj:
                cursor_obj.close()
            if connection_obj:
                connection_obj.close()

    def select(self, table_name, column):

        try:
            with cx_Oracle.connect(self.user, self.password,
                                   self.hostname + ":" + self.port_number + '/' + self.service_id) as connection_obj:
                with connection_obj.cursor() as cursor_obj:

                    if column == "ALL":
                        query_builder = "select " + column + " from " + table_name
                    else:
                        query_builder = "select " + column + " from " + table_name + " order by " + column + " asc"

                    cursor_obj.execute(query_builder)

                    return [record[0] for record in cursor_obj.fetchall()]

            print("\nTable - %s & # FETCHED records - %s " % (table_name, str(cursor_obj.rowcount)))

        except cx_Oracle.DatabaseError as e:
            print("Oracle DB Error! - Select function", e)

    def table_utility(self, operation, table_name):

        try:
            with cx_Oracle.connect(self.user, self.password,
                                   self.hostname + ":" + self.port_number + '/' + self.service_id) as connection_obj:
                with connection_obj.cursor() as cursor_obj:

                    if operation == "list_columns":
                        query_builder = "select column_name from dba_tab_columns where table_name='" + olt.table_name + "' and owner='" + olt.schema_name + "'"
                    elif operation == "count":
                        query_builder = "select count(*) from " + table_name
                    elif operation == "list_tables":
                        count = 0
                    elif operation == "sample_row":
                        query_builder = "select * from (select * from " + table_name + " SAMPLE(1)) fetch first 1 rows " \
                                                                                       "only "

                    cursor_obj.execute(query_builder)

                    return cursor_obj.fetchall()

        except cx_Oracle.DatabaseError as e:
            print("Oracle DB Error - Describe function!", e)

    def update(self, table_name, column, values, update_many=True):

        try:
            with cx_Oracle.connect(self.user, self.password,
                                   self.hostname + ":" + self.port_number + '/' + self.service_id) as connection_obj:
                with connection_obj.cursor() as cursor_obj:
                    query_builder = "update " + str(table_name) + " set " + column + " = :1 where " + column + " = :2"
                    if update_many:
                        print(query_builder)
                        cursor_obj.executemany(query_builder, values)
                    else:
                        cursor_obj.execute("update Customers set CUSTOMER_ID = :1 where CUSTOMER_ID = :2", values)

                    connection_obj.commit()
                    print(values)
                    print("\nTable - %s & # UPDATED records - %s " % (table_name, str(cursor_obj.rowcount)))

        except cx_Oracle.DatabaseError as e:
            print("Oracle DB Error!", e)

    def delete(self, table_name, column, values, delete_many=True):

        try:
            with cx_Oracle.connect(self.user, self.password,
                                   self.hostname + ":" + self.port_number + '/' + self.service_id) as connection_obj:
                with connection_obj.cursor() as cursor_obj:
                    query_builder = "delete from " + table_name + " where " + column + "=:1"
                    if delete_many:
                        cursor_obj.executemany(query_builder, values)
                    else:
                        cursor_obj.execute(query_builder, values)

                    connection_obj.commit()

                    print("\nTable - %s & # DELETED records - %s " % (table_name, str(cursor_obj.rowcount)))

        except cx_Oracle.DatabaseError as e:
            print("Oracle DB Error!", e)


def main(olt):

    full_table_name = olt.schema_name + "." + olt.table_name
    olt.columns = [column[0] for column in olt.table_utility("list_columns", full_table_name)]

    if olt.operation == "insert":

        mockup_data = []
        if olt.is_mockup:
            random_record = list(olt.table_utility("sample_row", full_table_name)[0])
            for i in range(olt.num_rows):
                random_spec_id = random.randint(190000000, 200000000)
                random_record[2] = random_spec_id
                mockup_data.append(tuple(random_record))

            olt.insert(full_table_name, ",".join(olt.columns), mockup_data)

    elif olt.operation == "list_columns":
        print(olt.columns)

    elif olt.operation == "count" or olt.operation == "sample_row" or olt.operation == "list_tables":
        records = olt.table_utility(olt.operation, full_table_name)


    '''if olt.opertion == "insert":

        namelist = olt.generate_namelist(number_records)
        id_list = olt.generate_numlist(number_records, 2, 20000)
        records = [(id, name) for name, id in zip(namelist, id_list)]

        sample_id_list = random.sample(id_list, round(len(id_list) * 0.4))

        id_list_from_sample = random.choices(sample_id_list, k=number_records)
        value_list = olt.generate_numlist(number_records, 40000, 100000)
        order_records = [(id, value) for id, value in zip(id_list_from_sample, value_list)]

        customer_table_name, order_table_name = None, None

        if "Customers" in table_list:
            customer_table_name = "Customers"
        elif "CustomersIL" in table_list:
            customer_table_name = "CustomersIL"

        if customer_table_name is not None:
            olt.insert(customer_table_name, "CUSTOMER_ID, CUSTOMER_NAME", records)
            print("Sample Records below:(CUSTOMER_ID, CUSTOMER_NAME)")
            print(records[:2])

        if "Orders" in table_list:
            order_table_name = "Orders"
        elif "OrdersIL" in table_list:
            order_table_name = "OrdersIL"

        if order_table_name is not None:
            olt.insert(order_table_name, "CUSTOMER_ID, ORDER_VALUE", order_records)
            print("Sample Records below:(CUSTOMER_ID, ORDER_VALUE)")
            print(order_records[:2])'''

    if olt.operation == "update":

        records = olt.select(full_table_name, "CUSTOMER_ID")

        if len(records) >= olt.num_rows:
            old_values = random.sample(records, olt.num_rows)
            new_values = olt.generate_numlist(olt.num_rows, 10, 100000)
            update_values = [(old, new) for old, new in zip(old_values, new_values)]
            print(update_values)
            olt.update(olt.table_name, "CUSTOMER_ID", update_values)

    if olt.operation == "delete":

        records = olt.select(full_table_name, "CUSTOMER_ID")

        if len(records) >= olt.num_rows:
            old_vaules = random.sample(records, olt.num_rows)
            update_values = [(old,) for old in old_vaules]
            print(update_values)
            olt.delete(olt.table_name, "CUSTOMER_ID", update_values)
        else:
            print("Error: # Records in the table is " + str(len(records)))


if __name__ == "__main__":

    olt = OracleLoadTest()

    parser = argparse.ArgumentParser()
    parser.add_argument("--cred_from_file", action="store_true", dest="cred_from_file")
    parser.add_argument("--user", type=str, dest="user")
    parser.add_argument("--password", type=str, dest="password")
    parser.add_argument("--hostname", type=str, dest="hostname")
    parser.add_argument("--service_id", type=str, dest="service_id")
    parser.add_argument("--port_number", type=str, dest="port_number")
    parser.add_argument("--schema_name", type=str, dest="schema_name")
    parser.add_argument("--table_name", type=str, dest="table_name")
    parser.add_argument("--operation", type=str, choices=["insert", "update", "delete", "list_columns","list_tables", "count", "sample_row"], dest="operation",
                        required=True)
    parser.add_argument("--is_mockup", action="store_true", dest="is_mockup")
    parser.add_argument("--num_rows", type=int, dest="num_rows")

    args = parser.parse_args(namespace=olt)

    if args.cred_from_file:
        olt.read_creds_from_file()

    main(olt)