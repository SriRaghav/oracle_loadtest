import cx_Oracle
import names
import random
import sys


class OracleLoadTest:

    def __init__(self, user, password, hostname, service_id, port_number):
        self.user = user
        self.password = password
        self.hostname = hostname
        self.service_id = service_id
        self.port_number = port_number

    @staticmethod
    def generate_namelist(number_records):
        name_list = []

        gender = ["male", "female"]
        for i in range(0, number_records):
            name_list.append(names.get_full_name(gender=random.choice(gender)))

        return name_list

    @staticmethod
    def generate_numlist(number_records, limit_min, limit_max):

        id_list = []
        i = 0

        while i <= number_records:

            rand_num = random.randint(limit_min, limit_max)
            if rand_num not in id_list:
                id_list.append(rand_num)
                i += 1

        return id_list

    def insert(self, table_name, col_names, values):

        try:
            connection_obj = cx_Oracle.connect(self.user, self.password, self.hostname + '/' + self.service_id)

            cursor_obj = connection_obj.cursor()
            query_builder = "insert into " + table_name + " (" + col_names + ") values (:1, :2)"
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
            with cx_Oracle.connect(self.user,
                                   self.password,
                                   self.hostname + '/' + self.service_id) as connection_obj:
                with connection_obj.cursor() as cursor_obj:
                    query_builder = "select " + column + " from " + table_name
                    cursor_obj.execute(query_builder)

                    return [record[0] for record in cursor_obj.fetchall()]

            print("\nTable - %s & # FETCHED records - %s " % (table_name, str(cursor_obj.rowcount)))

        except cx_Oracle.DatabaseError as e:
            print("Oracle DB Error!", e)

    def update(self, table_name, column, values):

        try:
            with cx_Oracle.connect(self.user,
                                   self.password,
                                   self.hostname + '/' + self.service_id) as connection_obj:
                with connection_obj.cursor() as cursor_obj:
                    query_builder = "update " + table_name + " set " + column + "= :1 where " + column + "=:2"
                    print(query_builder)
                    cursor_obj.executemany(query_builder, values)

                    connection_obj.commit()

                    print("\nTable - %s & # UPDATED records - %s " % (table_name, str(cursor_obj.rowcount)))

        except cx_Oracle.DatabaseError as e:
            print("Oracle DB Error!", e)

    def delete(self, table_name, column, values):

        try:
            with cx_Oracle.connect(self.user,
                                   self.password,
                                   self.hostname + '/' + self.service_id) as connection_obj:
                with connection_obj.cursor() as cursor_obj:
                    query_builder = "delete from " + table_name + " where " + column + "=:1"
                    print(query_builder)
                    cursor_obj.executemany(query_builder, values)

                    connection_obj.commit()

                    print("\nTable - %s & # DELETED records - %s " % (table_name, str(cursor_obj.rowcount)))

        except cx_Oracle.DatabaseError as e:
            print("Oracle DB Error!", e)


def main(query, tables, number_records):
    olt = OracleLoadTest('striim', 'oracle', 'localhost', 'xe', 1521)
    table_list = tables.split(",")

    if query == "insert":

        namelist = olt.generate_namelist(number_records)
        id_list = olt.generate_numlist(number_records, 2, 20000)
        records = [(id, name) for name, id in zip(namelist, id_list)]

        sample_id_list = random.sample(id_list, round(len(id_list) * 0.4))

        id_list_from_sample = random.choices(sample_id_list, k=number_records)
        value_list = olt.generate_numlist(number_records, 40000, 100000)
        order_records = [(id, value) for id, value in zip(id_list_from_sample, value_list)]

        if "Customers" in table_list or "CustomersIL" in table_list:
            olt.insert("Customers", "CUSTOMER_ID, CUSTOMER_NAME", records)
            print("Sample Records below:(CUSTOMER_ID, CUSTOMER_NAME)")
            print(records[:2])

        if "Orders" in table_list or "OrdersIL" in table_list:
            olt.insert("Orders", "CUSTOMER_ID, ORDER_VALUE", order_records)
            print("Sample Records below:(CUSTOMER_ID, ORDER_VALUE)")
            print(order_records[:2])

    if query == "update":

        records = olt.select(tables, "CUSTOMER_ID")

        if len(records) >= number_records:
            old_vaules = random.sample(records, number_records)
            new_values = olt.generate_numlist(number_records, 10, 100000)
            update_values = [(old, new) for old, new in zip(old_vaules, new_values)]
            olt.update(tables, "CUSTOMER_ID", update_values)

    if query == "delete":

        records = olt.select(tables, "CUSTOMER_ID")

        if len(records) >= number_records:
            old_vaules = random.sample(records, number_records)
            update_values = [ (old) for old in old_vaules]
            olt.update(tables, "CUSTOMER_ID", update_values)
        else:
            print("Error: # Records in the table is " + str(len(records)))


if __name__ == "__main__":

    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    else:
        print("Wrong number of inputs, exiting now!")
