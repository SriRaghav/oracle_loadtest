import cx_Oracle
import names
import random
import sys


#  con = cx_Oracle.connect('striim', 'oracle', 'localhost/xe')

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
        for i in range(1, number_records):
            name_list.append(names.get_full_name(gender=random.choice(gender)))

        return name_list

    @staticmethod
    def generate_idlist(number_records, limit_min, limit_max):

        id_list = []
        i = 0

        while i < number_records:

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

            print("Table - %s and number of records affected - %s " % (table_name, str(cursor_obj.rowcount)))

        except cx_Oracle.DatabaseError as e:
            print("Oracle DB Error!", e)

        finally:
            if cursor_obj:
                cursor_obj.close()
            if connection_obj:
                connection_obj.close()


def main(query, number_records):
    id_min = 1000
    id_max = 10000
    olt = OracleLoadTest('striim', 'oracle', 'localhost', 'xe', 1521)

    if query == "insert":
        namelist = olt.generate_namelist(number_records)
        id_list = olt.generate_idlist(number_records, id_min, id_max)
        records = [(id, name) for name, id in zip(namelist, id_list)]
        olt.insert("Customers", "CUSTOMER_ID, CUSTOMER_NAME", records)


if __name__ == "__main__":

    if len(sys.argv) == 3:
        main(sys.argv[1], int(sys.argv[2]))
    else:
        print("Wrong number of inputs, exiting now!")
