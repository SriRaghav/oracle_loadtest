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

        id_list = [], i = 0

        while i < number_records:

            rand_num = random.randint(limit_min, limit_max)
            if rand_num not in id_list:
                id_list.add(rand_num)
                i += 1

        return id_list

    def insert(self, table_name, values):

        try:
            connection_object = cx_Oracle.connect(self.user, self.password, self.hostname + '/' + self.service_id)

            # Now execute the sqlquery
            cursor = connection_object.cursor()
            cursor.execute("insert into customers values(60)")

            # commit that insert the provided data
            connection_object.commit()

            print("value inserted successful")

        except cx_Oracle.DatabaseError as e:
            print("There is a problem with Oracle", e)

            # by writing finally if any error occurs
            # then also we can close the all database operation
        finally:
            if cursor:
                cursor.close()
            if connection_object:
                connection_object.close()


def main(query, number_records):
    id_min = 1000
    id_max = 10000
    olt = OracleLoadTest('striim', 'oracle', 'localhost', 'xe', 1521)

    if query == "insert":
        namelist = olt.generate_namelist(number_records)
        id_list = olt.generate_idlist(number_records, id_min, id_max)
        records = [(name, id) for name, id in zip(namelist, id_list)]
        print(records)


if __name__ == "__main__":

    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        print("Exiting now!")
