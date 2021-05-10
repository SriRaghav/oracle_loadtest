import sys
import threading
from OracleLoadTest


def main(num_insert, num_update, num_delete, test_type, table_name):
    olt = OracleLoadTest('striim', 'oracle', 'localhost', 'xe', 1521)

    namelist = olt.generate_namelist(num_insert)

    id_list = olt.generate_numlist(num_insert, 1, 10)
    id_list_to_update = olt.generate_numlist(num_insert, 100, 110)

    records = [(id, name) for name, id in zip(namelist, id_list)]
    records_update = [(old, new) for old, new in zip(id_list, id_list_to_update)]

    if test_type == "sequential":
        olt.insert(table_name, "CUSTOMER_ID, ORDER_VALUE", records)

        for i in range(num_update):
            x = threading.Thread(target=olt.update, args=(table_name, "CUSTOMER_ID", records_update))
            x.start()


if __name__ == "__main__":

    if len(sys.argv) == 4:
        main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5] )
    else:
        print("Wrong number of inputs, exiting now!")
