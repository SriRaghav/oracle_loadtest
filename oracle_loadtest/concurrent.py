import sys
import threading
from oracle_loadtest.insert import OracleLoadTest


def main(num_insert, num_update, num_delete, test_type, table_name):
    olt = OracleLoadTest('striim', 'oracle', 'localhost', 'xe', 1521)

    namelist = olt.generate_namelist(num_insert)

    id_list = olt.generate_numlist(num_insert, 1, 100)
    records = [(id, name) for name, id in zip(namelist, id_list)]

    if test_type == "sequential":
        olt.insert(table_name, "CUSTOMER_ID, CUSTOMER_NAME", records)

        for i in range(num_update):
            records_update = (i, records[0][0])
            print(records_update)
            x = threading.Thread(target=olt.update, args=(table_name, "CUSTOMER_ID", records_update, False))
            x.start()


if __name__ == "__main__":

    if len(sys.argv) == 6:
        main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5] )
    else:
        print("Wrong number of inputs, exiting now!")
