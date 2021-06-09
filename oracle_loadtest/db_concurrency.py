import sys
import threading
from oracle_loadtest.insert import OracleLoadTest


def main(num_insert, num_update, num_delete, test_type, table_name):
    olt = OracleLoadTest('striim', 'oracle', 'localhost', 'xe', 1521)

    namelist = olt.generate_namelist(num_insert)
    id_list = olt.generate_numlist(num_insert, 1, 10000)
    records = [(id, name) for name, id in zip(namelist, id_list)]

    if test_type == "concurrent":

        updated_records = list()
        olt.insert(table_name, "CUSTOMER_ID, CUSTOMER_NAME", records)

        for i in range(num_update):
            record_update = ((records[i][0] * 100), records[i][0])
            updated_records.append(record_update)

            print(record_update)
            x = threading.Thread(target=olt.update, args=(table_name, "CUSTOMER_ID", record_update, False))
            x.start()

        if num_update >= num_delete:
            for i in range(num_delete):
                record_delete = (updated_records[i][0],)

                print(record_update)
                x = threading.Thread(target=olt.delete, args=(table_name, "CUSTOMER_ID", record_delete, False))
                x.start()

    elif test_type == "sequential":
        olt.insert(table_name, "CUSTOMER_ID, CUSTOMER_NAME", records)
        temp = records[0][0]

        for i in range(num_update):
            records_update = (i, temp)
            olt.update(table_name, "CUSTOMER_ID", records_update, False)
            temp = i

        olt.delete(table_name, "CUSTOMER_ID", [(temp,)])

if __name__ == "__main__":

    if len(sys.argv) == 6:
        main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5] )
    else:
        print("Wrong number of inputs, exiting now!")
