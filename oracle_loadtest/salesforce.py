import argparse
import json
import random
import pprint
from simple_salesforce import Salesforce


class SalesforceAPI:
    file_path = "/usr/local/google/home/raghavendiran/Documents/project/salesforce_cred.txt"

    def __init__(self):
        self.cred_file_path = True
        self.instance_url = None
        self.username = None
        self.password = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.operation = None
        self.object_name = None
        self.security_token = None
        self.is_bulk = False
        self.is_mockup = False
        self.num_rows = 0
        self.col_names = []

    def read_creds_from_file(self):
        with open(self.file_path, "r") as reader:
            sf_cred_json = json.loads(reader.read())

        self.instance_url = sf_cred_json["instance_url"]
        self.client_id = sf_cred_json["client_id"]
        self.client_secret = sf_cred_json["client_secret"]
        self.access_token = sf_cred_json["access_token"]
        self.username = sf_cred_json["username"]
        self.password = sf_cred_json["password"]
        self.security_token = sf_cred_json["security_token"]

    @staticmethod
    def get_transaction_mockup():
        data = {}
        data['Name'] = 'Transaction Mockup - Demo Test '
        data['Currency__c'] = 'USD'
        data['Account__c'] = random.choice(['0012F00000oY8lBQAS','0012F00000oY8lCQAS'])
        data['Strategy__c'] = random.choice(['a0M2F000002CIGCUA4', 'a0M2F000002CIHZUA4','a0M2F000002CIGPUA4','a0M2F000002CIGLUA4'])
        '''data['ID_Deal_Status__c'] = random.choice(
            ['Cancelled', 'Pipeline', 'Critical Date', 'Completed', 'Pause', 'On Hold', 'Monitoring', 'Active'])
        data['ID_Property_Type__c'] = random.choice(
            ['General Corporate', 'Virtual Office', 'Retail', 'Parking', 'Corp Apt', 'Warehouse and Manufacturing',
             'Land', 'Income Lease', 'Registration', 'Data Center'])
        #data['RecordTypeName__c'] = 'Misc_Transaction'
        data['RecordTypeId'] = '01270000000UJ8OAAW'
        data['Business_Unit_c'] = 'Google - Core'
        '''
        return data

    @staticmethod
    def get_strategy_mockup():
        data = {}
        data['Name'] = 'Strategy Mockup - Demo Test '
        return data

    @staticmethod
    def get_account_mockup():
        data = {}
        data['Name'] = 'Account Mockup - Demo Test '
        data['RecordTypeId'] = random.choice(['01270000000LyTsAAK', '01270000000LyTtAAK'])
        return data

    def set_colnames(self, sf_object):
        self.col_names = [key['name'] for key in sf_object.describe()['fields']]

    def sf_insert(self, sf_insert_object, data):

        '''if len(data) == 1:
            return json.dumps(sf_insert_object.create(data))
        else:
            print(data)'''
        return sf_insert_object.insert(data, batch_size=1000, use_serial=True)

    def sf_update(self, sf_object, data, id=None):
        if id is None:
            print(sf_object.update(data, batch_size=1000, use_serial=True))
        else:
            print(sf_object.update(id, data))

    def sf_select(self, sf_object, columns, object_name):
        query_builder = "Select " + columns + " from " + object_name + " where Name LIKE '%Test%'"
        # query_builder = "Select " + columns + " from " + object_name
        return sf_object.query_all(query_builder)['records']

    def sf_delete(self, sf_object, data):
        return (sf_object.delete(data, batch_size=1000, use_serial=True))


def main(sfa):
    data_bulk = []
    response_json = []

    sf = Salesforce(username=sfa.username, password=sfa.password, domain="test", client_id=sfa.client_id,
                    security_token=sfa.security_token)
    sfa.set_colnames(getattr(sf, sfa.object_name))

    for i in range(sfa.num_rows):

        if sfa.object_name == "Transaction__c":
            data = SalesforceAPI.get_transaction_mockup()
            print(data)
        elif sfa.object_name == "Strategy__c":
            data = SalesforceAPI.get_strategy_mockup()
        elif sfa.object_name == "Account":
            data = SalesforceAPI.get_account_mockup()

        if sfa.num_rows == 1:
            data['Name'] += str(random.randint(100000, 1000000))
        else:
            data['Name'] += str((1000 + i))
        data_bulk.append(data)

    if sfa.operation == "insert":
        print("Printing a record that will be sent in CURL request for reference:")
        print(data_bulk[:1])
        if sfa.is_bulk:
            response_json = sfa.sf_insert(getattr(sf.bulk, sfa.object_name), data_bulk)
        else:
            response_json = sfa.sf_insert(getattr(sf, sfa.object_name), data_bulk)

    if sfa.operation == "update":
        response_json = sfa.sf_select(sf, "Id,Name", sfa.object_name)
        if response_json is None or sfa.num_rows > len(response_json):
            print("Either response is empty or check the input to number of rows to update. Exiting now!")
            return

        if sfa.is_bulk:
            random_indexes = random.sample(list(range(0, len(response_json))), sfa.num_rows)
            bulk_update = [{'Name': response_json[index]['Name'] + ' Modified', 'Id': response_json[index]['Id']} for
                           index in random_indexes]
            print("Printing a record that will be sent in CURL request for reference")
            print(bulk_update[:1])
            response_json = sfa.sf_update(getattr(sf.bulk, sfa.object_name), bulk_update)

        else:
            record_json = response_json[random.randint(0, len(response_json))]
            record_json['Name'] += " Modified"

            to_update = {key: value for key, value in record_json.items() if key in "Names"}
            response_json = sfa.sf_update(getattr(sf, sfa.object_name), to_update, record_json['Id'])

    if sfa.operation == "delete":
        response_json = sfa.sf_select(sf, "Id", sfa.object_name)

        if response_json is None or sfa.num_rows > len(response_json):
            print("Either response is empty or check the input to number of rows to update. Exiting now!")
            return

        random_indexes = random.sample(list(range(0, len(response_json))), sfa.num_rows)
        bulk_delete = [{'Id': response_json[index]['Id']} for index in random_indexes]
        response_json = sfa.sf_delete(getattr(sf.bulk, sfa.object_name), bulk_delete)

    if sfa.operation == "describe":
        print("Into Describe section")
        response_json = getattr(sf, sfa.object_name).describe()
        bulk_delete = [{'Name': field['name'], 'label': field['label']} for field in response_json['fields'] if
                       not field['nillable']]
        pprint.pprint(bulk_delete)

    if response_json is not None:
        print("Number of rows affected: " + str(len(response_json)))
        print("Printing first row of the response for reference: ")
        print(response_json[:2])

sfa = SalesforceAPI()
parser = argparse.ArgumentParser()
parser.add_argument("--cred_from_file", action="store_true", dest="cred_from_file")
parser.add_argument("--instance_url", type=str, dest="instance_url")
parser.add_argument("--username", type=str, dest="username")
parser.add_argument("--password", type=str, dest="password")
parser.add_argument("--client_id", type=str, dest="client_id")
parser.add_argument("--client_secret", type=str, dest="client_secret")
parser.add_argument("--security_token", type=str, dest="security_token")
parser.add_argument("--access_token", type=str, dest="access_token")
parser.add_argument("--operation", type=str, choices=["insert", "update", "delete", "describe"], dest="operation",
                    required=True)
parser.add_argument("--object_name", type=str, dest="object_name", required=True)
parser.add_argument("--is_bulk", action="store_true", dest="is_bulk")
parser.add_argument("--is_mockup", action="store_true", dest="is_mockup")
parser.add_argument("--num_rows", type=int, dest="num_rows", required=True)

args = parser.parse_args(namespace=sfa)

if args.cred_from_file:
    sfa.read_creds_from_file()

main(sfa)
