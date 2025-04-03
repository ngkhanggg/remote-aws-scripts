import boto3
import csv

from datetime import datetime, timedelta

from settings import *


AWS_REGION = 'ap-southeast-1'
OUTPUT_FOLDER = './get_dynamodb_tables/'


def get_list_dynamodb_tables(dynamodb_client):
    table_names = []

    paginator = dynamodb_client.get_paginator('list_tables')
    for page in paginator.paginate():
        table_names.extend(page.get('TableNames', []))

    return table_names


def get_dynamodb_table_schema(dynamodb_client, table_name):
    response = dynamodb_client.describe_table(TableName=table_name)
    table_description = response['Table']

    primary_keys = {key['AttributeName'] for key in table_description.get('KeySchema', [])}

    column_details = {}
    item_count = table_description.get('ItemCount', 0)

    if item_count > 0:
        scan_response = dynamodb_client.scan(TableName=table_name, Limit=item_count)
        for item in scan_response.get('Items', []):
            for column_name, value in item.items():
                data_type = 'string'
                column_details[column_name] = data_type

    for key in primary_keys:
        if key not in column_details:
            column_details[key] = 'UNKNOWN'

    schema_data = [[table_name, column_name, data_type, 'YES' if column_name in primary_keys else 'NO'] for column_name, data_type in column_details.items()]

    return schema_data, item_count


def export_dynamodb_tables_to_csv(dynamodb_client, output_folder):
    list_table_names = get_list_dynamodb_tables(dynamodb_client)

    print(f"There are {len(list_table_names)} tables.")

    empty_tables = []
    nonempty_tables = []

    for table_name in list_table_names:
        schema_data, item_count = get_dynamodb_table_schema(dynamodb_client, table_name)

        print(f"Table name: {table_name} | Count: {item_count}")

        if item_count > 0:
            nonempty_tables.extend(schema_data)
        else:
            empty_tables.extend(schema_data)

    now = datetime.now() + timedelta(hours=7)
    current_date = now.strftime('%Y%m%d')

    # Write non-empty tables to csv
    with open(f"{output_folder}/All_DynamoDB_Tables_{current_date}.csv", mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['TableName', 'ColumnName', 'DataType', 'IsPrimaryKey'])
        writer.writerows(nonempty_tables)


if __name__ == '__main__':
    now = datetime.now() + timedelta(hours=7)

    dynamodb_client = boto3.client('dynamodb', region_name=AWS_REGION)

    start_time = (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S.%f')
    print(f"{start_time} - Start")
    export_dynamodb_tables_to_csv(dynamodb_client, OUTPUT_FOLDER)
    end_time = (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S.%f')
    print(f"{end_time} - End")
