"""
Create CSV from database items
"""
import time
from datetime import datetime
import csv
import json
import boto3
from boto3.dynamodb.conditions import Key
from config import *

output_file = str(
    input("Enter name of the output file with .csv extension >\n")
).strip()


def get_data():
    """
    Driver function
    """
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=Region.US_EAST_1,
        aws_access_key_id=Creds.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Creds.AWS_SECERT_ACCESS_KEY,
    )

    table = dynamodb.Table(Dynamodb.TABLE)

    items = []
    startTime = datetime.now()
    response = table.query(
        IndexName=Dynamodb.TABLE_INDEX,
        KeyConditionExpression=Key("typeId").eq(Dynamodb.TYPE_ID),
    )

    items = response["Items"]

    while "LastEvaluatedKey" in response:
        response = table.query(
            IndexName=Dynamodb.TABLE_INDEX,
            KeyConditionExpression=Key("typeId").eq(Dynamodb.TYPE_ID),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )

        items.extend(response["Items"])

    print("DYNAMODB OPERATION TIME > ", startTime - datetime.now())
    print("Total items: {}".format(len(items)))

    keys = ["Created", "a.location", "a.gender", "status", "emotions"]

    for item in items:
        total = 0
        for key in list(item):
            if key not in keys:
                item.pop(key, None)

    with open(output_file, "w", newline="\n", encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(items)


if __name__ == "__main__":
    get_data()

