import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key
import json
from decimal import Decimal

# Reading first 1600 rows
df = pd.read_csv('Bitcoin_tweets.txt', sep="\t", nrows=1000)

print("------------------------------------------------")
print("Columns : ", df.columns)
print("Total columns in the dataset : ", len(df.columns))
print("Shape of the dataset : ", df.shape)
print("------------------------------------------------")

# Connecting to the dynamo db
dynamodb = boto3.resource("dynamodb", endpoint_url="http://localhost:8000")

print("-------------------------------------------------------")
print("Tables in the dynamo db: ", list(dynamodb.tables.all()))
print("-------------------------------------------------------")

response = dynamodb.create_table(

    # Table name
    TableName='ass02',
    KeySchema=[

        # Partition key
        {
            'AttributeName': 'user_name',
            'KeyType': 'HASH'
        },

        # Sort key
        {
            'AttributeName': 'date',
            'KeyType': 'RANGE'
        },
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'user_name',
            'AttributeType': 'S'
        },

        {
            'AttributeName': 'user_location',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'is_retweeted',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'user_followers',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'date',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'hashtags',
            'AttributeType': 'S'
        },
    ],
    GlobalSecondaryIndexes=[
        {
            'IndexName': 'user_location_index',
            'KeySchema': [
                {
                    'AttributeName': 'user_location',
                    'KeyType': 'HASH'
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL',
            },

            'ProvisionedThroughput': {
                'ReadCapacityUnits': 2000,
                'WriteCapacityUnits': 2000
            },
        },

        # Range cannot be specified without hash so had to use the hash key along with it on another column
        {
            'IndexName': 'is_retweeted_followers_index',
            'KeySchema': [
                {
                    'AttributeName': 'is_retweeted',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'user_followers',
                    'KeyType': 'RANGE'
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL',

            },

            'ProvisionedThroughput': {
                'ReadCapacityUnits': 2000,
                'WriteCapacityUnits': 2000
            },

        },
        {
            'IndexName': 'tags_index',
            'KeySchema': [
                {
                    'AttributeName': 'hashtags',
                    'KeyType': 'HASH'
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL',

            },

            'ProvisionedThroughput': {
                'ReadCapacityUnits': 2000,
                'WriteCapacityUnits': 2000
            },
        },

    ],

    ProvisionedThroughput={
        'ReadCapacityUnits': 2000,
        'WriteCapacityUnits': 2000
    }
)

print("Checking the tables of the dynamo db after creating a new table : ", list(dynamodb.tables.all()))
print("------------------------------------------------------------------------------------------------")
table = dynamodb.Table('ass02')

insertedRows = 0
for index, row in df.iterrows():
    # print(row)
    insertedRows = insertedRows + 1
    tuple = {
        'user_name': str(row['user_name']),
        "user_location": str(row['user_location']),
        "user_description": str(row['user_description']),
        "user_created": str(row['user_created']),
        "user_followers": int(row['user_followers']),
        "user_friends": str(row['user_friends']),
        "user_favourites": str(row['user_favourites']),
        "user_verified": str(row['user_verified']),
        "date": str(row['date']),
        "text": str(row['text']),
        "hashtags": str(row['hashtags']),
        "source": str(row['source']),
        "is_retweeted": str(row['is_retweet'])
    }

    item = json.loads(json.dumps(tuple), parse_float=Decimal)

    table.put_item(Item=item)

print("Inserted Rows : ", insertedRows)
