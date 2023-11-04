import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key
import json
from decimal import Decimal

# Connecting to the dynamo db
dynamodb = boto3.resource("dynamodb", endpoint_url="http://localhost:8000")


# Query-1
def all_tweets_of_user():
    table = dynamodb.Table('ass02')

    # No index needed as query already on the partition key
    response = table.query(KeyConditionExpression=Key('user_name').eq('BitcoinAgile'))

    for i in response['Items']:
        print("User ", i["user_name"], ":  ", i['text'])


# Query -2
def all_tweets_by_the_users_from_the_same_location():
    table = dynamodb.Table('ass02')

    # Using the index created on the location attribute
    response = table.query(
        IndexName='user_location_index',
        KeyConditionExpression=Key('user_location').eq('London, England')
    )
    for i in response['Items']:
        print(i['user_location'], '  ', i['text'])


# QUERY - 3
def top_k_users_with_the_most_followers():
    table = dynamodb.Table("ass02")

    limit = 100

    response1 = table.query(
        IndexName='is_retweeted_followers_index',
        KeyConditionExpression=Key('is_retweeted').eq('False'),
        ScanIndexForward=False,
        Limit=100)

    count = 0
    for i in response1['Items']:
        # print(i["is_retweeted"], " : ", i["user_name"], " has followers : ", i["user_followers"])

        print(count, " : ", i["user_name"], " has followers : ", i["user_followers"])
        count = count + 1


def tweets_by_top_k_users_with_the_most_followers():
    table = dynamodb.Table("ass02")
    limit = 100

    # response = table.scan(
    #     Limit=k,
    #     IndexName='UserFollowersIndex',
    #     ScanIndexForward=False
    # )

    response1 = table.query(
        IndexName='is_retweeted_followers_index',
        KeyConditionExpression=Key('is_retweeted').eq('False'),
        ScanIndexForward=False,
        Limit=100)

    users = []
    for i in response1['Items']:
        if i['user_name'] not in users:
            users.append(i['user_name'])
        if len(users) == limit:
            break
    userNo = 1

    for user in users:
        print("---------------------------------> User number : ", user)

        # Get all the tweets of the particular user now
        response = table.query(KeyConditionExpression=Key('user_name').eq(user))
        tweetNo = 1
        for row in response['Items']:
            print("Tweet Number ::::::::::::: ", tweetNo)
            print(row['text'])
            tweetNo = tweetNo + 1

        userNo = userNo + 1


def top_k_tweets_with_the_most_matching_tags():
    table = dynamodb.Table("ass02")

    # top 10 tweets with the most matching tags
    limit = 10

    response1 = table.query(
        IndexName='tags_index',
        KeyConditionExpression=Key('hashtags').eq("['Bitcoin']"),
        ScanIndexForward=False,
        Limit=limit)

    for row in response1['Items']:
        print("---> : ", row['text'], "with hash tag ", row["hashtags"])


def delete_all_posts_of_user_with_followers_less_than_thresshold():
    table = dynamodb.Table("ass02")

    # # WHERE FOLLOWER COUNT IS LESS THAN 1000
    # # Thresshold is 1000
    # response = table.scan(
    #     FilterExpression='user_followers < :follower_count',
    #     ExpressionAttributeValues={
    #         ':follower_count': 1000
    #     }
    # )
    #
    # # DELETING THE ITEMS BASED ON THE SORT KEY
    # for i in response['Items']:
    #     response = table.delete_item(Key={
    #         'user_name': i['user_name'], 'date': i['date']})

    # Quering the data again after deleting
    response = table.scan(
        FilterExpression='user_followers < :follower_count',
        ExpressionAttributeValues={
            ':follower_count': 1000
        }
    )
    for i in response['Items']:
        print(i['user_followers'])


# Query-1
# all_tweets_of_user()

# Query-2
# all_tweets_by_the_users_from_the_same_location()

# Query-3
# top_k_users_with_the_most_followers()

# Query-4
# tweets_by_top_k_users_with_the_most_followers()

# Query-5
# top_k_tweets_with_the_most_matching_tags()

# Query-6
# delete_all_posts_of_user_with_followers_less_than_thresshold()

print("All queries end ")