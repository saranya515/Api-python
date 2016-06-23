import datetime
import logging

from boto3.dynamodb.conditions import Key, Attr
import boto3
import botocore

dynamodb = boto3.resource('dynamodb')
# Getting high leve api access.
client = boto3.client('dynamodb')
# Geting low level api access.

logging.basicConfig(
    filename='aggregate_log_test.log', filemode='w',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO)


class Clicks_aggregation:
    """
    Aggregate all banner clicks tables (prod1.ad-processor.banner-clicks.
    <appid>) data into clicks aggregation table (dev.dcmn.clicks.aggregation.
    <appid>) with fields targetDay, qualifier and clickcount.
    """
    def clicks(self):
        logging.info('------STARTING APPLICATION------')
        logging.info('List of tables are:')
        aggre_prod_name = ("prod1.ad-processor."
                           "banner-clicks.")
        skip_table = 'prod1.ad-processor.banner-clicks.apk.android.mobomarket'

        for table_iterator in dynamodb.tables.all():
            # Querying all dynamodb tables.
            if aggre_prod_name in table_iterator.name:
                # Checking for banner clicks tables.
                logging.info("Current table " + table_iterator.name)
                if table_iterator.name == skip_table:
                    # apk.android.mobomarket has an old design, so skipping
                    # that for now.
                    continue
                loop_limitor = 1
                if loop_limitor:
                    # for testing with few number of tables .
                    table = dynamodb.Table(table_iterator.name)
                    # Creating dynamodb object for the current click table.
                    index_name = 'fingerprint-receivedAt-index'
                    # GSI of the click tables.
                    attributes_to_get = [
                        'receivedDate',
                        'campaignId',
                        'cityName',
                        'countryId',
                        'publisherId',
                        'partnerId']
                    # Attributes used for aggregation.
                    limit = 15
                    # Scanning limit, only this much elements will be scanned.
                    response = scan_table(
                        table, index_name, attributes_to_get, limit)
                    # Retriving clicks table data with limit defined.
                    logging.info("Scanned response " + str(response))
                    table_name_prefix = 'dev.dcmn.clicks.aggregation'
                    # General prefix for creating aggregation table.
                    split_table_name = table_iterator.name
                    app_name = split_table_name.split(".")
                    # Getting app id from the clicks table name to create
                    # aggregate table name.
                    app_id = get_appid(app_name)
                    table_name = table_name_prefix + app_id
                    table_check = dynamodb.Table(table_name)
                    # Getting dynamodb table object with aggregate table name.
                    try:
                        # Aggregate table is already there.
                        table_description = client.describe_table(
                            TableName=table_name)
                        logging.info(
                            "Table description " +
                            str(table_description))
                        for item_iterator in response['Items']:
                            # Getting items to update table.
                            target_day, qualifier = retrieve_aggre_items(
                                item_iterator)
                            logging.info(
                                "Target day is" +
                                target_day +
                                "qualifier is " +
                                qualifier)
                            items = query_clicks(
                                table_check, qualifier, target_day)
                            logging.info("Query results ")
                            logging.info(items)
                            if not items:
                                click_count = 1
                                insert_clicks_data(
                                    table_check, qualifier, target_day,
                                    click_count)
                                logging.info("Data inserted")
                            else:
                                # print(items)
                                click_count = items[0]['clickCount']
                                click_count += 1  # Updating clickcount.
                                update_clicks_data(
                                    table_check, qualifier, target_day,
                                    click_count)
                                logging.info(
                                    "Table updated with clickCount " +
                                    str(click_count))
                    except botocore.exceptions.ClientError as e:
                        # Aggregate table is not created yet.
                        logging.info("Table doesn't exist")
                        table = create_table(table_name)
                        # Table created.
                        for item_iterator in response['Items']:
                            # Getting data to insert into the table.
                            target_day, qualifier = retrieve_aggre_items(
                                item_iterator)
                            items = query_clicks(
                                table, qualifier, target_day)
                            logging.info("Query results ")
                            logging.info(items)
                            if items:
                                click_count = items[0]['clickCount']
                                click_count += 1  # Updating clickcount.
                                update_clicks_data(
                                    table, qualifier, target_day,
                                    click_count)
                                logging.info(
                                    "Table updated with clickCount " +
                                    str(click_count))
                            else:
                                click_count = 1
                                insert_clicks_data(
                                    table, qualifier, target_day, click_count)

                    # loop_limitor += 1


def query_clicks(table, qualifier, target_day):
    """
    Querying aggregate table with parameter qualifier and target day.
    Return queried result.
    """
    query_response = table.query(
                     KeyConditionExpression=Key('qualifier').eq(qualifier) &
                     Key('targetDay').eq(target_day)
                     )
    items = query_response['Items']
    return items


def insert_clicks_data(table, qualifier, target_day, click_count):
    """
    Insert data into aggregate table with items: qualifier , target day and
    click count.
    """
    table.put_item(
        Item={
            'targetDay': target_day,
            'qualifier': qualifier,
            'clickCount': click_count
        }
    )


def update_clicks_data(table, qualifier, target_day, click_count):
    """
    Updating aggregate table with changed click count
    """
    table.update_item(
        Key={
            'targetDay': target_day,
            'qualifier': qualifier
        },
        UpdateExpression='SET clickCount = :val1',
        ExpressionAttributeValues={
            ':val1': click_count
        }
    )


def retrieve_aggre_items(item_iterator):
    """
    Retriving useful data from scanned result.
    Return Target day and qualfier.
    """
    target_day = item_iterator['receivedDate']
    camp_id = item_iterator['campaignId']
    country_name = item_iterator['countryId']
    pub_id = item_iterator['publisherId']
    par_id = item_iterator['partnerId']
    qualifier = "par+camp+pub://" + par_id + "/" + camp_id + "/" + pub_id
    return target_day, qualifier


def create_table(table_name):
    """
    Creating aggregate table with target day as partion key and qualifier
    as sort key.
    Return table object.
    """
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'targetDay',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'qualifier',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'targetDay',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'qualifier',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return table


def scan_table(table, index_name, attributes_to_get, limit):
    """
    Scanning clicks table with index name, name of attributes to retrieve
    and limit.
    """
    try:
        response = table.scan(
            IndexName=index_name,
            AttributesToGet=attributes_to_get,
            Limit=limit)
        return response
    except botocore.exceptions.ClientError as e:
        logging.info(e)


def get_appid(app_name):
    """
    Getting appid from clicks table name.
    Return app id.
    """
    app_id = ""
    if len(app_name) == 4:
        app_id = "." + app_name[3]
    else:
        for i in app_name[3:]:
            app_id = app_id + "." + i
    return app_id


if __name__ == '__main__':
    Clicks_aggregation().clicks()
