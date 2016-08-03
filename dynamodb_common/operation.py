import logging

from boto3.dynamodb.conditions import Key, Attr
import boto3
import botocore

dynamodb = boto3.resource('dynamodb')
# Getting high leve api access.
client = boto3.client('dynamodb')
# Geting low level api access.


class CommonOperations:
    def scan_table(self, table, attr, attr_value):
        """
        Scanning clicks table with index name, name of attributes to retrieve
        and limit.
        """
        try:
            response = table.scan(
                FilterExpression=Attr(attr).eq(attr_value))
            return response
        except botocore.exceptions.ClientError as e:
            logging.info(e)

    def scan_table_2(self, table, attr1, attr1_value, attr2, attr2_value, installed_time):
        """
        Scanning clicks table with index name, name of attributes to retrieve
        and limit.
        """
        try:
            response = table.scan(
                FilterExpression=Attr(attr1).eq(attr1_value) &
                                 (Attr(attr2).gt(attr2_value) &
                                  Attr(attr2).lt(installed_time)))
            return response
        except botocore.exceptions.ClientError as e:
            logging.info(e)


    def query_with_single_attr(self, table, attr, attr_value):
        """
        Querying aggregate table with parameter qualifier and target day.
        Return queried result.
        """
        query_response = table.query(
            KeyConditionExpression=Key(attr).eq(attr_value))
        items = query_response['Items']
        return items