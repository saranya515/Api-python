import logging
import datetime

from user_agents import parse
import boto3
import uuid

from dynamodb_common import CommonOperations, DynamoDbResources


dynamodb = boto3.resource('dynamodb')
# Getting high leve api access.
client = boto3.client('dynamodb')
# Geting low level api access.
common_op = CommonOperations()
# Getting common operations on tables.


logging.basicConfig(
    filename='clics_to_install_matching.log', filemode='w',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO)


class ClickInstallMatching:
    """
    Find a install match in the clicks table (prod1.ad-processor.banner-clicks.
    <appid>) for hardcoded install data.
    """

    def match(self):
        print("Matching start here.")
        app_id = 'com.ixigo'
        dynamodb_tables = DynamoDbResources()
        reg_prefix = dynamodb_tables.resource('table_names', 'reg')
        print (reg_prefix)
        reg_name = reg_prefix + app_id
        table = dynamodb.Table(reg_name)
        device_id = 'dcmn-test-id'
        # device_id = 'android-id:314527ec-c38a-39ba-8852-5d34c5fa8944'
        # Test device id.
        # date = datetime.date.today().strftime("%Y-%m-%d")
        # installed_date = '2015-07-19'
        installed_date_time = '2015-07-20T16:31:19.556Z'
        installation_ip_address = '182.74.233.194'
        registration = common_op.scan_table(table, 'deviceId', device_id)
        # Getting entry in registration table for device id.
        exact_param = dynamodb_tables.resource('conf', 'exact_window')
        if not registration['Items']:
            print ("Not registered, carry on")
            # Not registered yet
            clicks_prefix = dynamodb_tables.resource('table_names', 'clicks')
            clicks_name = clicks_prefix + app_id
            table_clicks = dynamodb.Table(clicks_name)
            fingerprint = get_fingerprint(installation_ip_address)
            app_conf_name = dynamodb_tables.resource('table_names', 'app-conf')
            table_conf = dynamodb.Table(app_conf_name)
            conf = common_op.scan_table(table_conf, 'packageName', app_id)
            # print (conf)
            exact_win_val = get_window_values(conf, exact_param)

            # date_time = datetime.date('2015-07-19T16:31:19.556Z')
            date_time = get_datetime(installed_date_time, exact_win_val)
            matching_items = common_op.scan_table_2(
                table_clicks,
                'fingerprint',
                fingerprint,
                'receivedAt',
                date_time,
                installed_date_time)
            print (matching_items)
            if matching_items['Items']:
                print ("Matches with exact window")
            else:
                print ("No exact match, have to find fyzzy match")
                fuzzy_param = dynamodb_tables.resource('conf', 'fuzzy_window')
                fuzzy_win_val = get_window_values(conf, fuzzy_param)
                print (fuzzy_win_val)
                fuzzy_date = get_datetime(installed_date_time, fuzzy_win_val)
                matching_items = common_op.scan_table_2(
                    table_clicks,
                    'receivedFrom',
                    installation_ip_address,
                    'receivedAt',
                    fuzzy_date,
                    installed_date_time)

            # exact_window_match = get_exact_window()
        else:
            print ("Device already registered on " +
                   registration['Items'][0]['registeredAtDay'])
            # print (registration)


def get_fingerprint(ip):
    """
    Generating fingerprint using ip and user agent.
    """
    ua = (
        "User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_4 like Mac OS X) "
        "AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B554a "
        "Safari/9537.53 ")
    user_agent = parse(ua)
    os_family = user_agent.os.family
    os_version = user_agent.os.version
    os_version_major = os_version[0]
    os_version_minor = os_version[1]
    os_version_bugfix = os_version[2]
    ua_list = list()
    ua_list.append(os_family)
    ua_list.append(os_version_major)
    ua_list.append(os_version_minor)
    ua_list.append(os_version_bugfix)
    ua_list.append(ip)
    key = uuid.UUID('{e8ff6de1-3f31-524f-8f83-bb751fa3cc46}')
    # setting a key for uuid generation
    for i in ua_list:
        fingerprint = uuid.uuid5(key, str(i))
        key = fingerprint
    # fingerprint = '8855735d-0824-5a44-b9f6-44211b14cd27'
    return str(fingerprint)


def get_datetime(installed_date_time, window_val):
    """
    Getting date range, by performing subtract operation on date.
    """
    # print (installed_date_time)
    date_time = datetime.datetime.strptime(
        installed_date_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    date_time1 = date_time - datetime.timedelta(minutes=int(window_val))
    # print (date_time1)
    date_time = date_time1.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # print (date_time)
    return date_time


def get_window_values(conf, param):
    """
    Fetching values for exact and fuzzy windows.
    """
    for item in conf['Items']:
        if (item['parameterName'] == param):
            # print (item['parameterValue'])
            param_val = item['parameterValue']
    return param_val


if __name__ == '__main__':
    ClickInstallMatching().match()
