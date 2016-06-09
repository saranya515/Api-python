import datetime
import logging

import web
import boto3

logging.basicConfig(
     filename='pythonlog.log', filemode='w',
     format='%(asctime)s %(levelname)s: %(message)s',
     level=logging.INFO)

urls = (
    '/clicks', 'get_clicks')
app = web.application(urls, globals())


class get_clicks:
    def GET(self):
        """
        Return updated bannerclick aggregation table data for today.
        """
        logging.info('------STARTING APPLICATION------')
        dynamodb = boto3.resource('dynamodb')
        table_name = "prod1.ad-processor.banner-clicks-aggregation.date-shift.DainikBhaskar"
        table = dynamodb.Table(table_name)
        date = datetime.date.today().strftime("%Y-%m-%d")
        logging.info('Querying table with qualifier: testing://c1sdk/p1sdk and date: ' + date)
        result = table.get_item(
                       Key={
                        'qualifier': 'testing://c1sdk/p1sdk',
                        'targetDay': date})  # Querying dynamodb table.
        count = result['Item']['clickCount']  # Retrieving current click count.
        val = str(count)
        logging.info('Current click count' + val)
        count += 1
        logging.info('Updating table...')
        table.put_item(
            Item={
              'clickCount': count,
              'qualifier': 'testing://c1sdk/p1sdk',
              'targetDay': date})  # Updating dynamodb table.
        logging.info('Table updated...')
        return '{clickCount: ' + count + '}'


if __name__ == "__main__":
    app.run()
