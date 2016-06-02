import web
import boto3
import datetime
import logging

logging.basicConfig(filename='pythonlog.log', filemode='w',format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
logging.info('------STARTING APPLICATION------')

urls = (
    '/clicks', 'get_clicks'
)

app = web.application(urls, globals())

class get_clicks:        
    def GET(self):
	      dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('prod1.ad-processor.banner-clicks-aggregation.date-shift.DainikBhaskar')
        date = datetime.date.today().strftime("%Y-%m-%d")
        logging.info('Querying table with qualifier : testing://c1sdk/p1sdk and date : ' + date)
        result  = table.get_item(
                       Key={

                        'qualifier': 'testing://c1sdk/p1sdk',
                        'targetDay': date
                      }
                  )

        
        count = result['Item']['clickCount']
        val = str(count)
        logging.info('Current click count' + val)
        count = count + 1
        logging.info('Updating table ....')
        table.put_item(
            Item={
              'clickCount': count,
              'qualifier': 'testing://c1sdk/p1sdk',
              'targetDay': date
            }
        )
        logging.info('Table updated..')
        return '{ clickCount : ' + count + '}'



if __name__ == "__main__":
    app.run()
