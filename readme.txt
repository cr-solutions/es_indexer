The class "es_indexer" can help you to add documents or
database records from MySQL (MariaDB) via Python Version >= 3.6  to ES Index (Elasticsearch)

All database field types are converted to comparable field types in Elasticsearch. 
JSON strings in the database are directly supported as Elasticsearch JSON (see also sample.elk.json.string.mapping.json).

You can use the indexer with public endpoints (RDS, ES, S3),
private endpoints (via VPC) + NAT gateway required for boto3 with S3 or
private endpoints with config on local filesystem instead of S3
depending on your system environment


Install on local env (EC2) require:
$ pip3 install --upgrade pymysql, boto3, requests

Install for AWS Lambda require AWS Lambda Deployment Package in Python (boto3 is default installed on Lambda):
for more see also https://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html
$ cd /home/ubuntu/python/myproject
$ pip3 install --upgrade requests pymysql --system -t .
$ zip -r ./test_es_indexer.zip .

after the setup of the code it can e.g. periodically indexed via cron jobs or cloud watch rules

Default filesystem structure:
folder with source file of entry point, as sample "test_es_indexer.py"
                       |
                        test_es_indexer.py
                        libs
                            |
                             es_indexer.py
                        config
                            |
                             sample.es_indexer.config.json



Sample Code "test_es_indexer.py":

#################################
# only for local test
def lambda_handler(event, context):
#################################

   ###
   # cut from here for Lambda
   import time, datetime, traceback, os
   from libs.es_indexer_lib import es_indexer

   print('start ' + time.strftime("%Y-%m-%d %H:%M"))
   start_time = time.time()

   try:
      es_indexer.enable_debug() # activate debug output print

      es_indexer('s3://my-bucket', 'config', 'test', 5) # without filename, the class search for a config file like test.json

      es_indexer('file://', 'config', 'index1', 10, 'index1.json')

      print(es_indexer.measure())  # to get output on screen or CloudWatch Logs

      ####################################################################

      # sample use full index if not lambda (started as sample from EC2)
      if os.environ.get('AWS_REGION') is None:

         es_indexer.disable_debug()
         offset = 0
         limit = 1000
         time = 0
         rows_left_to_upd = True
         while rows_left_to_upd:
            es_indexer('file://', 'config', 'index1', limit, 'index1.json', offset)

            measure = es_indexer.measure()
            time += measure['timings']['total']

            offset += limit

            if offset % 3000 == 0:
               print("Offset: "+str(offset)+"\r\n")
               print(time)
               time = 0

            if measure['indexed'] == 0:  # stop if no more records to index
               rows_left_to_upd = False

   except UserWarning as err: # exceptions raised from the class
      print('Error', err)
      traceback.print_tb(err.__traceback__)

   except BaseException as err:
      print('Unknown Error', err)
      traceback.print_tb(err.__traceback__)

   print('end ' + time.strftime("%Y-%m-%d %H:%M"))
   print("--- %s seconds ---" % (time.time() - start_time))

   return None

   # cut till here for Lambda
   ###

#################################
# only for local test
lambda_handler(None, None)
#################################



Copyright (c) 2019, PantherMedia (https://www.panthermedia.net), CR-Solutions (https://www.cr-solutions.net), Ricardo Cescon