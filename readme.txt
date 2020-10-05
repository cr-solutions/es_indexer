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

Sample Bash Deployment Package
$ cd /home/ubuntu/python/myproject
$ pip3 install --upgrade requests pymysql --system -t .
$ zip -r9 ./test_es_indexer.zip .


Sample for a deploy.sh 
# !/bin/sh
python3 -m pip install --upgrade requests pymysql --system --target package
cd package
zip -r9 ../test_es_indexer.zip .
cd ..
zip -g test_es_indexer.zip libs/es_indexer_lib.py
zip -g test_es_indexer.zip config/media.json
zip -g test_es_indexer.zip test_es_indexer.py
aws lambda update-function-code --function-name test_es_indexer --zip-file fileb://test_es_indexer.zip



after the setup of the code it can e.g. periodically indexed via local cron jobs or AWS Cloud Watch rules in combination with AWS Lambda

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


import time, datetime, traceback, os
from libs.es_indexer_lib import es_indexer
#################################
def lambda_handler(event, context):
#################################

   print('start ' + time.strftime("%Y-%m-%d %H:%M"))
   start_time = time.time()

   try:
      # sample use to full indexing as sample from an EC2 instance
      if os.environ.get('AWS_REGION') is None:

         es_indexer.enable_debug() # activate debug output print
         offset = 0
         limit = 1000
         duration = 0
         rows_left_to_upd = True
         while rows_left_to_upd:
            es_indexer('file://', 'config', 'index1', limit, 'index1.json', offset)

            measure = es_indexer.measure()
            duration += measure['timings']['total']

            offset += limit

            if offset % 200 == 0:
               print("Offset: "+str(offset)+"\r\n")
               print(duration)
               duration = 0
               time.sleep(0.90)

            if measure['indexed'] == 0:  # stop if no more records to index
               rows_left_to_upd = False

            # sample use to batch indexing as sample via cron job
         else:
            es_indexer.disable_debug()

            # sample 1
            es_indexer('s3://my-bucket', 'config', 'test', 5) # without filename, the class search for a config file like test.json

            # sample 2
            es_indexer('file://', 'config', 'index1', 10, 'index1.json')

            print(es_indexer.measure())  # to get output on screen or CloudWatch Logs

   except UserWarning as err: # exceptions raised from the class
      print('Error', err)
      traceback.print_tb(err.__traceback__)

   except BaseException as err:
      print('Unknown Error', err)
      traceback.print_tb(err.__traceback__)

   print('end ' + time.strftime("%Y-%m-%d %H:%M"))
   print("--- %s seconds ---" % (time.time() - start_time))

   return None


#################################
# to test on local OS
if os.environ.get('AWS_REGION') is None:
   lambda_handler(None, None)
#################################



# The Initial Developers of the Original Code are:
# Copyright (c) 2019-2020, CR-Solutions (https://www.cr-solutions.net), Ricardo Cescon
# Contributor(s): Steffen Blaszkowski, PantherMedia (https://www.panthermedia.net)
# All Rights Reserved.