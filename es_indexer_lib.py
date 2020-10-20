# The contents of this file are subject to the Mozilla Public License
# Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
# https://www.mozilla.org/en-US/MPL/2.0/

# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either expressed or implied. See the License for
# the specific language governing rights and limitations under the License.

# The Initial Developers of the Original Code are:
# Copyright (c) 2019-2020, CR-Solutions (https://www.cr-solutions.net), Ricardo Cescon
# Contributor(s): Steffen Blaszkowski, PantherMedia (https://www.panthermedia.net)
# All Rights Reserved.


import pymysql
from pymysql._compat import text_type

import boto3, json, traceback, urllib3, requests, inspect, os, sys, re, datetime, time, collections, warnings
from requests.auth import HTTPBasicAuth


###########################################################
###########################################################
###########################################################



class es_indexer:
   s3bucket = ''
   s3prefix = ''

   filetype = ''
   folder = ''

   indexname = ''

   config_file = ''
   config = ''

   db = None

   bulklimit = 0

   upd_keys = []

   debug = False

   measure = {}

   offset = None

   last_modified_timestamp_upd = True

   ###########################################################

   def __init__(self, s3bucket_filetype: str, s3prefix_folder: str, indexname: str, bulklimit: int,
                configfile: str = '', offset: int = None, last_modified_timestamp_upd: bool = True):
      """
      Parameters
      ----------
      s3bucket_filetype : str
         s3://my-bucket or file://
      s3prefix_folder : str
         S3 prefix or local folder name where the config file is located
      indexname : str, optional
         name of index in Elasticsearch
      bulklimit : int
         number of records per run or in case of init per loop
      configfile : str, optional
         name of config file
      offset : int, optional
         used for initial indexing
      last_modified_timestamp_upd : bool, optional
         useful if you initial indexing an test env from same source as productive

      Samples
      ----------
      # sample 1
      es_indexer('s3://my-bucket', 'config', 'test', 5) # without filename, the class search for a config file like test.json
      # sample 2
      es_indexer('file://', 'config', 'index1', 10, 'index1.json')
      # sample 3
      es_indexer('file://', 'config', 'index1', limit, 'index1.json', offset, False)
      """

      global ES_INDEXER_DEBUG

      warnings.simplefilter("error", category=pymysql.Warning)

      if os.environ.get('ES_INDEXER_DEBUG') is not None:
         ES_INDEXER_DEBUG = bool(os.environ.get('ES_INDEXER_DEBUG'))

      try:
         if ES_INDEXER_DEBUG:
            self.debug = True
      except NameError:
         pass

      self.measure = {}

      if s3bucket_filetype.find('s3://') != -1:
         self.s3bucket = s3bucket_filetype[5:]
         self.s3prefix = s3prefix_folder
      elif s3bucket_filetype.find('file://') != -1:
         self.filetype = s3bucket_filetype[7:]
         self.folder = s3prefix_folder
      else:
         raise UserWarning('Error, parameter "s3bucket_filetype" must contain s3:// or file://')

      self.indexname = indexname
      self.bulklimit = bulklimit
      self.config_file = configfile

      if self.bulklimit > 5000:  # max ES bulk limit
         self.bulklimit = 5000
      if self.bulklimit < 1:
         self.bulklimit = 1

      self.offset = offset
      self.last_modified_timestamp_upd = last_modified_timestamp_upd

      # print('debug', __class__, inspect.currentframe().f_back.f_lineno)
      # return None

      tick = time.time()

      if s3bucket_filetype.find('s3://') != -1:
         self._s3getConfig()
      else:
         self._fs_getConfig()

      elapsed_time = time.time() - tick
      self.measure['timings'] = {'config': elapsed_time}

      self.upd_keys = []

      if self.debug:
         print("\r\nDebug " + inspect.currentframe().f_code.co_name + ";\r\n", "Config File: " + self.config_file,
               "\r\n", "Payload: " + json.dumps(self.config, sort_keys=True, indent=3), "\r\n\r\n", "#" * 50, "\r\n")

      self._do()

      timeings = self.measure['timings']
      total = 0
      for key in timeings:
         total += timeings[key]

      self.measure['timings'].update({'total': total})

      global ES_INDEXER_MEASURE
      ES_INDEXER_MEASURE = self.measure

   ###########################################################

   def _fs_getConfig(self):
      buf = ''
      config_file = ''

      if self.config_file == '':
         config_file = self.indexname + '.json'
      else:
         config_file = self.config_file

      try:
         if not self.folder:
            config_file = config_file
         else:
            config_file = self.folder + '/' + config_file

         config_file = os.path.realpath(os.path.dirname(os.path.abspath(__file__)) + '/../' + config_file)

         self.config_file = config_file

         hFile = open(config_file, 'r')
         buf = hFile.read()
         hFile.close()

      except BaseException as err:
         raise UserWarning('Error read config from local fs, File: "' + config_file + '" - ' + str(err))

      try:
         self.config = json.loads(buf)
      except BaseException as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error - ' + str(err))

   ###########################################################

   def _s3getConfig(self):
      str = ''
      config_file = ''

      if self.config_file == '':
         config_file = self.indexname + '.json'
      else:
         config_file = self.config_file

      try:
         if not self.s3prefix:
            config_file = config_file
         else:
            config_file = self.s3prefix + '/' + config_file

         self.config_file = config_file

         s3 = boto3.resource('s3')
         obj = s3.Object(self.s3bucket, config_file)
         str = obj.get()['Body'].read().decode('utf-8')
      except BaseException as err:
         raise UserWarning(
            'Error read config from s3 bucket "' + self.s3bucket + '", File: "' + config_file + '" - ' + str(err))

      try:
         self.config = json.loads(str)
      except BaseException as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error - ' + str(err))

   ###########################################################

   def _rdsConnect(self):
      if self.db != None:
         return self.db

      endpoint = ''
      user = ''
      pw = ''
      try:
         endpoint = self.config['rds']['endpoint']
         user = self.config['rds']['user']
         pw = self.config['rds']['password']
      except KeyError as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error, missing key: ' + str(err))

      timeout = None
      try:
         timeout = int(self.config['rds']['timeout'])
      except KeyError as err:
         timeout = 3
         pass

      host = endpoint
      port = 3306
      if endpoint.find(':') != -1:
         endpoint = endpoint.split(':')
         port = int(endpoint[1])
         endpoint = endpoint[0]

      try:
         self.db = pymysql.connect(host=endpoint, port=port, user=user, passwd=pw, charset='utf8',
                                   connect_timeout=timeout)
      except pymysql.err.OperationalError as err:
         raise UserWarning('Error , current timeout ' + str(
            timeout) + ', you can increase it via key timeout in the *.json file - ' + str(err))

      return self.db

   ###########################################################

   def _sqlSelect(self):
      last_mod_field = ''
      data = ''
      group_by = ''
      sort = ''
      additional_where = ''
      additional_primary_key_for_full_indexing = ''

      try:
         last_mod_field = self.config['sql']['last-modified-timestamp-field']
         data = self.config['sql']['data']
         if 'group-by' in self.config['sql']:
            group_by = self.config['sql']['group-by']
         if 'sort' in self.config['sql']:
            sort = self.config['sql']['sort']
         if 'additional-where' in self.config['sql']:
            additional_where = self.config['sql']['additional-where']
         if 'additional-primary-key-for-full-indexing' in self.config['sql']:
            additional_primary_key_for_full_indexing = self.config['sql']['additional-primary-key-for-full-indexing']
      except KeyError as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error, missing key: ' + str(err))



      fields = ''
      tfrom = ''
      i = 0
      joins = []
      try:
         for item in data:
            schema_table = item["schema"] + '.' + item["table"] + '.'
            if i == 0:
               tfrom = item["schema"] + '.' + item["table"]
            else:
               joins.append({"join": item["join"], "schema": item["schema"], "table": item["table"]})

            for fname in item["fields"]:
               if fname.find('.') > 0:  # to support functions
                  schema_table = ''

               fields += schema_table + fname + ', '

            i += 1

         query = 'SELECT ' + fields[0:-2] + ' FROM ' + tfrom

         for item in joins:
            query += ' LEFT JOIN ' + item["schema"] + '.' + item["table"]
            if item["join"].upper().find(' ON ') == -1:
               query += ' ON ' + item["join"]
            else:
               query += ' ' + item["join"]

         if self.offset is None:
            query += ' WHERE ' + last_mod_field + ' != "1970-01-01 00:00:00"'
         else:
            if len(additional_primary_key_for_full_indexing) > 0:
               query += ' WHERE ' + additional_primary_key_for_full_indexing + ' >= ' + str(
                  self.offset) + ' AND ' + additional_primary_key_for_full_indexing + ' <= ' + str(
                  self.offset + self.bulklimit)

         if len(additional_where) > 0:
            if self.offset is None or query.find(' WHERE ') != -1:
               query += ' AND ' + additional_where
            else:
               query += ' WHERE ' + additional_where

         if len(group_by) > 0:
            query += ' GROUP BY ' + group_by

         offset = ''
         if self.offset is not None and len(
                 additional_primary_key_for_full_indexing) == 0:  # use OFFSET/LIMIT only if not possible via PK
            offset = str(self.offset) + ', '

         if len(sort) > 0:
            query += ' ORDER BY ' + last_mod_field + ' ' + sort

         query += ' LIMIT ' + offset + str(self.bulklimit)


      except KeyError as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error, missing key: ' + str(err))

      return query

   ###########################################################

   def _execSelect(self):
      db = self._rdsConnect()

      if 'query-pre' in self.config['sql']:
         try:
            query_pre = self.config['sql']['query-pre']

            if len(query_pre) > 0:
               cursor = db.cursor()
               cursor.execute(query_pre)
               db.commit()

               if self.debug:
                  print("\r\nDebug " + inspect.currentframe().f_code.co_name + ";\r\n", "Query-Pre: " + query_pre,
                        "\r\n\r\n", "#" * 50, "\r\n")

         except pymysql.Warning as err:
            raise UserWarning('SQL warning', err, query_pre)
         except pymysql.err.ProgrammingError as err:
            raise UserWarning('SQL error', err, query_pre)


      cursor = db.cursor(pymysql.cursors.DictCursor)
      cursor._defer_warnings = True
      query = self._sqlSelect()

      if self.debug:
         print("\r\nDebug " + inspect.currentframe().f_code.co_name + ";\r\n", "Query: " + query, "\r\n\r\n", "#" * 50,
               "\r\n")

      tick = time.time()

      try:
         cursor.execute(query)

      except pymysql.Warning as err:
         raise UserWarning('SQL warning', err, query)
      except pymysql.err.ProgrammingError as err:
         raise UserWarning('SQL error', err, query)

      rows = cursor.fetchall();

      elapsed_time = time.time() - tick
      self.measure['timings'].update({'sql_select': elapsed_time})

      self.measure['indexed'] = len(rows)

      return rows

   ###########################################################

   @property
   def _mapping(self):

      json_str = ''
      mapping = None
      last_mod_field_upd_key = None

      rows = self._execSelect()

      try:
         mapping = self.config["mapping"]
         last_mod_field_upd_key = self.config['sql']['last-modified-timestamp-upd-key']
      except KeyError as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error, missing key: ' + str(err))

      if not '_id' in mapping:
         raise UserWarning('internal ES _id mapping is missing')

      es_id_var_name = mapping['_id']
      del mapping['_id']

      if not '_type' in mapping:
         raise UserWarning('internal ES _type mapping is missing')

      es_type = mapping['_type']
      del mapping['_type']

      if last_mod_field_upd_key.find('=') == -1:
         raise UserWarning('last-modified-timestamp-upd-key, missing variable allocation like: id=$id_doc')

      last_mod_field_upd_key = last_mod_field_upd_key.split('=')
      upd_key_name = last_mod_field_upd_key[0]
      upd_key_var = last_mod_field_upd_key[1]

      tick = time.time()

      if len(rows) > 0:
         fieldnames = rows[0].keys()

         for row in rows:
            mapping = self.config["mapping"]
            es_id = es_id_var_name

            if '_comment' in mapping:
               del mapping['_comment']

            mapping_str = json.dumps(mapping)
            upd_key_str = ''

            for field in fieldnames:
               var = '$' + field
               ftype = type(row[field])
               val = str(row[field])

               # remove non printable chars, linefeeds etc.
               val = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', val).strip()


               # dynamic field mapping for ES, https://www.elastic.co/guide/en/elasticsearch/reference/6.5/dynamic-field-mapping.html
               if ftype == int or ftype == float:
                  mapping_str = mapping_str.replace('"' + var + '"', val)
               elif ftype == datetime.datetime:
                  val = val.replace('-', '/')
                  mapping_str = mapping_str.replace('"' + var + '"', '"' + val + '"')
               elif ftype == bool:
                  val = val.lower()
                  mapping_str = mapping_str.replace('"' + var + '"', val)
               elif row[field] is None:
                  mapping_str = mapping_str.replace('"' + var + '"', 'null')
               else:
                  is_json = True
                  try:
                     json.loads(row[field])
                  except ValueError as e:
                     is_json = False

                  # 'Infinity' and 'NaN' string is a special case for JSON, check also for digit because json.loads == True for numbers
                  if is_json and val != 'Infinity' and val != 'NaN' and not val.replace('.','',1).isdigit():
                     mapping_str = mapping_str.replace('"' + var + '"', row[field])

                  else: # strings
                     # escape characters
                     val = re.sub(pattern=r'([\"\\])', repl=r'\\\1', string=val)

                     mapping_str = mapping_str.replace('"' + var + '"', '"' + val + '"')

               if es_id == var:
                  es_id = str(row[field])

               if upd_key_var == var:
                  upd_key_str = upd_key_name + '=' + str(row[field])

            if es_id.find('$') != -1:
               raise UserWarning('no database field for internal ES _id mapping found')

            action = '{"index":{"_index":"' + self.indexname + '", "_type":"' + es_type + '", "_id":"' + es_id + '"}}' + "\n"

            if (sys.getsizeof(json_str) + sys.getsizeof(action) + sys.getsizeof(
                    mapping_str)) > 1024 * 1024 * 5:  # max. MB size for bulk
               break

            json_str += action
            json_str += mapping_str + "\n"

            self.upd_keys.append(upd_key_str)

      elapsed_time = time.time() - tick
      self.measure['timings'].update({'mapping': elapsed_time})

      json_byte = json_str.encode('utf-8')
      # for debug
      # print(json_byte)
      # return False

      return json_byte

   ###########################################################

   def _es_bulk(self, json_byte):

      if self.debug:
         print("\r\nDebug " + inspect.currentframe().f_code.co_name + ";\r\n", "Payload: " + str(json_byte, 'utf-8'),
               "\r\n\r\n", "#" * 50, "\r\n")

      headers = {"Content-Type": "application/json; charset=utf-8"}

      endpoint = ''
      try:
         endpoint = self.config['es']['endpoint']
      except KeyError as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error, missing key: ' + str(err))

      timeout = None
      try:
         timeout = int(self.config['es']['timeout'])
      except KeyError as err:
         timeout = 3
         pass

      retry = None
      try:
         retry = int(self.config['es']['retry'])

         if retry < 1:
            retry = 1

      except KeyError as err:
         retry = 1
         pass

      retry_wait_sec = None
      try:
         retry_wait_sec = int(self.config['es']['retry_wait_sec'])
      except KeyError as err:
         retry_wait_sec = 1
         pass

      user = None
      try:
         user = self.config['es']['user']
      except KeyError as err:
         user = None
         pass

      pw = None
      try:
         pw = self.config['es']['password']
      except KeyError as err:
         pw = None
         pass

      ###

      replicas = None
      try:
         replicas = self.config['settings']['replicas']
      except KeyError as err:
         replicas = None
         pass

      shards = None
      try:
         shards = self.config['settings']['shards']
      except KeyError as err:
         shards = None
         pass


      urllib3.disable_warnings(
         urllib3.exceptions.InsecureRequestWarning)  # to support local ES endpoints via SSH tunnel, sample: https://127.0.0.1:9200

      tick = time.time()


      res = None
      x = 0
      for x in range(0, retry):
         try:
            ##################
            # create index with settings if not exists
            if replicas is not None and shards is not None:
               if user is not None and pw is not None:
                  res = requests.head(url=endpoint + '/' + self.indexname, verify=False, headers=headers, timeout=timeout, auth=HTTPBasicAuth(user, pw))
               else:
                  res = requests.head(url=endpoint + '/' + self.indexname, verify=False, headers=headers, timeout=timeout)


               if res.status_code == 404:
                  setting_json_str = '{   "settings": {   "index": {   "number_of_shards" : '+str(shards)+', "number_of_replicas" : '+str(replicas)+'   }   }   }'
                  setting_json_byte = setting_json_str.encode('utf-8')

                  if user is not None and pw is not None:
                     res = requests.put(url=endpoint + '/' + self.indexname, verify=False, data=setting_json_byte, headers=headers, timeout=timeout, auth=HTTPBasicAuth(user, pw))
                  else:
                     res = requests.put(url=endpoint + '/' + self.indexname, verify=False, data=setting_json_byte, headers=headers, timeout=timeout)

                  if res.status_code != 200:
                     raise UserWarning('Error create index: ' + str(res.content))


            ##################


            if user is not None and pw is not None:
               res = requests.put(url=endpoint + '/_bulk', verify=False, data=json_byte, headers=headers, timeout=timeout, auth=HTTPBasicAuth(user, pw))
            else:
               res = requests.put(url=endpoint + '/_bulk', verify=False, data=json_byte, headers=headers, timeout=timeout)

            break
         except requests.exceptions.ConnectionError as err:
            raise UserWarning('Connect Error: ' + str(err))
         except requests.exceptions.ReadTimeout as err:
            if x < retry-1:
               if self.debug:
                  print("\r\nDebug " + inspect.currentframe().f_code.co_name + ";\r\n", "Read Timeout, retry in ", str(retry_wait_sec), "sec. ...", "\r\n\r\n",
                        "#" * 50, "\r\n")
               time.sleep(retry_wait_sec)
               continue
            else:
               raise UserWarning('HTTP Read Error:, current timeout ' + str(
                  timeout) + ', you can increase it via key timeout in the *.json file - ' + str(err))
         except UserWarning as err:
            raise err
         except Exception as err:
            print('Unexpected error: ' + str(err))
            raise

      resJSON = json.loads(res.text)


      last_detected_errors = ''
      try:
         msg = ''
         msg += "\r\n\r\nRequest:\r\n"
         msg += str(json_byte, 'utf-8')
         msg += "\r\n\r\nResponse:\r\n"
         msg += res.text

         if res.status_code != 200:
            raise UserWarning('HTTP Error ' + str(res.status_code) + msg)

         if resJSON['errors'] != False:
            for items in resJSON['items']:
               try:
                  if items['index']['status'] != 200:
                     last_detected_errors += 'Doc Id - ' + items['index']['_id'] + "\r\n" + json.dumps(items['index']['error']) + "\r\n\r\n"
               except KeyError as err:
                  pass

            # do not raise an error, print only the last_detected_errors instead
            print('Bulk Error add/update index ' + msg + "\r\n\r\n" + "Last detected errors:\r\n" + last_detected_errors + "\r\n\r\n")

      except KeyError as err:
         raise UserWarning('JSON response format error, missing key: ' + str(err) + "\r\n\r\n" + msg)

      elapsed_time = time.time() - tick
      self.measure['timings'].update({'es_bulk': elapsed_time})

      if self.last_modified_timestamp_upd:
         self._sqlUpd()

   ###########################################################

   def _sqlUpd(self):
      db = self._rdsConnect()

      retry = None
      try:
         retry = int(self.config['rds']['retry'])

         if retry < 1:
            retry = 1

      except KeyError as err:
         retry = 1
         pass

      retry_wait_sec = None
      try:
         retry_wait_sec = int(self.config['rds']['retry_wait_sec'])
      except KeyError as err:
         retry_wait_sec = 1
         pass

      last_mod_field = self.config['sql']['last-modified-timestamp-field']
      last_mod_field = last_mod_field.split('.')



      if len(last_mod_field) != 3:
         raise UserWarning('format error, <schema>.<table>.<field>')

      ###########################################################################
      # optimized UPDATE I/O workload for better lock handling via single updates in transaction instead of IN()

      # sql = 'UPDATE ' + last_mod_field[0] + '.' + last_mod_field[1] + ' SET ' + last_mod_field[
      #    2] + ' = "1970-01-01 00:00:00" WHERE '
      #
      # i = 0
      # for item in self.upd_keys:
      #    last_mod_field_upd_key = item.split('=')
      #    upd_key_name = last_mod_field_upd_key[0]
      #    upd_key_val = last_mod_field_upd_key[1]
      #
      #    if i == 0:
      #       sql += upd_key_name + ' IN(' + upd_key_val + ','
      #    else:
      #       sql += upd_key_val + ','
      #
      #    i += 1
      #
      # sql = sql[0:-1]
      # sql += ');'

      base_sql = 'UPDATE ' + last_mod_field[0] + '.' + last_mod_field[1] + ' SET ' + last_mod_field[2] + ' = "1970-01-01 00:00:00" WHERE '
      sql = ''


      for item in self.upd_keys:
         last_mod_field_upd_key = item.split('=')
         upd_key_name = last_mod_field_upd_key[0]
         upd_key_val = last_mod_field_upd_key[1]

         sql += base_sql + upd_key_name + ' = ' + upd_key_val + '; '

      ###########################################################################

      if self.debug:
         print("\r\nDebug " + inspect.currentframe().f_code.co_name + ";\r\n", sql, "\r\n\r\n",
               "Key(s) to Update; " + str(len(self.upd_keys)), "\r\n", "#" * 50, "\r\n")

      tick = time.time()

      try:
         lock_messages_error = ['Deadlock found', 'Lock wait timeout exceeded']
         MAXIMUM_RETRY_ON_DEADLOCK = retry
         rcount = 0
         while rcount <= MAXIMUM_RETRY_ON_DEADLOCK:
            try:
               cursor = db.cursor()
               db.begin()
               cursor.executemany(sql, [])
               db.commit()

            except pymysql.err.OperationalError as err:
               (code, message) = err.args
               if any(msg in message for msg in lock_messages_error):
                  if  rcount < MAXIMUM_RETRY_ON_DEADLOCK:
                     rcount += 1
                     time.sleep(retry_wait_sec)
                     continue
                  elif rcount >= MAXIMUM_RETRY_ON_DEADLOCK:
                     raise UserWarning('DB Lock Error, retried ' + str(rcount) + ' times with ' + str(retry_wait_sec) + ' sec. per retry', err, sql)
               else:
                  raise

            break

      except pymysql.Warning as err:
         raise UserWarning('SQL warning', err, sql)
      except pymysql.err.ProgrammingError as err:
         raise UserWarning('SQL error', err, sql)


      elapsed_time = time.time() - tick
      self.measure['timings'].update({'sql_update': elapsed_time})

   ###########################################################

   def _do(self):
      json_byte = self._mapping

      if len(json_byte) > 0 and len(self.upd_keys) > 0:
         self._es_bulk(json_byte)
      elif self.debug:
         print('Info: no data found for update index - JSON len:', len(json_byte), 'update key count:',
               len(self.upd_keys))

   ###########################################################

   def enable_debug():
      global ES_INDEXER_DEBUG
      ES_INDEXER_DEBUG = True

   ###########################################################

   def disable_debug():
      global ES_INDEXER_DEBUG
      ES_INDEXER_DEBUG = False

   ###########################################################

   def measure():
      global ES_INDEXER_DEBUG
      return ES_INDEXER_MEASURE

###########################################################
###########################################################
###########################################################
