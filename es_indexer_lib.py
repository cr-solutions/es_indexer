# The contents of this file are subject to the Mozilla Public License
# Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
# https://www.mozilla.org/en-US/MPL/2.0/

# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either expressed or implied. See the License for
# the specific language governing rights and limitations under the License.

# The Initial Developers of the Original Code are:
# Copyright (c) 2019, PantherMedia (https://www.panthermedia.net), CR-Solutions (https://www.cr-solutions.net), Ricardo Cescon
# Contributor(s): Steffen Blaszkowski
# All Rights Reserved.


import pymysql, boto3, json, traceback, urllib3, requests, inspect, os, sys, re

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

   ###########################################################

   def __init__(self, s3bucket_filetype: str, s3prefix_folder: str, indexname: str, bulklimit: int, configfile: str = ''):
      global ES_INDEXER_DEBUG

      try:
         if ES_INDEXER_DEBUG:
            self.debug = True
      except NameError:
            pass


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

      #print('debug', __class__, inspect.currentframe().f_back.f_lineno)
      #return None

      if s3bucket_filetype.find('s3://') != -1:
         self._s3getConfig()
      else:
         self._fs_getConfig()


      if self.debug:
         print("\r\nDebug "+inspect.currentframe().f_code.co_name+":\r\n", self.config_file, "\r\n", self.config, "\r\n\r\n", "#"*50, "\r\n")

      self._do()

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
         raise UserWarning('Error read config from s3 bucket "' + self.s3bucket + '", File: "' + config_file + '" - ' + str(err))


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
         self.db = pymysql.connect(host=endpoint, port=port, user=user, passwd=pw, charset='utf8', connect_timeout=timeout)
      except pymysql.err.OperationalError as err:
         raise UserWarning('Error , current timeout ' + str(timeout) + ', you can increase it via key timeout in the *.json file - ' + str(err))

      return self.db

   ###########################################################

   def _sqlSelect(self):
      last_mod_field = ''
      data = ''
      try:
         last_mod_field = self.config['sql']['last-modified-timestamp-field']
         data = self.config['sql']['data']
      except KeyError as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error, missing key: '+str(err))


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
            query += ' LEFT JOIN ' + item["schema"] + '.' + item["table"] + ' ON ' + item["join"]


         query += ' WHERE ' + last_mod_field + ' != "1970-01-01 00:00:00" ORDER BY '+last_mod_field+' ASC LIMIT '+str(self.bulklimit)

      except KeyError as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error, missing key: '+str(err))


      return query

   ###########################################################

   def _execSelect(self):
      db = self._rdsConnect()

      cursor = db.cursor(pymysql.cursors.DictCursor)
      query = self._sqlSelect()

      if self.debug:
         print("\r\nDebug " + inspect.currentframe().f_code.co_name + ":\r\n", query, "\r\n\r\n", "#" * 50, "\r\n")

      try:
         cursor.execute(query)

      except pymysql.err.ProgrammingError as err:
         print('SQL error', err, query)

      rows = cursor.fetchall();
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
         raise UserWarning('JSON file ' + self.config_file + ' format error, missing key: '+str(err))


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

      if len(rows) > 0:
         fieldnames = rows[0].keys()

         for row in rows:
            mapping = self.config["mapping"]
            es_id = es_id_var_name
            last_mod_field_upd_key = self.config['sql']['last-modified-timestamp-upd-key']

            if '_comment' in mapping:
               del mapping['_comment']


            mapping_str = json.dumps(mapping)
            upd_key_str = ''

            for field in fieldnames:
                var = '$'+field
                val = str(row[field])

                val = val.replace("\n", '')  # remove linefeeds to not break JSON
                val = val.replace("\r", '')
                val = val.replace("\t", '')
                val = re.sub(pattern=r'([\"\\])', repl=r'\\\1', string=val)  # escape characters
                mapping_str = mapping_str.replace('"'+var+'"', '"'+val+'"')

                if es_id == var:
                   es_id = str(row[field])

                if upd_key_var == var:
                   upd_key_str = upd_key_name+'='+str(row[field])



            if es_id.find('$') != -1:
               raise UserWarning('no database field for internal ES _id mapping found')


            action = '{"index":{"_index":"'+self.indexname+'", "_type":"'+es_type+'", "_id":"'+es_id+'"}}' + "\n"

            if (sys.getsizeof(json_str) + sys.getsizeof(action) + sys.getsizeof(mapping_str)) > 1024 * 1024 * 5:  # max. MB size for bulk
               break

            json_str += action
            json_str += mapping_str + "\n"

            self.upd_keys.append(upd_key_str)



      json_byte = json_str.encode('utf-8')
      # for debug
      #print(json_byte)
      #return False

      return json_byte

   ###########################################################

   def _es_bulk(self, json_byte):
      if self.debug:
         print("\r\nDebug " + inspect.currentframe().f_code.co_name + ":\r\n", str(json_byte,'utf-8'), "\r\n\r\n", "#" * 50, "\r\n")

      headers = {"Content-Type": "application/json; charset=utf-8"}

      endpoint = ''
      try:
         endpoint = self.config['es']['endpoint']
      except KeyError as err:
         raise UserWarning('JSON file ' + self.config_file + ' format error, missing key: '+str(err))

      timeout = None
      try:
         timeout = int(self.config['es']['timeout'])
      except KeyError as err:
         timeout = 3
         pass


      urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # to support local ES endpoints via SSH tunnel, sample: https://127.0.0.1:9200

      res = None
      try:
         res = requests.put(url=endpoint+'/_bulk', verify=False, data=json_byte,  headers=headers, timeout=timeout)
      except requests.exceptions.ConnectionError as err:
         raise UserWarning('Connect Error '+str(err))
      except requests.exceptions.ReadTimeout as err:
         raise UserWarning('HTTP Read Error, current timeout ' + str(timeout) + ', you can increase it via key timeout in the *.json file - ' + str(err))

      resJSON = json.loads(res.text)

      try:
         msg = ''
         msg += "\r\n\r\nRequest:\r\n"
         msg += str(json_byte,'utf-8')
         msg += "\r\n\r\nResponse:\r\n"
         msg += res.text

         if res.status_code != 200:
            raise UserWarning('HTTP Error ' + str(res.status_code) + msg)

         if resJSON['errors'] != False:
            raise UserWarning('Error add/update index '+msg)

      except KeyError as err:
         raise UserWarning('JSON response format error, missing key: '+str(err)+"\r\n\r\n"+msg)


      self._sqlUpd()

   ###########################################################

   def _sqlUpd(self):
      db = self._rdsConnect()

      last_mod_field = self.config['sql']['last-modified-timestamp-field']
      last_mod_field = last_mod_field.split('.')

      if len(last_mod_field) != 3:
         raise UserWarning('format error, <schema>.<table>.<field>')


      sql = 'UPDATE '+last_mod_field[0]+'.'+last_mod_field[1]+' SET '+last_mod_field[2]+' = "1970-01-01 00:00:00" WHERE '

      for item in self.upd_keys:
         sql += item+' OR '

      sql = sql[0:-4]


      if self.debug:
         print("\r\nDebug " + inspect.currentframe().f_code.co_name + ":\r\n", sql, "\r\n\r\n", "#" * 50, "\r\n")

      try:
         cursor = db.cursor()
         cursor.execute(sql)
         db.commit()

      except pymysql.err.ProgrammingError as err:
         print('SQL error', err, query)

   ###########################################################

   def _do(self):
      json_byte = self._mapping
      self._es_bulk(json_byte)


   ###########################################################

   def enable_debug():
      global ES_INDEXER_DEBUG
      ES_INDEXER_DEBUG = True

   ###########################################################

   def disable_debug():
      global ES_INDEXER_DEBUG
      ES_INDEXER_DEBUG = False

   ###########################################################



###########################################################
###########################################################
###########################################################
