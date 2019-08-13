# target-himydata
# Copyright 2018 Himydata, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at
# himydata, Inc.(https://himydata.com/).

import os
import logging
import requests
import json
from target_himydata.buffer import Buffer

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE_BYTES = 4194304
DEFAULT_BATCH_DELAY_MILLIS = 60000

class Client(object):

    _buffer = Buffer()

    def __init__(self,
                 client_id,
                 api_key,
                 table_name=None,
                 callback_function=None,
                 himydata_url=None,
                 batch_size_bytes=DEFAULT_BATCH_SIZE_BYTES,
                 batch_delay_millis=DEFAULT_BATCH_DELAY_MILLIS):

        assert isinstance(client_id, int), 'client_id is not an integer: {}'.format(client_id)  # nopep8

        self.client_id = client_id
        self.api_key = api_key
        self.table_name = table_name
        self.himydata_url = himydata_url
        self.batch_size_bytes = batch_size_bytes
        self.batch_delay_millis = batch_delay_millis
        self.callback_function = callback_function

    def push(self, himydata_record, table_name, callback_arg=None):
        buffer_item = {}
        buffer_item["record"] = himydata_record
        buffer_item["client_id"] = self.client_id
        buffer_item["table_name"] = table_name

        self._buffer.put(buffer_item, callback_arg)            

        batch = self._buffer.take(
            self.batch_size_bytes, self.batch_delay_millis)
        if batch is not None:
            self._send_batch(batch)


    def _himydata_request(self, client_id, records, table_name):
        url = "{}singerio/manage/{}/records/".format(self.himydata_url, str(table_name))
        headers = {
            'Authorization': 'Token {}'.format(self.api_key),
            'Content-Type': 'application/json'}
        return requests.post(url, headers=headers, data=records)

    def _send_batch(self, batch):
        logger.debug("Sending batch of %s entries", len(batch))
        records = {}
        for entry in batch:
            client_id = entry.value["client_id"] #never changes
            table_name = entry.value["table_name"]
            if table_name not in records:
                records[table_name] = []
            records[table_name].append(entry.value["record"])
        for table_name, data in records.items():
            data = json.dumps(data) #up to this point, data is a list/dict, stringify for request
            response = self._himydata_request(client_id, data, table_name)
            if response.status_code < 300:
                if self.callback_function is not None:
                    self.callback_function([x.callback_arg for x in batch])
            else:
                raise RuntimeError("Error sending data to the Himydata Platform  API. {0.status_code} - {0.content}"  # nopep8
                                   .format(response))

    def check_dataset(self, record, table_name):
        url = "{}singerio/manage/{}/schema/".format(self.himydata_url, str(table_name))
        headers = {
            'Authorization': 'Token {}'.format(self.api_key),
            'Content-Type': 'application/json'}
        data = json.dumps(record) 
        return requests.post(url, headers=headers, data=data)
        


    def flush(self):
        while True:
            batch = self._buffer.take(0, 0)
            if batch is None:
                return

            self._send_batch(batch)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.flush()
