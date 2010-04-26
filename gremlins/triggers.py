#!/usr/bin/env python
#
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import threading
import time
from BaseHTTPServer import HTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
import cgi

from gremlins import faults, metafaults

class Trigger(object):
  pass

class Periodic(Trigger):
  def __init__(self, period, fault):
    self.period = period
    self.fault = fault
    self.thread = threading.Thread(target=self._thread_body)
    self.thread.setDaemon(True)
    self.should_stop = False

  def start(self):
    self.thread.start()

  def stop(self):
    self.should_stop = True

  def join(self):
    self.thread.join()

  def _thread_body(self):
    logging.info("Periodic trigger starting")
    while not self.should_stop:
      logging.info("Periodic triggering fault " + repr(self.fault))
      self.fault()
      time.sleep(self.period)
    logging.info("Periodic trigger stopping")


class WebServerTrigger(Trigger):
  def __init__(self, port):
    self.port = port
    self.server = HTTPServer(('', port), WebServerTrigger.MyHandler)

  def start(self):
    self.thread = threading.Thread(target=self.server.serve_forever)
    self.thread.setDaemon(True)
    self.thread.start()
    time.sleep(60)

  def stop(self):
    self.server.shutdown()

  def join(self):
    self.thread.join()

  class MyHandler(SimpleHTTPRequestHandler):
    def do_POST(self):
      ctype,pdict = cgi.parse_header(self.headers.getheader('Content-type'))
      if ctype != 'multipart/form-data':
        self.sendresponse(500)
        self.end_headers()
        self.wfile.write('Must post form with fault= data')
        return

      query = cgi.parse_multipart(self.rfile, pdict)
      print query

      try:
        code = query.get('fault', ["NO FAULT"])[0]
        print "code: " + code
        result = eval(code, globals())
        if not result or not callable(result):
          raise "Fault must be a callable!"
        result()
        self.send_response(200)
        self.end_headers()
        self.wfile.write("Success! " + repr(result) + "\n")
      except Exception, e:
        self.send_response(500)
        self.end_headers()
        self.wfile.write('Error: ' + repr(e))
