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

import random
import time
from gremlins import faults, profiles
import signal
import logging
from optparse import OptionParser
import sys

LOG_FORMAT='%(asctime)s %(module)-12s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

def run_profile(profile):
  for trigger in profile:
    trigger.start()

  logging.info("Started profile")
  while True:
    time.sleep(1)

  for trigger in profile:
    trigger.stop()
    trigger.join()


def main():
  parser = OptionParser()
  parser.add_option("-m", "--import-module", dest="modules",
    help="module to import", metavar="MODULE", action="append")
  parser.add_option("-p", "--profile", dest="profile",
    help="fault profile to run", metavar='PROFILE')
  parser.add_option("-f", "--fault", dest="faults", action="append",
    help="faults to run", metavar='FAULT')

  (options, args) = parser.parse_args()

  things_to_do = 0
  if options.profile: things_to_do += 1
  if options.faults: things_to_do += 1

  if len(args) > 0 or things_to_do != 1:
    parser.print_help(sys.stderr)
    sys.exit(1)

  print repr(options)
  imported_modules = {}
  for m in options.modules:
    name = m.split(".")[-1]
    imported = __import__(m, {}, {}, name)
    imported_modules[name] = imported

  def eval_arg(arg):
    eval_globals = dict(globals())
    eval_globals.update(imported_modules)
    return eval(arg, eval_globals)

  if options.profile:
    run_profile(eval_arg(options.profile))
  elif options.faults:
    for fault_arg in options.faults:
      fault = eval_arg(fault_arg)
      fault()

if __name__ == "__main__":
  main()


