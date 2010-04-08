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
from gremlins import faults
import signal
import logging

logging.basicConfig(level=logging.INFO)

active_faults = []
fault_weights=[

# kill -9s
  (1, faults.kill_daemon("HRegionServer", signal.SIGKILL, 20)),
  (1, faults.kill_daemon("DataNode", signal.SIGKILL, 20)),

# pauses (simulate GC?)
  (1, faults.pause_daemon("HRegionServer", 60)),
  (1, faults.pause_daemon("DataNode", 10)),

# drop packets (simulate network outage)
  (1, faults.drop_packets_to_daemon("DataNode", 20)),
  (1, faults.drop_packets_to_daemon("HRegionServer", 20)),

  ]

fault_frequency = 1

def inject_a_fault():
  fault = pick_fault()
  fault()

def pick_fault():
  total_weight = sum( wt for wt,fault in fault_weights )
  pick = random.random() * total_weight
  accrued = 0
  for wt, fault in fault_weights:
    accrued += wt
    if pick <= accrued:
      return fault

  assert "should not get here, pick=" + pick

def main():
  while True:
    time.sleep(fault_frequency)
    inject_a_fault()


if __name__ == "__main__":
  main()


