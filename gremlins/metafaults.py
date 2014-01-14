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
import logging

def pick_fault(fault_weights):
  def do():
    logging.info("pick_fault triggered")
    total_weight = sum( wt for wt,fault in fault_weights )
    pick = random.random() * total_weight
    accrued = 0
    for wt, fault in fault_weights:
      accrued += wt
      if pick <= accrued:
        fault()
        return
    assert "should not get here, pick=" + pick
  return do

def maybe_fault(likelyhood, fault):
  def do():
    logging.info("maybe_fault triggered, %3.2f likelyhood" % likelyhood)
    if random.random() <= likelyhood:
      fault()
    return
  return do
