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
import os

from gremlins import faults, metafaults, triggers, hostutils

bastion = os.getenv("GREMLINS_BASTION_HOST", hostutils.guess_remote_host())

if not bastion:
  raise Exception("GREMLINS_BASTION_HOST not set, and I couldn't guess your remote host.")

logging.info("Using %s as bastion host for network failures. You should be able to ssh from that host at all times." % bastion)

fail_node_long = faults.fail_network(bastion_host=bastion, seconds=300, restart_daemons=["Accumulo-All"], use_flush=True)
# XXX make sure this is greater than ZK heartbeats
fail_node_short = faults.fail_network(bastion_host=bastion, seconds=45, restart_daemons=["Accumulo-All"], use_flush=True)
# XXX make sure this is less than ZK heartbeats
fail_node_transient = faults.fail_network(bastion_host=bastion, seconds=10, restart_daemons=["Accumulo-All"], use_flush=True)

profile = [
  triggers.Periodic(
# How often do you want a failure? for master nodes, you should probably give enough time for recovery ~5-15 minutes
    60,
    metafaults.maybe_fault(
# How likely do you want a failure? decreasing this will make failures line up across nodes less often.
      0.33,
      metafaults.pick_fault([
# You can change the weights here to see different kinds of flaky nodes
        (1, fail_node_long),
        (1, fail_node_short),
        (2, fail_node_transient),
      ]))
    ),
  ]

