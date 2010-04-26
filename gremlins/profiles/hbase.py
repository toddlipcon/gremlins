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

import signal

from gremlins import faults, metafaults, triggers

rs_kill_long = faults.kill_daemons(["HRegionServer"], signal.SIGKILL, 100)
rs_kill_short = faults.kill_daemons(["HRegionServer"], signal.SIGKILL, 3)

dn_kill_long = faults.kill_daemons(["DataNode"], signal.SIGKILL, 100)
dn_kill_short = faults.kill_daemons(["DataNode"], signal.SIGKILL, 3)

rs_pause = faults.pause_daemons(["HRegionServer"], 62)
dn_pause = faults.pause_daemons(["DataNode"], 20)

# This fault isn't that useful yet, since it only drops inbound packets
# but outbound packets (eg, the ZK pings) keep going.
rs_drop_inbound_packets = faults.drop_packets_to_daemons(["HRegionServer"], 64)

profile = [
  triggers.Periodic(
    45,
    metafaults.pick_fault([
    # kill -9s
      (5, rs_kill_long),
      (1, dn_kill_long),
    # fast kill -9s
      (5, rs_kill_short),
      (1, dn_kill_short),

    # pauses (simulate GC?)
      (10, rs_pause),
      (1, dn_pause ),

    # drop packets (simulate network outage)
      #(1, faults.drop_packets_to_daemons(["DataNode"], 20)),
      #(1, rs_drop_inbound_packets),

      ])),
#  triggers.WebServerTrigger(12321)
  ]

