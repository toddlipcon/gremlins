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

import os
from gremlins import procutils, iptables

LASTCMD = "/usr/bin/last"

def guess_remote_host():
  """
  Attempt to find the host our current user last logged in from.
  """
  user = os.environ.get("USER")
  sudo_user = os.environ.get("SUDO_USER")
  if sudo_user:
    user = sudo_user
  if user:
    last = procutils.run([LASTCMD, "-a", user, "-n", "1"]).splitlines()[0]
    return last.rpartition(' ')[2]
  else:
    return None
