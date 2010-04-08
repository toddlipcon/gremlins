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
import re
from gremlins import procutils
import time

IPTABLES="/sbin/iptables"

def list_chains():
  """Return a list of the names of all iptables chains."""
  ret = procutils.run([IPTABLES, "-L"])
  chains = re.findall(r'^Chain (\S+)', ret, re.MULTILINE)
  return chains

def create_gremlin_chain(ports_to_drop):
  """
  Create a new iptables chain that drops all packets
  to the given list of ports.

  @param ports_to_drop: list of int port numbers to drop packets to
  @returns the name of the new chain
  """
  chain_id = "gremlin_%d" % int(time.time())
  
  # Create the chain
  procutils.run([IPTABLES, "-N", chain_id])

  # Add the drop rules
  for port in ports_to_drop:
    procutils.run([IPTABLES,
      "-A", chain_id,
      "-p", "tcp",
      "--dport", str(port),
      "-j", "DROP"])
  return chain_id

def add_user_chain_to_input_chain(chain_id):
  """Insert the given user chain into the system INPUT chain"""
  procutils.run([IPTABLES, "-A", "INPUT", "-j", chain_id])

def remove_user_chain_from_input_chain(chain_id):
  """Remove the given user chain from the system INPUT chain"""
  procutils.run([IPTABLES, "-D", "INPUT", "-j", chain_id])

def delete_user_chain(chain_id):
  """
  Delete a user chain.

  You must remove it from the system chains before this will succeed.
  """
  procutils.run([IPTABLES, "--flush", chain_id])
  procutils.run([IPTABLES, "--delete-chain", chain_id])

def remove_gremlin_chains():
  """
  Remove any gremlin chains that are found on the system.
  """
  for chain in list_chains():
    if chain.startswith("gremlin_"):
      remove_user_chain_from_input_chain(chain)
      delete_user_chain(chain)

