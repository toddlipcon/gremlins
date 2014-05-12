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

def create_gremlin_network_failure(bastion_host):
  """
  Create a new iptables chain that isolates the host we're on
  from all other hosts, save a single bastion.

  @param bastion_host: a hostname or ip to still allow ssh to/from
  @returns an array containing the name of the new chains [input, output]
  """
  chain_prefix = "gremlin_%d" % int(time.time())

  # Create INPUT chain
  chain_input = "%s_INPUT" % chain_prefix
  procutils.run([IPTABLES, "-N", chain_input])

  # Add rules to allow ssh to/from bastion
  procutils.run([IPTABLES, "-A", chain_input, "-p", "tcp",
    "--source", bastion_host, "--dport", "22",
    "-m", "state", "--state", "NEW,ESTABLISHED",
    "-j", "ACCEPT"])
  procutils.run([IPTABLES, "-A", chain_input, "-p", "tcp",
    "--sport", "22",
    "-m", "state", "--state", "ESTABLISHED",
    "-j", "ACCEPT"])

  # Add rule to allow ICMP to/from bastion
  procutils.run([IPTABLES, "-A", chain_input, "-p", "icmp",
    "--source", bastion_host,
    "-j", "ACCEPT"])
  # Drop everything else
  procutils.run([IPTABLES, "-A", chain_input,
    "-j", "DROP"])

  # Create OUTPUT chain
  chain_output = "%s_OUTPUT" % chain_prefix
  procutils.run([IPTABLES, "-N", chain_output])

  # Add rules to allow ssh to/from bastion
  procutils.run([IPTABLES, "-A", chain_output, "-p", "tcp",
    "--sport", "22",
    "-m", "state", "--state", "ESTABLISHED",
    "-j", "ACCEPT"])
  procutils.run([IPTABLES, "-A", chain_output, "-p", "tcp",
    "--destination", bastion_host, "--dport", "22",
    "-m", "state", "--state", "NEW,ESTABLISHED",
    "-j", "ACCEPT"])
  # Add rule to allow ICMP to/from bastion
  procutils.run([IPTABLES, "-A", chain_output, "-p", "icmp",
    "--destination", bastion_host,
    "-j", "ACCEPT"])
  # Drop everything else
  procutils.run([IPTABLES, "-A", chain_output,
    "-j", "DROP"])

  return [chain_input, chain_output]

def add_user_chain_to_input_chain(chain_id):
  """Insert the given user chain into the system INPUT chain"""
  procutils.run([IPTABLES, "-A", "INPUT", "-j", chain_id])

def remove_user_chain_from_input_chain(chain_id):
  """Remove the given user chain from the system INPUT chain"""
  procutils.run([IPTABLES, "-D", "INPUT", "-j", chain_id])

def add_user_chain_to_output_chain(chain_id):
  """Insert the given user chain into the system OUTPUT chain"""
  procutils.run([IPTABLES, "-A", "OUTPUT", "-j", chain_id])

def remove_user_chain_from_output_chain(chain_id):
  """Remove the given user chain from the system OUTPUT chain"""
  procutils.run([IPTABLES, "-D", "OUTPUT", "-j", chain_id])

def flush(chain_id=None):
  """
  Flush iptables chains. Defaults to all chains.

  @param chain_id optionally limit flushing to given chain
  """
  if chain_id:
    procutils.run([IPTABLES, "--flush", chain_id])
  else:
    procutils.run([IPTABLES, "--flush"])

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
  output_chains = map((lambda entry: entry.partition(" ")[0]), procutils.run([IPTABLES, "-L", "OUTPUT"]).splitlines()[2:])

  for chain in list_chains():
    if chain.startswith("gremlin_"):
      if chain in output_chains:
        remove_user_chain_from_output_chain(chain)
      else:
        remove_user_chain_from_input_chain(chain)
      delete_user_chain(chain)

