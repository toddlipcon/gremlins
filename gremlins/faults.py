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

from gremlins import procutils, iptables
import signal
import os
import subprocess
import logging
import time

def kill_daemons(daemons, signal, restart_after):
  """Kill the given daemons with the given signal, then
  restart them after the given number of seconds.

  @param daemons: the names of the daemon (eg HRegionServer)
  @param signal: signal to kill with
  @param restart_after: number of seconds to sleep before restarting
  """
  def do():
    # First kill
    for daemon in daemons:
      pid = procutils.find_jvm(daemon)
      if pid:
        logging.info("Killing %s pid %d with signal %d" % (daemon, pid, signal))
        os.kill(pid, signal)
      else:
        logging.info("There was no %s running!" % daemon)

    logging.info("Sleeping for %d seconds" % restart_after)
    time.sleep(restart_after)

    for daemon in daemons:
      logging.info("Restarting %s" % daemon);
      procutils.start_daemon(daemon)
  return do

def pause_daemons(jvm_names, seconds):
  """
  Pause the given daemons for some period of time using SIGSTOP/SIGCONT

  @param jvm_names: the names of the class to pause: eg ["DataNode"]
  @param seconds: the number of seconds to pause for
  """
  def do():
    # Stop all daemons, record their pids
    for jvm_name in jvm_names:
      pid = procutils.find_jvm(jvm_name)
      if not pid:
        logging.warn("No pid found for %s" % jvm_name)
        continue
      logging.warn("Suspending %s pid %d for %d seconds" % (jvm_name, pid, seconds))
      os.kill(pid, signal.SIGSTOP)

    # Pause for prescribed amount of time
    time.sleep(seconds)

    # Resume them
    for jvm_name in jvm_names:
      pid = procutils.find_jvm(jvm_name)
      if pid:
        logging.warn("Resuming %s pid %d" % (jvm_name, pid))
        os.kill(pid, signal.SIGCONT)
  return do

def drop_packets_to_daemons(daemons, seconds):
  """
  Determines which TCP ports the given daemons are listening on, and sets up
  an iptables firewall rule to drop all packets to any of those ports
  for a period of time.

  @param daemons: the JVM class names of the daemons
  @param seconds: how many seconds to drop packets for
  """
  def do():
    logging.info("Going to drop packets from %s for %d seconds..." %
                 (repr(daemons), seconds))

    # Figure out what ports the daemons are listening on
    all_ports = []
    for daemon in daemons:
      pid = procutils.find_jvm(daemon)
      if not pid:
        logging.warn("Daemon %s not running!" % daemon)
        continue
      ports = procutils.get_listening_ports(pid)
      logging.info("%s is listening on ports: %s" % (daemon, repr(ports)))
      all_ports.extend(ports)

    if not all_ports:
      logging.warn("No ports found for daemons: %s. Skipping fault." % repr(daemons))
      return

    # Set up a chain to drop the packets
    chain = iptables.create_gremlin_chain(all_ports)
    logging.info("Created iptables chain: %s" % chain)
    iptables.add_user_chain_to_input_chain(chain)

    logging.info("Gremlin chain %s installed, sleeping %d seconds" % (chain, seconds))
    time.sleep(seconds)

    logging.info("Removing gremlin chain %s" % chain)
    iptables.remove_user_chain_from_input_chain(chain)
    iptables.delete_user_chain(chain)
    logging.info("Removed gremlin chain %s" % chain)
  return do

def fail_network(bastion_host, seconds, restart_daemons=None, use_flush=False):
  """
  Cuts off all network traffic for this host, save ssh to/from a given bastion host,
  for a period of time.

  @param bastion_host: a host or ip to allow ssh with, just in case
  @param seconds: how many seconds to drop packets for
  @param restart_daemons: optional list of daemon processes to restart after network is restored
  @param use_flush: optional param to issue an iptables flush rather than manually remove chains from INPUT/OUTPUT
  """
  def do():
    logging.info("Going to drop all networking (save ssh with %s) for %d seconds..." %
                 (bastion_host, seconds))
    # TODO check connectivity, or atleast DNS resolution, for bastion_host
    chains = iptables.create_gremlin_network_failure(bastion_host)
    logging.info("Created iptables chains: %s" % repr(chains))
    iptables.add_user_chain_to_input_chain(chains[0])
    iptables.add_user_chain_to_output_chain(chains[1])

    logging.info("Gremlin chains %s installed, sleeping %d seconds" % (repr(chains), seconds))
    time.sleep(seconds)

    if use_flush:
      logging.info("Using flush to remove gremlin chains")
      iptables.flush()
    else:
      logging.info("Removing gremlin chains %s" % repr(chains))
      iptables.remove_user_chain_from_input_chain(chains[0])
      iptables.remove_user_chain_from_output_chain(chains[1])
    iptables.delete_user_chain(chains[0])
    iptables.delete_user_chain(chains[1])
    logging.info("Removed gremlin chains %s" % repr(chains))
    if restart_daemons:
      logging.info("Restarting daemons: %s", repr(restart_daemons))
      for daemon in restart_daemons:
        procutils.start_daemon(daemon)
  return do
