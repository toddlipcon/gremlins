#!/usr/bin/env python

from gremlins import procutils, iptables
import signal
import os
import subprocess
import logging
import time


def kill_daemon(daemon, signal, restart_after):
  """Kill the given daemon with the given signal, then
  restart it after the given number of seconds.

  @param daemon: the name of the daemon (eg HRegionServer)
  @param signal: signal to kill with
  @param restart_after: number of seconds to sleep before restarting
  """
  def do():
    pid = procutils.find_jvm(daemon)
    if pid:
      logging.info("Killing %s pid %d with signal %d" % (daemon, pid, signal))
      os.kill(pid, signal)
      logging.info("Sleeping for %d seconds" % restart_after)
      time.sleep(restart_after)
    else:
      logging.info("There was no %s running!" % daemon)
    logging.info("Restarting %s" % daemon);
    procutils.start_daemon(daemon)
  return do

def pause_daemon(jvm_name, seconds):
  """
  Pause the given daemon for some period of time using SIGSTOP/SIGCONT

  @param jvm_name: the name of the class to pause: eg DataNode
  @param seconds: the number of seconds to pause for
  """
  def do():
    pid = procutils.find_jvm(jvm_name)
    if not pid:
      logging.warn("No pid found for %s" % jvm_name)
      return
    logging.warn("Suspending %s pid %d for %d seconds" % (jvm_name, pid, seconds))
    os.kill(pid, signal.SIGSTOP)
    time.sleep(seconds)
    logging.warn("Resuming %s pid %d" % (jvm_name, pid))
    os.kill(pid, signal.SIGCONT)
  return do

def drop_packets_to_daemon(daemon, seconds):
  """
  Determines which TCP ports the given daemon is listening on, and sets up
  an iptables firewall rule to drop all packets to any of those ports
  for a period of time.

  @param daemon: the JVM class name of the daemon
  @param seconds: how many seconds to drop packets for
  """
  def do():
    logging.info("Going to drop packets from %s for %d seconds..." % (daemon, seconds))
    pid = procutils.find_jvm(daemon)
    if not pid:
      logging.warn("Daemon %s not running!" % daemon)
      return
    ports = procutils.get_listening_ports(pid)
    logging.info("%s is listening on ports: %s" % (daemon, repr(ports)))

    chain = iptables.create_gremlin_chain(ports)
    logging.info("Created iptables chain: %s" % chain)
    iptables.add_user_chain_to_input_chain(chain)

    logging.info("Gremlin chain %s installed, sleeping %d seconds" % (chain, seconds))
    time.sleep(seconds)

    logging.info("Removing gremlin chain %s" % chain)
    iptables.remove_user_chain_from_input_chain(chain)
    iptables.delete_user_chain(chain)
    logging.info("Removed gremlin chain %s" % chain)
  return do

