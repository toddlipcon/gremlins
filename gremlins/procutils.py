#!/usr/bin/env python

import re
import signal
import os
import subprocess
import logging

HBASE_HOME=os.getenv("HBASE_HOME", "/home/todd/monster-cluster/hbase")
HADOOP_HOME=os.getenv("HADOOP_HOME", "/home/todd/monster-cluster/hadoop-0.20.1+169.66")
LSOF=os.getenv("LSOF", "lsof")
JPS=os.getenv("JPS", "jps")

START_COMMANDS = {
  'HRegionServer': [HBASE_HOME + "/bin/hbase-daemon.sh", "start", "regionserver"],
  'DataNode': [HADOOP_HOME + "/bin/hadoop-daemon.sh", "start", "datanode"],
}


def run(cmdv):
  """Run a command.

  Throws an exception if it has a nonzero exit code.
  Returns the output of the command.
  """
  proc = subprocess.Popen(args=cmdv, stdout=subprocess.PIPE)
  (out, err) = proc.communicate()
  if proc.returncode != 0:
    raise Exception("Bad status code: %d" % proc.returncode)
  return out


def start_daemon(daemon):
  """Start the given daemon."""
  if daemon not in START_COMMANDS:
    raise Exception("Don't know how to start a %s" % daemon)
  cmd = START_COMMANDS[daemon]
  logging.info("Starting %s: %s" % (daemon, repr(cmd)))
  ret = subprocess.call(cmd)
  if ret != 0:
    logging.warn("Ret code %d starting %s" % (ret, daemon))

def find_jvm(java_command):
  """
  Find the jvm for the given java class by running jps

  Returns the pid of this JVM, or None if it is not running.
  """
  ret = run([JPS]).split("\n")
  for line in ret:
    if not line: continue
    pid, command = line.split(' ', 1)
    if command == java_command:
      logging.info("Found %s: pid %s" % (java_command, pid))
      return int(pid)
  logging.info("Found no running %s" % java_command)
  return None

def get_listening_ports(pid):
  """Given a pid, return a list of TCP ports it is listening on."""
  ports = []
  lsof_data = run([LSOF, "-p%d" % pid, "-n", "-a", "-itcp", "-P"]).split("\n")
  # first line is a header
  del lsof_data[0]
  # Parse out the LISTEN rows
  for record in lsof_data:
    m = re.search(r'TCP\s*.+?:(\d+)\s*\(LISTEN\)', record)
    if m:
      ports.append( int(m.group(1)) )
  return ports
