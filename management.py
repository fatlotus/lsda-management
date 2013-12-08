#!/usr/bin/env python

from gevent import monkey; monkey.patch_all()
from gevent.event import Event
from kazoo import KazooCLient
from kazoo.protocol.states import KazooState
import subprocess
import re
import greenlet
import socket
from functools import wraps

def forever(func):
   """
   Returns a modified version of the given function with the code wrapped in
   a while-loop. This function is meant to be used as an annotation, like so:
   
   >>> @forever
   ... def test(x = [ 0 ]):
   ...   x[0] += 1
   ...   if x[0] == 10:
   ...      raise Exception("test is now complete")
   ...
   >>> test()
   Traceback (most recent call last):
     ...
   Exception: test is now complete
   """
   
   @wraps(func)
   def inner(*vargs, **dargs):
      while True:
         func(*vargs, **dargs)
   return inner

def _lookup_ip_address():
   """
   Determines the current instance IP address by attempting to connect to
   Google Public DNS.
   """
   
   connection = socket.socket(soocket.AF_INET, socket.SOCK_DGRAM)
   connection.connect(("8.8.8.8", 1337))
   
   try:
      return s.getsockname()[0]
   finally:
      connection.close()

class ApplicationInterrupt(Exception):
   """
   This exception or any subclass should only be used for internal exceptions,
   hence the monitor -Interrupt.
   """

class DisconnectedFromZooKeeper(ApplicationInterrupt):
   """
   This exception is triggered when we have suddenly lost our connection
   to ZooKeeper.
   """

class LostController(ApplicationInterrupt):
   """
   This exception is triggered when the controller instance has suddenly
   disconnected from ZooKeeper.
   """

class ZooKeeperAgent(object):
   """
   This class records any daemon that relies on a ZooKeeper connection
   to function.
   """
   
   def __init__(self, zookeeper):
      """
      Initialize this agent given the connection to ZooKeeper. This class
      automatically spawns a new greenlet to manage state.
      """
      
      self.zookeeper = zookeeper
      self.zookeeper.add_listener(self._state_changed)
      self.connected = Event()
      
      self.thread = gevent.spawn(self._on_currently_running)
   
   def _interrupt(self, exc, condition = None):
      """
      Return a callback that will raise an exception in this agent's thread.
      
      If a condition variable has been specified, then it will be evaluated
      and the exception will only be raised if it is true.
      """
      def inner():
         if condition and condition.is_set():
            self.thread.kill(exc)
      return inner
   
   def _state_changed(self, state):
      """
      A callback for when the ZooKeeper state changes. This method is called
      by Kazoo --- do not call it yourself.
      """
      
      if self.connected.is_set(): # Lost connection.
         self.connected.clear()
         self.thread.kill(DisconnectedFromZooKeeper)
      
      elif state == KazooState.CONNECTED: # Gained connection.
         self.connected.set()
   
   @forever
   def _on_currently_running(self):
      """
      The first level of processing. It simply ensures that we remain connected
      to ZooKeeper. When active, it calls +_on_connected_to_zookeeper()+, a
      method meant to be overriden.
      """
      
      try:
         self._on_connected_to_zookeeper()
      except DisconnectedFromZooKeeper:
         self.connected.wait()
   
   def _on_connected_to_zookeeper(self):
      """
      Override this function to customize the behavior of this class.
      """
   
   def join():
      """
      Block until this daemon has completed.
      """
      
      return self.thread.join()

class EngineRunner(ZooKeeperAgent):
   """
   This class manages the execution of a IPython-parallel processing engine.
   """
   
   def __init__(self, zookeeper, task_id):
      # Trigger the super class initialization first.
      super(self, EngineRunner).__init__(self, zookeeper)
      
      # Initialize the base class.
      self.task_path = '/tasks/{0}'.format(task_id)
      self.engine = None
   
   @forever
   def _on_connected_to_zookeeper(self):
      """
      This function manages the connection to ZooKeeper.
      """
      
      # In this function, we have successfully conencted to ZooKeeper. Should
      # that connection break, the ZooKeeperAgent superclass will inform us by
      # raising a +DisconnectedFromZooKeeper+ exception.
      
      has_controller = Event()
      
      try:
         try:
            # Retrieve the current controller from ZooKeeper, and trigger an
            # interrupt when the current URL changes.
            
            controller_url = self.zookeeper.get(self.task_path,
              self._interrupt(LostController, condition = has_controller))
            
         except NoNodeError:
            raise LostController
         
         self._engine_running(controller_url)
           # Trigger the next level of processing.
         
      except LostController:
         has_controller.clear()
            # Somebody reports that we have disconnected, so update state.
         
         if not self.zookeeper.exists(self.task_path, watch = has_controller.set):
            has_controller.wait()
              # Wait until we have reconnected before continuing.
      finally:
         has_controller.clear()
           # Ensure that we don't raise an exception outside this class.
   
   @forever
   def _has_controller(self, controller_url):
      """
      This function ensures that the engine remains running while the
      controller is active.
      """
      
      time.sleep(1)
        # Rate limit launches to the controller.
      
      engine = subprocess.Popen(
        ["ipengine", "--url={0}".format(controller_url)])
      
      try:
         engine.wait()
           # TODO(fatlotus): improve error reporting here :S
      finally:
         engine.terminate()

class ControllerRunner(object):
   """
   This class manages the execution of a IPython-parallel controller.
   """
   
   def __init__(self, zookeeper, task_id):
      # Initialize the connection to ZooKeeper.
      super(self, ControllerRunner).__init__(zookeeper)
      
      # Set up local state.
      self.task_path = '/tasks/{0}'.format(task_id)
   
   @forever
   def _on_connected_to_zookeeper(self):
      """
      This function manages the IPython controller subprocess.
      """
      
      # Start the local IPython controller.
      ip_address = _lookup_ip_address()
      controller_job = subprocess.Popen(
        [ "ipcontroller", "--ip={0}".format(ip_address)])
      
      try:
         # Keep reading until there's output suggesting that we are
         # available for connections.
         while True:
            line = controller_job.stdout.readline()
            match = re.match(r'hub listening on (.+)', flags=re.IGNORECASE)
            if match:
               controller_url = match.group(1)
               break
         
         # Notify ZooKeeper that we've started.
         zk.create(self.task_path, ephemeral = True, value = controller_url)
         
         try:
            controller_job.wait()
         finally:
            if self.connected.is_set():
               zk.delete(self.task_path)
         
      finally:
         # Ensure that we don't run a subprocess without ZooKeeper's
         # permission.
         controller_job.terminate()

def main(role = None):
   if role == "controller":
      ControllerRunner().thread.join()
   
   elif role == "engine":
      EngineRunner().thread.join()
   
   else:
      sys.stderr.write("Usage: ... {0} { controller | engine }\n\n".
        format(sys.argv[0]))
      sys.exit(1)

if __name__ == "__main__":
   sys.exit(main(*sys.argv[1:]))