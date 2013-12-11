#!/usr/bin/env python

from gevent import monkey; monkey.patch_all()
from gevent.event import Event
from gevent import subprocess
import greenlet, gevent

from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.handlers.gevent import SequentialGeventHandler

import pika

import re, socket, argparse, sys, logging, time
from functools import wraps, partial

# logging.getLogger().setLevel(logging.INFO)

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

class DeepBreakException(Exception):
   pass

class Interruptable(object):
   """
   Allows breaking out of a deeply-nested block of code.
   """
   
   def __init__(self, description):
      self.exception = DeepBreakException()
      self.active = True
      self.thread = gevent.getcurrent()
      self.description = description
   
   def __enter__(self):
      self.active = True
      return self
   
   def __exit__(self, kind, instance, traceback):
      self.active = False
      return instance is self.exception
   
   def interrupt(self, *vargs, **dargs):
      if self.active:
         self.thread.kill(self.exception)

def wait_forever():
   """
   This function never returns.
   """
   
   while True:
      gevent.sleep(60)

def once(object, func):
   """
   Adds the given function as a one-time listener to the given object.
   """
   def inner(*vargs, **dargs):
      if func(*vargs, **dargs):
         object.remove_listener(func)
   object.add_listener(inner)

def _lookup_ip_address():
   """
   Determines the current instance IP address by attempting to connect to
   Google Public DNS.
   """
   
   connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
   connection.connect(("8.8.8.8", 1337))
   
   try:
      return connection.getsockname()[0]
   finally:
      connection.close()

class ApplicationInterrupt(Exception):
   """
   This exception or any subclass should only be used for internal exceptions,
   hence the moniker -Interrupt.
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

class TaskComplete(ApplicationInterrupt):
   """
   This exception is triggered when the application has finished processing
   a work item.
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
      self.thread = gevent.spawn(self._on_currently_running)
   
   @forever
   def _on_currently_running(self):
      """
      The first level of processing. It simply ensures that we remain connected
      to ZooKeeper. When active, it calls +_on_connected_to_zookeeper()+, a
      method meant to be overriden.
      """
      
      if self.zookeeper.state != 'CONNECTED':
         with Interruptable("Disconnected") as disconnected:
            once(self.zookeeper, lambda x: gevent.spawn(disconnected.interrupt)
                                   if x == 'CONNECTED' else True)
         
            wait_forever()
      
      with Interruptable("Connected") as connected:
         once(self.zookeeper, lambda x: gevent.spawn(connected.interrupt)
                                if x != 'CONNECTED' else True)
         
         self._on_connected_to_zookeeper()
   
   def _on_connected_to_zookeeper(self):
      """
      Override this function to customize the behavior of this class.
      """
   
   def join(self):
      """
      Block until this daemon has completed.
      """
      
      self.thread.get()

class EngineOrControllerRunner(ZooKeeperAgent):
   """
   This task initializes an agent that could run either an IPython
   controller or IPython engine task.
   """
   
   def __init__(self, zookeeper, amqp_channel, queue_name):
      # Initialize the connection to ZooKeeper.
      super(EngineOrControllerRunner, self).__init__(zookeeper)
      
      # Save local state.
      self.amqp_channel = amqp_channel
      self.queue_name = queue_name
   
   @forever
   def _on_connected_to_zookeeper(self):
      """
      This function manages the daemon to pull tasks from AMQP.
      """
      
      # Ensure that we don't busy wait.
      gevent.sleep(1)
      
      logging.info('Waiting for next AMQP event...')
      
      # Consume the next event.
      method_frame, header_frame, body = (
        self.amqp_channel.basic_get(self.queue_name))
      
      # Mask on an empty queue.
      if method_frame:
         # Parse the incoming message.
         kind, task_id = body.split(':', 1)
         
         with Interruptable("Task available") as task_available:
            
            # Ensure that this task has not already finished.
            if not self.zookeeper.exists('/done/{0}'.format(task_id),
                     partial(gevent.spawn, task_available.interrupt)):
               
               # Log the start of execution.
               logging.info('Processing task_id={0!r}, kind={1!r}'.
                 format(task_id, kind))
               
               with Interruptable("Still working") as still_working:
                  
                  # Launch the correct type of worker.
                  if kind == 'engine':
                     self._has_engine_task_to_perform(
                       task_id)
                  
                  elif kind == 'controller':
                     self._has_controller_task_to_perform(
                       task_id, still_working.interrupt)
               
               # Log completion.
               logging.info('Completed task_id={0!r}'.format(task_id))
               
               # Mark this task as having finished. This write is where atomic
               # updates should occur.
               self.zookeeper.create('/done/{0}'.format(task_id),
                 makepath = True)
         
         # ACK the AMQP task. Since we're trusting ZooKeeper for once-
         # only delivery, we can afford to let this fall off the stack
         # occasionally.
         self.amqp_channel.basic_ack(method_frame.delivery_tag)
   
   @forever
   def _has_engine_task_to_perform(self, task_id):
      """
      This function manages the connection to ZooKeeper.
      """
      
      # In this function, we have successfully conencted to ZooKeeper. Should
      # that connection break, the ZooKeeperAgent superclass will inform us by
      # raising a +DisconnectedFromZooKeeper+ exception.
      
      with Interruptable("Controller is ready") as has_controller:
         try:
            # Retrieve the current controller from ZooKeeper, and trigger an
            # interrupt when the current URL changes.
            
            controller_url = self.zookeeper.get('/{0}'.format(task_id),
              partial(gevent.spawn, has_controller.interrupt))
            
         except NoNodeError:
            has_controller.interrupt()
         
         self._has_controller(controller_url)
           # Trigger the next level of processing.
      
      with Interruptable("No controller ready") as no_controller:
         
         def inner(exists):
            if exists:
               no_controller.interrupt()
         
         # Ensure that the given task exists before continuing.
         if not self.zookeeper.exists('/{0}'.format(task_id),
                  partial(gevent.spawn, inner)):
            
            wait_forever()
   
   @forever
   def _has_controller(self, controller_url):
      """
      This function ensures that the engine remains running while the
      controller is active.
      """
      
      gevent.sleep(1)
        # Rate limit launches to the controller.
      
      engine = subprocess.Popen(
        ["ipengine", "--url={0}".format(controller_url)])
      
      try:
         engine.wait()
           # TODO(fatlotus): improve error reporting here :S
      finally:
         engine.terminate()
   
   @forever
   def _has_controller_task_to_perform(self, task_id, finish_job):
      """
      This function manages the IPython controller subprocess.
      """
      
      logging.info("Connected to ZooKeeper!")
      
      # Start the local IPython controller.
      ip_address = _lookup_ip_address()
      command = [ "ipcontroller", "--ip={0}".format(ip_address)]
      controller_job = subprocess.Popen(command, stderr=subprocess.PIPE)
      
      try:
         # Keep reading until there's output suggesting that we are
         # available for connections.
         while True:
            line = controller_job.stderr.readline()
            match = re.search(r'hub listening on ([^ ]+)', line,
                               flags=re.IGNORECASE)
            if match:
               controller_url = match.group(1)
               break
         
         # Notify ZooKeeper that we've started.
         try:
            self.zookeeper.create('/{0}'.format(task_id), ephemeral = True,
               value = controller_url)
         except NodeExistsError:
            raise TaskComplete
              # Generally this is a sign of a bad task -- but dragons remain.
         
         try:
            # Trigger main IPython job.
            main_job = subprocess.Popen([ "python", "program.py" ])
            main_job.wait()
            
            try:
               controller_job.terminate()
            except OSError:
               pass
            controller_job.wait()
            
            # Mark this controller as complete, which will deallocate the task
            # and expire the workers.
            finish_job()
         finally:
            try:
               self.zookeeper.delete('/{0}'.format(task_id))
            except KazooException:
               pass
         
      finally:
         # Ensure that we don't run a subprocess without ZooKeeper's
         # permission.
         try:
            controller_job.terminate()
         except OSError:
            pass

def main():
   parser = argparse.ArgumentParser(description='Run an LSDA worker node.')
   parser.add_argument('--zookeeper', action = 'append', required=True)
   parser.add_argument('--amqp', required=True)
   
   options = parser.parse_args()
   
   zookeeper = KazooClient(
     hosts = ','.join(options.zookeeper),
     handler = SequentialGeventHandler()
   )
   zookeeper.start_async()
   
   parameters = pika.ConnectionParameters(options.amqp)
   
   connection = pika.BlockingConnection(parameters)
   channel = connection.channel()
   
   channel.queue_declare('lsda_tasks', durable=True)
   
   EngineOrControllerRunner(zookeeper, channel, 'lsda_tasks').join()

if __name__ == "__main__":
   sys.exit(main())