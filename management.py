#!/usr/bin/env python
#
# Author: Jeremy Archer <jarcher@uchicago.edu>
# Date: 10 December 2013
# 


# Import the Python green threading library.
from gevent import monkey; monkey.patch_all()
from gevent.event import Event
from gevent.coros import Semaphore
from gevent import subprocess
import greenlet, gevent

# Import the pure-Python ZooKeeper implementation.
from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.handlers.gevent import SequentialGeventHandler

# Import the pure-Python AMQP client libraries.
import pika

# Finally, import the stdlib.
import re, socket, argparse, sys, logging, time, uuid, json, shutil, tempfile
import base64, os
from functools import wraps, partial

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
   """
   An exception to allows breaking out of multiply-nested code.
   """

class Interruptable(object):
   """
   This context manager allows a user to break out of code by calling the given
   function.
   """
   
   def __init__(self, description=None):
      self.exception = DeepBreakException()
      self.active = True
      self.thread = gevent.getcurrent()
      self.description = description
   
   def __enter__(self):
      self.active = True
      logging.info("Enter state={0!r}".format(self.description))
      return self
   
   def __exit__(self, kind, instance, traceback):
      self.active = False
      logging.info("Exit state={0!r}".format(self.description))
      return instance is self.exception
   
   def interrupt(self, *vargs, **dargs):
      """
      Jumps to the end of this block.
      """
      
      if self.active:
         self.thread.kill(self.exception)

def wait_forever():
   """
   Waits indefinitely.
   """
   
   while True:
      gevent.sleep(60)

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

class AMQPLoggingHandler(logging.Handler):
   """
   A Python logging handler to send logging results to AMQP for distribution
   and archival.
   """
   
   def __init__(self, amqp_channel, exchange_name):
      super(AMQPLoggingHandler, self).__init__()
      
      # Set up local state.
      self.amqp_channel = amqp_channel
      self.exchange = exchange_name
      self.disable = False
      self.task_id = None
      self.my_uuid = str(uuid.uuid4())
      self.setFormatter(logging.Formatter(
         "%(asctime)s [%(levelname)-8s] %(message)s"
      ))
      self.semaphore = Semaphore()
      
      # Initialize the AMQP exchange.
      self.amqp_channel.exchange_declare(exchange=exchange_name, type="topic")
   
   def emit_amqp(self, message):
      """
      Emit a message to this stream's STDERR.
      """
      
      # Ensure that we don't send crazy packets to AMQP.
      with self.semaphore:
         
         # Publish this message over AMQP.
         self.amqp_channel.basic_publish(
            exchange = self.exchange,
            routing_key = "stderr.{0}".format(self.task_id),
         
            # Pack the current worker and task IDs into the message.
            body = json.dumps(message)
         )
   
   def emit_close(self):
      """
      Closes the other end of this pipe.
      """
      
      self.emit_amqp(dict(type = 'close'))
   
   def emit_unformatted(self, message, level = None):
      """
      Emit an unformatted (string) logging message to AMQP.
      """
      
      # Bounce most logs to syslog.
      sys.stderr.write("{0}\n".format(message))
      sys.stderr.flush()
      
      # Ensure that generic messages are shown to the user.
      if level is None:
         level = logging.WARN
      
      # Ensure that we don't create log loops.
      if self.disable: return
      self.disable = True
      
      try:
         self.emit_amqp(dict(
            type = 'data',
            worker_uuid = self.my_uuid,
            task_id = self.task_id,
            message = message,
            level = level
         ))
         
      except pika.exceptions.ChannelClosed:
         print "Channel closed!"
      
      self.disable = False
   
   def emit(self, record):
      """
      Emits a logging message to AMQP.
      """
      
      return self.emit_unformatted(self.format(record), level = record.levelno)

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
      
      # This function triggers the given action when the connection is in the
      # specified states.
      def trigger_on_states(action, states):
         
         # Wait until the state changes.
         def inner(state):
            if state in states:
               gevent.spawn(action)
               self.zookeeper.remove_listener(inner)
         
         # Actually hook into the ZooKeeper connection.
         self.zookeeper.add_listener(inner)
      
      # Skip ahead if we're already connected.
      if self.zookeeper.state != 'CONNECTED':
         
         with Interruptable("Disconnected from ZooKeeper") as disconnected:
            trigger_on_states(disconnected.interrupt, ( 'CONNECTED' ))
               # Wake up when we are connected.
            
            wait_forever()
               # Block until this occurs.
      
      with Interruptable("Connected to ZooKeeper") as connected:
         trigger_on_states(connected.interrupt, ( 'SUSPENDED', 'LOST' ))
            # Wake up If we reach the given state.
         
         self._on_connected_to_zookeeper()
            # Process events until this occurs.
   
   def _on_connected_to_zookeeper(self):
      """
      Override this function to customize the behavior of this class.
      """
   
   def join(self):
      """
      Block until this daemon has completed.
      """
      
      # Wait until this thread completes or raise the exception it terminated
      # with.
      self.thread.get()

class EngineOrControllerRunner(ZooKeeperAgent):
   """
   This task initializes an agent that could run either an IPython
   controller or IPython engine task.
   """
   
   STUPID_JSON = (
     '/home/lsda/.ipython/profile_default/' +
     'security/ipcontroller-engine.json'
   )
   
   def __init__(self, zookeeper, amqp_channel, queue_name, logs_handler):
      # Initialize the connection to ZooKeeper.
      super(EngineOrControllerRunner, self).__init__(zookeeper)
      
      # Save local state.
      self.amqp_channel = amqp_channel
      self.queue_name = queue_name
      self.logs_handler = logs_handler
   
   @forever
   def _on_connected_to_zookeeper(self):
      """
      This function manages the daemon to pull tasks from AMQP.
      """
      
      # Ensure that we don't busy wait.
      gevent.sleep(1)
      
      # Consume the next event.      
      with self.logs_handler.semaphore:
         method_frame, header_frame, body = (
           self.amqp_channel.basic_get(self.queue_name))
      
      # Mask on an empty queue.
      if method_frame:
         # Parse the incoming message.
         kind, task_id, base64_commit = body.split(':', 2)
         
         # Decode the branch name to run.
         commit = base64.b64decode(base64_commit)
         
         with Interruptable("AMQP Task available") as task_available:
            
            # Ensure that this task has not already finished.
            if not self.zookeeper.exists('/done/{0}'.format(task_id),
                     partial(gevent.spawn, task_available.interrupt)):
               
               # Log the start of execution.
               logging.info('Processing task={0!r}, kind={1!r}, commit={2!r}'.
                 format(task_id, kind, commit))
               
               # Associate logs with this task.
               self.logs_handler.task_id = task_id
               
               try:
                  with Interruptable("Processing AMQP task") as still_working:
                     
                     try:
                        # Launch the correct type of worker.
                        if kind == 'engine':
                           self._has_engine_task_to_perform(task_id, commit)
                        
                        elif kind == 'controller':
                           self._has_controller_task_to_perform(
                             task_id, still_working.interrupt, commit)
                        
                        else:
                           logging.warn("Received task of unknown type {0!r}"
                                           .format(kind))
                     
                     except Exception:
                        logging.exception('Unhandled exception in daemon')
                  
               finally:
                  # Emitting a closing handler.
                  if kind == 'controller':
                     self.logs_handler.emit_close()

                  # Clean up logging.
                  self.logs_handler.task_id = None
               
               # Log completion.
               logging.info('Completed task_id={0!r}'.format(task_id))
               
               # Mark this task as having finished. This write is where atomic
               # updates should occur.
               self.zookeeper.create('/done/{0}'.format(task_id),
                 makepath = True)
            
            else:
               logging.warn("Removed task {0!r} that has already been run."
                              .format(task_id))
         
         # ACK the AMQP task. Since we're trusting ZooKeeper for once-
         # only delivery, we can afford to let this fall off the stack
         # occasionally.
         with self.logs_handler.semaphore:
            self.amqp_channel.basic_ack(method_frame.delivery_tag)
   
   @forever
   def _has_engine_task_to_perform(self, task_id, commit):
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
            
            controller_info = self.zookeeper.get('/{0}'.format(task_id),
              partial(gevent.spawn, has_controller.interrupt))[0]
            
         except NoNodeError:
            has_controller.interrupt()
              # If we couldn't find the controller, skip ahead.
         
         self._has_controller(controller_info, commit)
           # Trigger the next level of processing.
      
      with Interruptable("No controller ready") as no_controller:
         
         # Ensure that we only wake up when ready.
         def inner(exists):
            if exists:
               no_controller.interrupt()
         
         # Ensure that the given task exists before continuing.
         if not self.zookeeper.exists('/{0}'.format(task_id),
                  partial(gevent.spawn, inner)):
            
            wait_forever()
   
   @forever
   def _has_controller(self, controller_info, commit):
      """
      This function ensures that the engine remains running while the
      controller is active.
      """

      # Create the parent directory for the IPython configuration file.
      try:
         os.makedirs(os.path.dirname(self.__class__.STUPID_JSON))
      except OSError:
         pass
      
      # Save the stupid IPython profile configuration file.
      with open(self.__class__.STUPID_JSON, 'w+') as fp:
         fp.write(controller_info)
      
      # Run the main script in the sandbox.
      self._run_in_sandbox(commit, ["ipengine"])
   
   @forever
   def _has_controller_task_to_perform(self, task_id, finish_job, commit):
      """
      This function manages the IPython controller subprocess.
      """
      
      # Rate limit controller launches to avoid hammering Git.
      gevent.sleep(1)
      
      # Start the local IPython controller.
      command = ["/usr/local/bin/ipcontroller", "--init", "--ip=*"]
      
      # Record the arguments.
      logging.info("Starting ipcontroller cmd={0!r}".format(command))
      
      controller_job = subprocess.Popen(command, stderr=subprocess.PIPE)
      
      try:
         # Keep reading until there's output suggesting that we are
         # available for connections.
         while True:
            line = controller_job.stderr.readline()
            if 'scheduler started' in line.lower():
               controller_info = open(self.__class__.STUPID_JSON).read()
               break
         
         # Notify ZooKeeper that we've started.
         try:
            self.zookeeper.create('/{0}'.format(task_id), ephemeral = True,
               value = controller_info)
         except NodeExistsError:
            logging.warn('Potential race condition in task_id or done/task_id.')
            finish_job()
              # Generally this is a sign of a bad task -- but dragons remain.
         
         try:
            # Run the main script in the sandbox.
            self._run_in_sandbox(commit, ["main"])
            
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
            controller_job.wait()
         except OSError:
            pass
   
   def _run_in_sandbox(self, commit, command):
      """
      Runs the given type of process inside a project sandbox.
      """
      
      # Create a working directory for this project.
      code_directory = tempfile.mkdtemp()
      
      # Construct reference to the current code repository.
      git_url = (
         "http://gitolite-internal.lsda.cs.uchicago.edu:1337/" +
         "assignment-one.git"
      )
      
      # Checking out the proper source code.
      subprocess.call(["/usr/bin/git", "clone", "--quiet",
        git_url, code_directory])
      
      # Checking out the proper source code.
      subprocess.call(["/usr/bin/git", "checkout", commit],
        cwd = code_directory)
      
      # Trigger main IPython job.
      main_job = subprocess.Popen(
        ["/usr/bin/sudo", "/worker/sandbox.py"] + command,
        
        cwd = code_directory,
        stdout = subprocess.PIPE,
        stderr = subprocess.STDOUT
      )
      
      # Asynchronously log data from stdout/stderr.
      @gevent.spawn
      def task():
         while True:
            line = main_job.stdout.readline()
            if not line: return
            self.logs_handler.emit_unformatted(line[:-1])
      
      main_job.wait()
      task.join()

def main():
   # Configure logging
   # logging.basicConfig(
   #    format = "%(asctime)-15s [%(levelname)-8s] %(message)s",
   #    level=logging.INFO
   # )

   # Set up the argument parser.
   parser = argparse.ArgumentParser(description='Run an LSDA worker node.')
   parser.add_argument('--zookeeper', action = 'append', required=True)
   parser.add_argument('--amqp', required=True)
   
   options = parser.parse_args()
   
   # Connect to ZooKeeper.
   zookeeper = KazooClient(
     hosts = ','.join(options.zookeeper),
     handler = SequentialGeventHandler()
   )
   
   zookeeper.start_async()
   
   # Connect to AMQP.
   parameters = pika.ConnectionParameters(options.amqp)
   
   connection = pika.BlockingConnection(parameters)
   logging_channel = connection.channel()
   jobs_channel = connection.channel()
   
   # Configure logging.
   handler = AMQPLoggingHandler(logging_channel, 'lsda_logs')
   
   logging.getLogger().addHandler(handler)
   logging.getLogger().setLevel(logging.INFO)
   
   # Ensure that the queue we will pull from exists.
   jobs_channel.queue_declare('lsda_tasks', durable=True)
   
   # Begin processing requests.
   EngineOrControllerRunner(zookeeper, jobs_channel,
                            'lsda_tasks', handler).join()

if __name__ == "__main__":
   sys.exit(main())