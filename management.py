#!/usr/bin/env python
#
# LSDA Management Daemon.

"""
Running on every worker node, this script manages a persistent connection to
AMQP and ZooKeeper for the duration of each job.

Author: Jeremy Archer <jarcher@uchicago.edu>
Date: 10 December 2013
"""

# Import the Python green threading library.
from gevent import monkey
monkey.patch_all()
from gevent.coros import Semaphore
from gevent.pywsgi import WSGIServer
from gevent import subprocess
import gevent

# Import the pure-Python ZooKeeper implementation.
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.handlers.gevent import SequentialGeventHandler

# Import the pure-Python AMQP client libraries.
import pika

# Allow putting data to S3.
import boto.s3
import boto.ec2

# Allow adjusting the size of the worker pool.
from boto.ec2.autoscale import AutoScaleConnection

# Track subsystem statistics in ZooKeeper.
from linux_metrics import mem_stat, disk_stat, cpu_stat, net_stat

# Finally, import the stdlib.
import socket
import argparse
import sys
import logging
import time
import uuid
import json
import random
import tempfile
import os
import urllib
import zipfile
from functools import wraps, partial
from collections import namedtuple
import os.path


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
        """
        A wrapper for +func+.
        """
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

    def __init__(self, description=None, owner=None):
        self.exception = DeepBreakException()
        self.active = True
        self.thread = gevent.getcurrent()
        self.description = description
        self.owner = owner

    def __enter__(self):
        self.active = True
        if self.owner:
            self.owner.enter_state(self)
        return self

    def __exit__(self, kind, instance, traceback):
        self.active = False
        if self.owner:
            self.owner.exit_state(self)
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


def _get_ip_address():
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


def _get_public_ip_address():
    """
    Determines the publicly-routable instance IP address using Amazon magics.
    """

    return urllib.urlopen("http://169.254.169.254/latest/"
                          "meta-data/public-ipv4").read()


def _watchdog_timer(delay_in_seconds = 30 * 60):
    """
    Waits the given amount of time before shutting down this instance.
    """

    # Wait a random amount of time.
    seconds = delay_in_seconds + random.uniform(-300, 300)
    logging.info("About to wait for {}s before shutting down.".format(seconds))
    gevent.sleep(seconds)

    # Remove this node from the rotation.
    _remove_from_worker_pool()
    _shutdown_instance()

def _remove_from_worker_pool():
    """
    Ensures that this instance is shut down, and unregisted from the worker
    pool.
    """

    # Retrieve the current state of the pool.
    pool = AutoScaleConnection().get_all_groups(["LSDA Worker Pool"])[0]

    if pool.desired_capacity <= pool.min_size:
        return

    # Reduce the pool size and shut ourself down.
    pool.desired_capacity -= 1
    pool.update()

def _shutdown_instance():
    """
    Shuts down this instance and removes it from the worker pool.
    """

    # Retrieve the current instance ID.
    instance_id = urllib.urlopen("http://169.254.169.254/latest/"
                                 "meta-data/instance-id").read()

    conn = boto.ec2.connect_to_region("us-east-1")

    reservation = conn.get_all_instances([instance_id])[0]
    reservation.stop_all()


Task = namedtuple('Task', ['kind', 'from_user', 'task_id', 'sha1', 'file_name',
                           'owner'])


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
        self.owner = None
        self.sha1 = None
        self.task_type = None
        self.my_uuid = str(uuid.uuid4())
        self.ip_address = _get_public_ip_address()
        self.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)-8s] %(message)s"
        ))
        self.semaphore = Semaphore()

        # Initialize the AMQP exchange.
        self.amqp_channel.exchange_declare(
            exchange=exchange_name,
            type="topic")

    def emit_amqp(self, message):
        """
        Emit a message to this stream's STDERR.
        """

        # Ensure that we don't send crazy packets to AMQP.
        with self.semaphore:

            # Publish this message over AMQP.
            self.amqp_channel.basic_publish(
                exchange=self.exchange,
                routing_key="stderr.{0}".format(self.task_id),

                # Pack the current worker and task IDs into the message.
                body=json.dumps(message)
            )

    def emit_close(self):
        """
        Closes the other end of this pipe.
        """

        self.emit_amqp(dict(
            worker_uuid=self.my_uuid,
            task_id=self.task_id,
            owner=self.owner,
            sha1=self.sha1,
            task_type=self.task_type,
            ip_address=self.ip_address,
            type='close'
        ))

    def emit_unformatted(self, message, level=None):
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
        if self.disable:
            return
        self.disable = True

        try:
            self.emit_amqp(dict(
                type='data',
                worker_uuid=self.my_uuid,
                task_id=self.task_id,
                owner=self.owner,
                sha1=self.sha1,
                task_type=self.task_type,
                ip_address=self.ip_address,
                message=message,
                level=level,
            ))

        except pika.exceptions.ChannelClosed:
            print "Channel closed!"

        self.disable = False

    def emit(self, record):
        """
        Emits a logging message to AMQP.
        """

        return self.emit_unformatted(self.format(record), level=record.levelno)


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
        self.state_values = dict()
        self.agent_identifier = "/nodes/{}".format(_get_public_ip_address())

        self["state_stack"] = []

        self.thread = gevent.spawn(self._on_currently_running)
        self.update_thread = gevent.spawn(self._state_updater)

        self.web_server = WSGIServer(('', 1337), self._state_server)
        self.web_server_thread = gevent.spawn(self.web_server.serve_forever)

        self.metrics_threads = []
        self.metrics = {
            "mem_usage": (mem_stat.mem_stats,),
            "cpu_usage": (cpu_stat.cpu_percents, 1),
            "disk_throughput": (disk_stat.disk_reads_writes_persec, "xvdb", 1),
            "spindles": (disk_stat.disk_busy, "xvdb", 5),
            "disk_usage": (disk_stat.disk_usage, "/mnt"),
            "net_throughput": (self._netstat, "eth0")
        }

        self.update_metric_collection()

    def update_state(self):
        """
        Tells ZooKeeper of our current state, if we are connected.
        """

        if self.zookeeper.state == 'CONNECTED':
            value = json.dumps(self.state_values)

            # Create the given nameserver record, if possible.
            try:
                self.zookeeper.set(self.agent_identifier, value)
            except NoNodeError:
                try:
                    self.zookeeper.create(self.agent_identifier, value,
                                          makepath=True, ephemeral=True)
                except NodeExistsError:
                    pass

    def __delitem__(self, name):
        """
        Deletes the given state variable from this agent.
        """

        del self.state_values[name]

    def __setitem__(self, name, value):
        """
        Stores a globally-visible state variable.
        """

        self.state_values[name] = value

    def __getitem__(self, name):
        """
        Retrieves the value of the given state variable.
        """

        return self.state_values[name]

    def enter_state(self, state):
        """
        Tracks when this Agent enters a given state.
        """

        logging.info("Enter state={!r}".format(state.description))

        # Send an update to ZooKeeper.
        self["state_stack"].append(state.description)

    def exit_state(self, state):
        """
        Tracks when this Agent leaves a given state.
        """

        logging.info("Exit state={!r}".format(state.description))

        # Send an update to ZooKeeper.
        self["state_stack"].pop()

    def _state_server(self, environ, start_response):
        """
        A small WSGI server to stream data from this server.
        """

        # Prepare a HTTP response.
        start_response("200 Okay", [("Content-type", "text/json")])

        if environ['PATH_INFO'] == '/stream':

            # Continuously stream data out of this server.
            while True:
                yield json.dumps(self.state_values)
                gevent.sleep(2)

        elif environ['PATH_INFO'] == '/state':

            # Allow one-shot metrics collection.
            yield json.dumps(self.state_values)

        elif (environ['PATH_INFO'] == '/exit' and
              environ['REQUEST_METHOD'] == 'POST'):

            # Kills this instance.
            _shutdown_instance()
            yield json.dumps(dict(status="okay"))

    @forever
    def _state_updater(self):
        """
        Triggers a 0.5 Hz flush to ZooKeeper to track statistics about this
        worker."""

        self.update_state()
        gevent.sleep(2)

    def _netstat(self, interface):
        """
        Calculate the latest network status information.
        """

        irx, itx = net_stat.rx_tx_bytes(interface)
        gevent.sleep(1)
        frx, ftx = net_stat.rx_tx_bytes(interface)

        return dict(transmitted=(ftx - itx), received=(frx - irx))

    @forever
    def _metrics_thread(self, name, metric):
        """
        Update the server with the current value of the given metric.
        """

        function = metric[0]
        args = metric[1:]

        self[name] = function(*args)

        gevent.sleep(1)

    def update_metric_collection(self):
        """
        Update the background metrics threads given the values in self.metrics.
        """

        for thread in self.metrics_threads:
            thread.kill()

        for name, metric in self.metrics.items():
            self.metrics_threads.append(gevent.spawn(self._metrics_thread,
                                                     name, metric))

    @forever
    def _on_currently_running(self):
        """
        The first level of processing. It simply ensures that we remain
        connected to ZooKeeper. When active, it calls
        +_on_connected_to_zookeeper()+, a method meant to be overriden.
        """

        def trigger_on_states(action, states):
            """
            Helper function to trigger the given +action+ when the connection is
            in one of +states+."""

            def inner(state):
                """
                Handler function for the state change listener.
                """

                if state in states:
                    gevent.spawn(action)
                    self.zookeeper.remove_listener(inner)

            # Actually hook into the ZooKeeper connection.
            self.zookeeper.add_listener(inner)

        # Skip ahead if we're already connected.
        if self.zookeeper.state != 'CONNECTED':

            with Interruptable("Disconnected from ZooKeeper") as disconnected:
                trigger_on_states(disconnected.interrupt, ('CONNECTED'))
                    # Wake up when we are connected.

                wait_forever()
                    # Block until this occurs.

        # Mark ourselves as present in ZooKeeper.
        self.update_state()

        with Interruptable("Connected to ZooKeeper", self) as connected:
            trigger_on_states(connected.interrupt, ('SUSPENDED', 'LOST'))
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
        try:
            self.thread.get()
        finally:
            for thread in self.metrics_threads:
                thread.kill()
                thread.get()


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
        self.watchdog = None

        # Broadcast the current Git revision and AMQP channel.
        self["release"] = subprocess.check_output(
            ["/usr/bin/env", "git", "rev-parse", "HEAD"],
            env={"GIT_DIR": os.path.join(os.path.dirname(__file__), ".git")})
        self["queue_name"] = queue_name

    @forever
    def _on_connected_to_zookeeper(self):
        """
        This function manages the daemon to pull tasks from AMQP.
        """

        # Ensure that the node eventually shuts down.
        self.watchdog = gevent.spawn(_watchdog_timer)

        while True:
            # Rate-limit polling from AMQP.
            gevent.sleep(1)

            # Consume the next event.
            with self.logs_handler.semaphore:
                method_frame, _, body = (
                    self.amqp_channel.basic_get(self.queue_name))

            # Break out of this loop if not an event.
            if method_frame:
                break

        # Parse the incoming message.
        data = json.loads(body)

        # Keep this node alive for another 30m.
        if self.watchdog:
            self.watchdog.kill()
            self.watchdog = None

        # Ensure that all fields are defined.
        for key in data.keys():
            if key not in Task._fields:
                del data[key]

        for key in Task._fields:
            if key not in data:
                data[key] = None

        logging.warn("Data is {!r}".format(data))

        # Process the task.
        self._has_task_available(Task(**data))

        # ACK the AMQP task, if it is marked as having completed.
        if self.zookeeper.exists('/done/{0}'.format(data["task_id"])):
            with self.logs_handler.semaphore:
                self.amqp_channel.basic_ack(method_frame.delivery_tag)

    def _has_task_available(self, task):
        """
        Processes a task from AMQP.
        """

        with Interruptable("AMQP Task available", self) as task_available:

            # Ensure that this task has not already finished.
            if not self.zookeeper.exists(
               '/done/{0}'.format(task.task_id),
               partial(gevent.spawn, task_available.interrupt
            )):

                # Log the start of execution.
                logging.info('Got task={!r}'.format(task))

                # Associate logs with this task.
                self.logs_handler.task_id = task.task_id
                self.logs_handler.sha1 = task.sha1
                self.logs_handler.owner = task.owner
                self.logs_handler.task_type = task.kind

                # Report the current state in ZooKeeper.
                self["task"] = task._asdict()
                self["flag"] = ""

                try:
                    with Interruptable("Processing task", self) as working:

                        # Launch the correct type of worker.
                        if task.kind == 'engine':
                            self._has_engine_task_to_perform(task)

                        elif task.kind == 'controller':
                            result = self._has_controller_task_to_perform(task)

                            # Tell ZooKeeper that we have finished.
                            try:
                                self.zookeeper.create(
                                   '/done/{0}'.format(task.task_id),
                                   result, makepath=True)

                            except NodeExistsError:
                                pass

                        else:
                            logging.warn(
                                "Received task of unknown type {0!r}"
                                .format(task.kind)
                            )

                except Exception:
                    logging.exception(
                        'Unhandled exception in daemon')

                finally:
                    # Emitting a closing handler.
                    if task.kind == 'controller':
                        self.logs_handler.emit_close()

                    # Clean up logging.
                    self.logs_handler.task_id = None
                    self.logs_handler.owner = None
                    self.logs_handler.task_type = None
                    self.logs_handler.branch_name = None

                    # Clean up ZooKeeper state.
                    del self["task"]
                    del self["flag"]

                    self.update_state()

                # Log completion.
                logging.info('Completed task={0!r}'.format(task))

            else:
                logging.warn("Removed task {0!r} that has already been run."
                             .format(task.task_id))

    def _has_engine_task_to_perform(self, task):
        """
        This function waits until a controller is ready to handle this engine
        task.
        """

        # Wait until the controller is ready.
        for i in xrange(15):
            try:
                controller_info = self.zookeeper.get(
                  '/controller/{}'.format(task.task_id))[0]

            except NoNodeError:
                time.sleep(1)

            else:
                break

        else: # reschedule after 15s
            return

        self._has_controller(task, controller_info)

    def _has_controller(self, task, controller_info):
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
        with open(self.__class__.STUPID_JSON, 'w+') as output_file:
            output_file.write(controller_info)

        # Run the two engines in separate sandboxes.
        engines = []

        for i in xrange(2):
            engines.append(gevent.spawn(self._run_in_sandbox, task,
              ["ipengine"]))

        # Wait for completion.
        for engine in engines:
            engine.get()

    def _has_controller_task_to_perform(self, task):
        """
        This function manages the IPython controller subprocess.
        """

        # Rate limit controller launches to avoid hammering Git.
        gevent.sleep(1)

        # Start the local IPython controller.
        command = ["/usr/bin/env", "ipcontroller", "--init", "--ip=*"]

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

            # Start a local IPython engine.
            local_engine = gevent.spawn(self._has_controller,
                                        task, controller_info)

            # Notify ZooKeeper that we've started.
            try:
                self.zookeeper.create(
                    '/controller/{0}'.format(task.task_id),
                    ephemeral=True,
                    value=controller_info,
                    makepath=True
                )

            except NodeExistsError:
                self.zookeeper.set(
                    '/controller/{0}'.format(task.task_id),
                    ephemeral=True,
                    value=controller_info
                )

                logging.warn(
                    'Potential race condition in task_id or done/task_id.')

            @gevent.spawn
            def copy_output_from_controller():
                """
                Copies output from ipcontroller to AMQP.
                """

                while True:
                    line = controller_job.stderr.readline()
                    if line == '':
                        break
                    self.logs_handler.emit_unformatted("ipcontroller says {:}"
                                                       .format(line.strip()))

            try:
                # Run the main script in the sandbox.
                return self._run_in_sandbox(task, ["main"])
                
            finally:
                # Delete the controller job.
                try:
                    self.zookeeper.delete('/controller/{0}'.format(
                      task.task_id))
                except NoNodeError:
                    pass

                # Ensure that the output copier is shut down properly.
                copy_output_from_controller.kill()

        finally:
            # Ensure that we don't run a subprocess without ZooKeeper's
            # permission.
            try:
                controller_job.terminate()
                controller_job.wait()
            except OSError:
                pass

            # Kill the local instance.
            local_engine.kill()

    def _notebook_copier(self, code_directory, task_id):
        """
        Copies main.ipynb to the S3 bucket for the given +task_id+.
        """

        previous_time = 0
        path = os.path.join(code_directory, "__saved.ipynb")

        while True:

            # Ensure that we've actually changed since the last time.
            if os.path.exists(path) and os.path.getmtime(path) > previous_time:

                # Log the current state.
                logging.info("Pushing notebook file to S3.")

                # Upload the ipynb file to S3.
                try:
                    connection = boto.connect_s3()
                    bucket = connection.get_bucket('ml-submissions')
                    key = bucket.new_key('results/' + task_id + '.ipynb')

                    # Upload the resulting notebook.
                    key.set_contents_from_filename(path)

                except Exception:
                    logging.exception("Unable to upload notebook.")

                else:
                    previous_time = os.path.getmtime(path)


            gevent.sleep(3)

    def _stderr_copier(self, main_job, task_id):
        """
        Copies data from stdout/stderr to the main logging output, pushing
        some data values to S3.
        """

        while True:
            line = main_job.stdout.readline()

            # Allow reporting of "flag" values for running jobs.
            if line.startswith("REPORTING_SEMAPHORE "):
                self["flag"] = line.split(" ", 1)[1]

            # Quietly exit on EOF.
            elif not line:
                return

            # Otherwise log to RabbitMQ.
            else:
                self.logs_handler.emit_unformatted(line[:-1])
    
    def _run_in_sandbox(self, task, command):
        """
        Runs the given type of process inside a project sandbox.
        """

        # Ensure that we can write to /mnt (this is a band-aid).
        with Interruptable("Checking for /mnt", self):
            if not os.path.ismount("/mnt"):
                subprocess.check_call(["/usr/bin/sudo", "/bin/mount", "/mnt"])

            subprocess.check_call(
                ["/usr/bin/sudo", "/bin/chown", "lsda", "/mnt"])

        # Create a working directory for this project.
        code_directory = tempfile.mkdtemp(dir="/mnt")
        os.chmod(code_directory, 0o755)

        # Collect some per-task statistics.
        quota_used = self.zookeeper.Counter(
            '/quota_used/compute_time/{0}'.format(task.owner),
            default=0.0)
        quota_limit = self.zookeeper.Counter(
            '/quota_limit/compute_time/{0}'.format(task.owner),
            default=0.0)

        # Retrieve the zip archive from S3 for this commit.
        with tempfile.TemporaryFile() as fp:

            # Connect to S3.
            bucket = boto.connect_s3().get_bucket("ml-checkpoints")

            # Download the given git tree.
            for i in xrange(30):
                key = bucket.get_key("submissions/{}/{}.zip".format(
                    task.from_user, task.sha1))
                if key is not None:
                    break
            else:
                raise ValueError("Unable to find submission ZIP in 30s.")

            key.get_contents_to_file(fp)

            # Extract the Zip archive to our working directory.
            zipfile.ZipFile(fp).extractall(code_directory)

        try:
            # Trigger main IPython job.
            main_job = subprocess.Popen(
                ["/usr/bin/env", "sudo", "/worker/sandbox.py"] + command +
                [task.task_id, task.owner, task.file_name],

                cwd=code_directory,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )

            if command[0] == "main":
                copy_notebook_to_s3 = gevent.spawn(
                   self._notebook_copier,
                   code_directory,
                   task.task_id
                )
            else:
                copy_notebook_to_s3 = None

            stderr_copier = gevent.spawn(self._stderr_copier, main_job,
                                         task.task_id)

            @gevent.spawn
            def drain_quarters():
                """
                Count total per-user execution time.
                """

                # Continue taking time in one-minute increments.
                while quota_used.value < quota_limit.value:
                    quota_used.__add__(60.0)
                    gevent.sleep(60)

                # Kill the job when we're out.
                logging.error('Job killed -- out of time: used {} of {}'.format(
                    quota_used.value, quota_limit.value))
                main_job.kill()
                stderr_copier.kill()

            # Actually wait for completion.
            stderr_copier.join()
            status = main_job.wait()

            return "exit {}".format(status)
            
        finally:
            # Clean up main job.
            try:
                if main_job:
                    main_job.kill()
            except OSError:
                pass

            # Clean up helpers.
            if copy_notebook_to_s3:
                copy_notebook_to_s3.kill()

            if drain_quarters:
                drain_quarters.kill()

            if stderr_copier:
                stderr_copier.kill()


def main():
    """
    Main entry point for this persistent daemon.
    """

    # Set up the argument parser.
    parser = argparse.ArgumentParser(description='Run an LSDA worker node.')
    parser.add_argument('--zookeeper', action='append', required=True)
    parser.add_argument('--amqp', required=True)
    parser.add_argument('--queue', default='stable')

    options = parser.parse_args()

    # Connect to ZooKeeper.
    zookeeper = KazooClient(
        hosts=','.join(options.zookeeper),
        handler=SequentialGeventHandler()
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

    # Disable extraneous packet logs from Kazoo.
    import kazoo.client
    kazoo.client.log.setLevel(logging.WARN)

    # Ensure that the queue we will pull from exists.
    jobs_channel.queue_declare(options.queue, durable=True)

    # Begin processing requests.
    try:
        EngineOrControllerRunner(zookeeper, jobs_channel,
                                 options.queue, handler).join()
    except Exception:
        logging.exception("Unhandled exception at root level.")
        raise

if __name__ == "__main__":
    sys.exit(main())
