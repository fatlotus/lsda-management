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
import tempfile
import os
import urllib
from functools import wraps, partial
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

        # Broadcast the current Git revision.
        self["release"] = subprocess.check_output(
            ["/usr/bin/env", "git", "rev-parse", "HEAD"],
            env={"GIT_DIR": os.path.join(os.path.dirname(__file__), ".git")})

    @forever
    def _on_connected_to_zookeeper(self):
        """
        This function manages the daemon to pull tasks from AMQP.
        """

        # Ensure that we don't busy wait.
        gevent.sleep(1)

        # Consume the next event.
        with self.logs_handler.semaphore:
            method_frame, _, body = (
                self.amqp_channel.basic_get(self.queue_name))

        # Mask an empty queue.
        if method_frame:
            # Parse the incoming message.
            try:
                kind, owner, task_id, sha1 = body.split(':', 3)
            except ValueError:
                kind, task_id, sha1 = body.split(':', 3)
                owner = "nobody"

            self._has_task_available(kind, owner, task_id, sha1)

            # ACK the AMQP task. Since we're trusting ZooKeeper for once-
            # only delivery, we can afford to let this fall off the stack
            # occasionally.
            with self.logs_handler.semaphore:
                self.amqp_channel.basic_ack(method_frame.delivery_tag)

    def _has_task_available(self, kind, owner, task_id, sha1):
        """
        Processes a task from AMQP.
        """

        with Interruptable("AMQP Task available", self) as task_available:

            # Ensure that this task has not already finished.
            if not self.zookeeper.exists(
               '/done/{0}'.format(task_id),
               partial(gevent.spawn, task_available.interrupt
            )):

                # Log the start of execution.
                logging.info(
                    'Got task={!r}, kind={!r}, owner={!r}, sha1={!r}'.
                    format(task_id, kind, owner, sha1))

                # Associate logs with this task.
                self.logs_handler.task_id = task_id
                self.logs_handler.sha1 = sha1
                self.logs_handler.owner = owner
                self.logs_handler.task_type = kind

                # Report the current state in ZooKeeper.
                self["task_id"] = task_id
                self["sha1"] = sha1
                self["owner"] = owner
                self["task_type"] = kind

                try:
                    with Interruptable("Processing task", self) as working:

                        try:
                            # Launch the correct type of worker.
                            if kind == 'engine':
                                self._has_engine_task_to_perform(
                                    task_id, owner, sha1)

                            elif kind == 'controller':
                                self._has_controller_task_to_perform(
                                    task_id, owner, sha1, working.interrupt)

                            else:
                                logging.warn(
                                    "Received task of unknown type {0!r}"
                                    .format(kind)
                                )

                        except Exception:
                            logging.exception(
                                'Unhandled exception in daemon')

                finally:
                    # Emitting a closing handler.
                    if kind == 'controller':
                        self.logs_handler.emit_close()

                    # Clean up logging.
                    self.logs_handler.task_id = None
                    self.logs_handler.owner = None
                    self.logs_handler.task_type = None
                    self.logs_handler.branch_name = None

                    # Clean up ZooKeeper state.
                    del self["task_id"]
                    del self["sha1"]
                    del self["owner"]
                    del self["task_type"]

                    self.update_state()

                # Log completion.
                logging.info('Completed task_id={0!r}'.format(task_id))

                # Mark this task as having finished. This write is where
                # atomic updates should occur.
                if kind == 'controller':
                    try:
                        self.zookeeper.create('/done/{0}'.format(task_id),
                                              "complete", makepath=True)
                    except NodeExistsError:
                        pass

            else:
                logging.warn("Removed task {0!r} that has already been run."
                             .format(task_id))

    @forever
    def _has_engine_task_to_perform(self, task_id, owner, sha1):
        """
        This function manages the connection to ZooKeeper.
        """

        # In this function, we have successfully conencted to ZooKeeper. Should
        # that connection break, the ZooKeeperAgent superclass will inform us by
        # raising a +DisconnectedFromZooKeeper+ exception.

        with Interruptable("Controller is ready", self) as has_controller:
            try:
                # Retrieve the current controller from ZooKeeper, and trigger an
                # interrupt when the current URL changes.

                controller_info = self.zookeeper.get(
                    '/controller/{}'.format(task_id),
                    partial(gevent.spawn, has_controller.interrupt)
                )[0]

            except NoNodeError:
                has_controller.interrupt()
                # If we couldn't find the controller, skip ahead.

            self._has_controller(task_id, controller_info, owner, sha1)
            # Trigger the next level of processing.

        with Interruptable("No controller ready", self) as no_controller:

            def inner(exists):
                """
                Interrupt the controller, if it is ready.
                """

                if exists:
                    no_controller.interrupt()

            # Ensure that the given task exists before continuing.
            if not self.zookeeper.exists('/controller/{0}'.format(task_id),
                                         partial(gevent.spawn, inner)):

                wait_forever()

    def _has_controller(self, task_id, controller_info, owner, sha1):
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

        # Run the main script in the sandbox.
        self._run_in_sandbox(task_id, owner, sha1, ["ipengine"])

    def _has_controller_task_to_perform(
            self, task_id, owner, sha1, finish_job):
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
                                        task_id, controller_info, owner, sha1)

            # Notify ZooKeeper that we've started.
            try:
                self.zookeeper.create(
                    '/controller/{0}'.format(task_id),
                    ephemeral=True,
                    value=controller_info,
                    makepath=True
                )

            except NodeExistsError:
                logging.warn(
                    'Potential race condition in task_id or done/task_id.')
                finish_job()
                # Generally this is a sign of a bad task -- but dragons remain.

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
                self._run_in_sandbox(task_id, owner, sha1, ["main"])

                # Mark this controller as complete, which will deallocate the
                # task and expire the workers.
                finish_job()
            finally:
                # Delete the controller job.
                try:
                    self.zookeeper.delete('/controller/{0}'.format(task_id))
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
        path = os.path.join(code_directory, "main.ipynb")

        while True:

            # Ensure that we've actually changed since the last time.
            if os.path.getmtime(path) > previous_time:

                # Log the current state.
                logging.info("Pushing notebook file to S3.")

                # Upload the ipynb file to S3.
                connection = boto.connect_s3()
                bucket = connection.get_bucket('ml-submissions')
                key = bucket.new_key('results/' + task_id + '.ipynb')

                previous_time = os.path.getmtime(path)

                # Upload the resulting notebook.
                key.set_contents_from_filename(path)

            gevent.sleep(3)

    def _run_in_sandbox(self, task_id, owner, sha1, command):
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
            '/quota_used/compute_time/{0}'.format(owner),
            default=0.0)
        quota_limit = self.zookeeper.Counter(
            '/quota_limit/compute_time/{0}'.format(owner),
            default=0.0)

        # Construct reference to the current code repository.
        git_url = (
            "http://gitolite-internal.lsda.cs.uchicago.edu:1337/" +
            "assignment-one.git"
        )

        # Checking out the proper source code.
        subprocess.call(["/usr/bin/env", "git", "clone", "--quiet",
                         git_url, code_directory])

        # Checking out the proper source code.
        subprocess.call(["/usr/bin/env", "git", "checkout", sha1],
                        cwd=code_directory)

        try:
            # Trigger main IPython job.
            main_job = subprocess.Popen(
                ["/usr/bin/env", "sudo", "/worker/sandbox.py"] + command +
                [task_id, owner],

                cwd=code_directory,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )

            copy_notebook_to_s3 = gevent.spawn(
               self._notebook_copier,
               code_directory,
               task_id
            )

            # Asynchronously log data from stdout/stderr.
            @gevent.spawn
            def stderr_copier():
                """
                Logs data from stdout/stderr to RabbitMQ.
                """

                while True:
                    line = main_job.stdout.readline()
                    if not line:
                        return
                    self.logs_handler.emit_unformatted(line[:-1])

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
            main_job.wait()

        finally:
            # Clean up main job.
            try:
                main_job.kill()
            except OSError:
                pass

            # Clean up helpers.
            copy_notebook_to_s3.kill()
            drain_quarters.kill()
            stderr_copier.kill()


def main():
    """
    Main entry point for this persistent daemon.
    """

    # Set up the argument parser.
    parser = argparse.ArgumentParser(description='Run an LSDA worker node.')
    parser.add_argument('--zookeeper', action='append', required=True)
    parser.add_argument('--amqp', required=True)

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
    jobs_channel.queue_declare('lsda_tasks', durable=True)

    # Begin processing requests.
    EngineOrControllerRunner(zookeeper, jobs_channel,
                             'lsda_tasks', handler).join()

if __name__ == "__main__":
    sys.exit(main())
