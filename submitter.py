#!/usr/bin/env python
#
# Author: Jeremy Archer <jarcher@uchicago.edu>
# Date: 14 December 2013
#

import pika, uuid, subprocess, json, sys, logging, base64, prctl, signal
import atexit, smtplib, boto.ses

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

# Kinda janky, but it allows us to break out of the existing event loop.
class DoneException(Exception):
   pass

done_yet = False

# Ensure that we die with the SSH parent.
prctl.set_pdeathsig(signal.SIGINT)

# Ensure that these branches can be fetched over HTTP.
subprocess.check_call(['git', 'update-server-info'])

sys.exit(0)

# Set up the connection to AMQP.
connection = pika.BlockingConnection(pika.ConnectionParameters(
  'amqp.lsda.cs.uchicago.edu'))
channel = connection.channel()

channel.queue_declare("lsda_tasks", durable=True)
channel.exchange_declare("lsda_logs", type='topic')

# Fetch all branches pushed
branches_to_run = sys.argv[1:]
commits_to_run = [ ]

# Ensure that only submission branches are pushed.
for branch in branches_to_run:
   if branch.startswith('refs/heads/submissions/'):
      commits_to_run.append(branch[11:])

# Track completed branches.
completed = set()

def handle_end_of_submission(task_id):
   """
   Inform the user on unexpected connection loss about their job.
   """
   if task_id in completed:
      return
   
   conn = boto.ses.connect_to_region("us-east-1")
   conn.send_email(
      "\"Bob the Build Bot\" <jarcher@uchicago.edu>",
      "AUTO: Connection Lost",
      """\
Hello there!

It appears that a recent run of yours died unexpectedly. If you want to
keep watching the output, visit the link below. Note that if you make another
submission this one will be terminated.

http://nbviewer.ipython.org/url/ml-submissions.s3-website-us-east-1.amazonaws.com/results/{task_id}.ipynb

If anything seems fishy, please don't hesitate to contact me.

All the best,
-J
""".format(**locals()),
      ["jarcher@uchicago.edu"]
   )

# Submit each commit serially.
for commit in commits_to_run:
   
   # Create the existing task to submit.
   task_id = uuid.uuid4()
   jobs = ['controller', 'engine', 'engine', 'engine']
   
   # Notify the user if the connection breaks early.
   atexit.register(handle_end_of_submission, task_id)
   
   # Publish each job in this task.
   for job in jobs:
      channel.basic_publish(
         exchange = '',
         routing_key = 'lsda_tasks',
         body = '{0}:{1}:{2}'.format(job, task_id, base64.b64encode(commit))
      )

   # Prepare a STDERR pipe to respond to the queue.
   queue_name = channel.queue_declare(exclusive=True).method.queue

   channel.queue_bind(
      exchange = 'lsda_logs',
      queue = queue_name,
      routing_key = 'stderr.{0}'.format(task_id)
   )
   
   # Handle the resulting output.
   def output(channel, method, properties, body):
      unpacked = json.loads(body)
      
      # Clear the progress message posted earlier.
      sys.stderr.write("                           \r")
      sys.stderr.flush()
      
      if unpacked['type'] == 'close':
         raise DoneException
      elif unpacked['level'] >= logging.WARN:
         sys.stderr.write("{ip_address:<15} {message}\n".format(**unpacked))
         sys.stderr.flush()
   
   # Inform the user that execution has not yet started.
   sys.stderr.write("Waiting for an instance...\r")
   sys.stderr.flush()
   
   # Process output from the invocation of the script.
   channel.basic_consume(output, queue=queue_name, no_ack=True)
   
   try:
      channel.start_consuming()
   except DoneException:
      channel.stop_consuming()
   
   # Track this commit as completed.
   completed.add(task_id)