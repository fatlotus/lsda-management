#!/usr/bin/env python
#
# Author: Jeremy Archer <jarcher@uchicago.edu>
# Date: 14 December 2013
#

import pika, uuid, subprocess, json, sys, logging, base64

# Kinda janky, but it allows us to break out of the existing event loop.
class DoneException(Exception):
   pass

# Set up the connection to AMQP.
connection = pika.BlockingConnection(pika.ConnectionParameters(
  'amqp.lsda.cs.uchicago.edu'))
channel = connection.channel()

channel.queue_declare("lsda_tasks", durable=True)

# Fetch all branches pushed
branches_to_run = sys.argv[1:]
commits_to_run = [ ]

# Ensure that only submission branches are pushed.
for branch in branches_to_run:
   if branch.startswith('refs/heads/submissions/'):
      commits_to_run.append(branch[11:])

# Submit each commit serially.
for commit in commits_to_run:
   
   # Create the existing task to submit.
   task_id = uuid.uuid4()
   jobs = ['controller', 'engine']

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
      
      if unpacked['type'] == 'close':
         raise DoneException
      elif unpacked['level'] >= logging.WARN:
         print unpacked['message']
   
   # Process output from the invocation of the script.
   channel.basic_consume(output, queue=queue_name, no_ack=True)
   
   try:
      channel.start_consuming()
   except DoneException:
      channel.stop_consuming()
