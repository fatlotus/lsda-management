import pika
import uuid
import json
import sys
import logging

# Set up the connection to AMQP.
connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
channel = connection.channel()

channel.queue_declare("lsda_tasks", durable=True)

# Create the existing task to submit.
task_id = uuid.uuid4()
branch_sha1 = sys.argv[2] # The SHA-1 of the branch being pushed.
jobs = ['controller', 'engine']

# Publish each job in this task.
for job in jobs:
   channel.basic_publish(
      exchange = '',
      routing_key = 'lsda_tasks',
      body = '{0}:{1}:{2}'.format(job, task_id, branch_sha1)
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
      sys.exit(0)
   elif unpacked['level'] >= logging.WARN:
      print unpacked['message']

# Process output from the invocation of the script.
channel.basic_consume(output, queue=queue_name, no_ack=True)
channel.start_consuming()