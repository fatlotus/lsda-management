#!/usr/bin/env python
#
# This script runs "main.py" in a chroot. It is meant to be run setuid root.
#
# Author: Jeremy Archer <jarcher@uchicago.edu>
# Date: 2 January 2013
#

import sys
import os
import pwd
import sys
import stat
import resource
import shutil
import subprocess
import tempfile
import json
import logging
import atexit
import traceback

# Bane of my existence... :(
MAGIC_JSON_FILES = '.ipython/profile_default/security'
HOME_DIR = '/home/lsda' # <- Don't put a slash at the end!
USELESS_README = (
  'usr/local/lib/python2.7/dist-packages/IPython/config/profile/README_STARTUP')

# Specifies what modules are allowed inside the sandbox. Unfortunately these
# modules will have root access to the computer.
ALLOWED_MODULES = [
   "os",
   "pwd",
   "sys",
   "IPython",
   "IPython.parallel",
   "IPython.parallel.apps.ipengineapp",
   "IPython.parallel.client.magics",
   "IPython.kernel.zmq.iostream",
   "IPython.kernel.inprocess.ipkernel",
   "IPython.core.completerlib",
   "IPython.utils.rlineimpl",
   "apport.fileutils",
   "_strptime",
   "xml.sax.expatreader",
   "zmq.utils.garbage",
   "runipy.main",
   "boto",
   "scipy.stats",
   "boto.s3.connection",
   "DAL",
   "czipfile",
   "leargist",
   "matplotlib",
   "heapq",
   "itertools",
   "collections",
   "matplotlib.pyplot",
   "matplotlib.backends",
   "scipy.linalg"
]

# Allow people to use UTF-8 and ASCII codecs in this script.
u"".encode('utf-8').decode('utf-8').encode('ascii').decode('ascii')

# Allow the idna encoding
u"".encode('idna').decode('idna')

# Allow the use of the "string-escape" encoding.
b"".decode('string-escape')

# Allow the use of pickling.
import pickle
pickle.loads(pickle.dumps([u"hello", u"world"]))

# Decide what user to run this script as.
user_id = pwd.getpwnam('sandbox').pw_uid
prefix = os.getcwd()
connect_to_ip = None

# Determine if the user running this script is an administrator.
task_id = sys.argv[2]
username = sys.argv[3]
file_name = sys.argv[4]

# Limit the number of processes in the sandbox.
resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))
resource.setrlimit(resource.RLIMIT_NPROC, (100, 100))

# Create proper environment variables.
os.environ = {
   'HOME': '/home',
   'PATH': '/',
   'LANG': 'en_US.UTF-8',
   'TASK_ID': task_id,
   'SUBMITTER': username,
}
os.umask(0)

# Set up necessary UNIX utilities.
try:
   os.mkdir('tmp', 0777); tempfile.tempdir = 'tmp'
   os.mkdir('home', 0777)
   os.mkdir('dev', 0555)

   os.mknod('dev/null',    0666 | stat.S_IFCHR, os.makedev(1, 3))
   os.mknod('dev/random',  0666 | stat.S_IFCHR, os.makedev(1, 8))
   os.mknod('dev/urandom', 0444 | stat.S_IFCHR, os.makedev(1, 9))

   # Add IPython cookes into the mix.
   os.makedirs(os.path.join('home', MAGIC_JSON_FILES), 0777)
   os.chown(os.path.join('home', MAGIC_JSON_FILES), user_id, user_id)

   # Add the DAL configuration file to the sandbox.
   shutil.copyfile('/worker/dalconfig.json', 'dalconfig.json')

   for item in os.listdir(os.path.join(HOME_DIR, MAGIC_JSON_FILES)):
      src = os.path.join(HOME_DIR, MAGIC_JSON_FILES, item)
      dst = os.path.join('home', MAGIC_JSON_FILES, item)
      
      shutil.copyfile(src, dst)
      os.chmod(dst, 0777)
      
      if item == 'controller-engine.json':
         contents = json.load(open(src, 'r'))
         connect_to_ip = contents['location']
   
   # Add the stupid README file IPython apparently needs.
   os.makedirs(os.path.dirname(USELESS_README))
   with open(USELESS_README, 'w') as fp:
      pass

except OSError:
   pass

# Spawn a cleanup daemon.
child = os.fork()
if child != 0:
   @atexit.register
   def cleanup_handler():
      # Kill all dangling processes.
      subprocess.call(['/usr/bin/killall', '-u', 'sandbox', '-9', '-w'])
      
      if sys.argv[1] == 'main':
         
         # Upload the resulting ipynb file to S3.
         import boto.s3
         connection = boto.connect_s3()
         bucket = connection.get_bucket('ml-submissions')
         key = bucket.new_key('results/' + task_id + '.ipynb')
         
         # Upload the resulting notebook.
         try:
            key.set_contents_from_filename("__saved.ipynb")
         except (OSError, IOError):
            traceback.print_exc()
      
      # Delete the sandbox.
      subprocess.call(['/bin/rm', '-rf', prefix])
   
   # Wait for child termination.
   result = os.wait()[1]
   
   print("Exit status: {}".format(result))
   sys.exit((result & 0xff) - ((result >> 8) & 0xff))

# Allow modification of the notebook.
for notebook in ["__main.ipynb", "__saved.ipynb"]:
    if os.path.exists(notebook):
        os.remove(notebook)

open("__saved.ipynb", "w").close()
os.chmod("__saved.ipynb", 0o666)

# Create a __main.ipynb file from whatever input we want.
with open("__main.ipynb", "w") as fp:

    if file_name.endswith(".py"):
        # Wrap a Python script as a notebook.
        notebook_data = {
            "metadata": { "name": "" },
            "nbformat": 3,
            "nbformat_minor": 0,
            "worksheets": [ {
                "cells": [ {
                    "cell_type": "code",
                    "collapsed": false,
                    "input": open(file_name).readlines(),
                    "language": "python",
                    "metadata": {},
                    "outputs": []
                } ],
                "metadata": {}
            } ]
        }
    else:
        # Read the notebook as given.
        notebook_data = json.loads(open(file_name).read())

    # Write the data back to the __main file.
    open("__main.ipynb", "w").write(json.dumps(notebook_data))

# Enable verbose logging.
import logging
logging.basicConfig(level=logging.DEBUG)

# Knock out the KernelManager.
import IPython.kernel
from IPython.kernel.inprocess import InProcessKernelManager

IPython.kernel.KernelManager = InProcessKernelManager

# Knock out the history cleaning thread.
from IPython.core.history import HistorySavingThread
HistorySavingThread.run = (lambda *v, **d: None)

# Mask exceptions out the 0MQ garbage collecting thread.
from zmq.error import ZMQError
from zmq.utils.garbage import GarbageCollectorThread

prev = GarbageCollectorThread.run
def masked(self, *vargs, **dargs):
    try:
        prev(self, *vargs, **dargs)
    except ZMQError:
        pass
GarbageCollectorThread.run = masked

# Knock out the history saving process.
from IPython.core import history
history.sqlite3 = None

# Allow pylab in the sandbox.
os.environ['HOME'] = os.path.join(prefix, 'home')
from runipy.notebook_runner import NotebookRunner, NotebookError
runner = NotebookRunner()
os.environ['HOME'] = 'home'

# Ensure that we also import the DAL.
sys.path.append("/worker/dal")

# Make sure we can display a PNG with PIL.
import scipy.misc
import numpy
import StringIO

img = scipy.misc.toimage(numpy.array([[0]]))
img.save(StringIO.StringIO(), format="PNG")

# Require the imported modules as late as possible.
os.chdir('/') # <- beware of os.getcwd() calls in initializers!
for module in ALLOWED_MODULES:
   __import__(module)
os.chdir(prefix)

# Actually drop down to an unprivileged user.
os.chroot('.')
os.setuid(user_id)

# Rewrite PATH so that we can import files if in a virtualenv.
new_path = []
for item in sys.path:
   if item.startswith(prefix):
      new_path.append(item[len(prefix):])

sys.path = new_path
sys.path.append('/')

# Reopen STDOUT and STDERR so that things get written immediately.
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0)

if sys.argv[1] == 'main':
   # Run the notebook or Python source file.
    runner.run_notebook("__main.ipynb", autosave = "__saved.ipynb")

elif sys.argv[1] == 'ipengine':
   # Run "ipengine"
   
   sys.argv = ['ipengine', '--log-level', 'ERROR']
   
   import IPython.parallel.apps.ipengineapp
   IPython.parallel.apps.ipengineapp.launch_new_instance()
   