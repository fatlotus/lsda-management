#!/usr/bin/env python
#
# This script runs "main.py" in a chroot. It is meant to be run setuid root.
#
# Author: Jeremy Archer <jarcher@uchicago.edu>
# Date: 2 January 2013
#

import os
import pwd
import sys
import resource

# Specifies what modules are allowed inside the sandbox. Unfortunately these
# modules will have root access to the computer.
ALLOWED_MODULES = [
   "os",
   "pwd",
   "sys",
   "IPython"
]

for module in ALLOWED_MODULES:
   __import__(module)

# Decide what user to run this script as.
user_id = pwd.getpwnam('sandbox').pw_uid
prefix = os.getcwd()

# Limit the number of processes in the sandbox.
resource.setrlimit(resource.RLIMIT_NOFILE, (1024, 1024))
resource.setrlimit(resource.RLIMIT_NPROC, (100, 100))

# Actually drop down to an unprivileged user.
os.chroot('.')
os.setuid(user_id)

# Rewrite PATH so that we can import files if in a virtualenv.
new_path = []
for item in sys.path:
   if item.startswith(prefix):
      new_path.append(item[len(prefix):])

sys.path = new_path

if sys.argv[1] == 'main':
   # Run "main.py"
   
   import runpy
   runpy.run_module('main', run_name = '__main__')

elif sys.argv[1] == 'ipengine':
   # Run "ipengine"
   
   import IPython.parallel.apps.ipengineapp
   IPython.parallel.apps.ipengineapp.launch_new_instance()