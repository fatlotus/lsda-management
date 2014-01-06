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
import stat
import resource
import shutil
import subprocess

# Bane of my existence... :(
MAGIC_JSON_FILES = '.ipython/profile_default/security'
HOME_DIR = '/home/lsda' # <- Don't put a slash at the end!

# Specifies what modules are allowed inside the sandbox. Unfortunately these
# modules will have root access to the computer.
ALLOWED_MODULES = [
   "os",
   "pwd",
   "sys",
   "IPython",
   "IPython.parallel",
   "apport.fileutils",
   "_strptime"
]

for module in ALLOWED_MODULES:
   __import__(module)

# Allow people to use UTF-8 and ASCII codecs in this script.
u"".encode('utf-8').decode('utf-8').encode('ascii').decode('ascii')

# Decide what user to run this script as.
user_id = pwd.getpwnam('sandbox').pw_uid
prefix = os.getcwd()

# Limit the number of processes in the sandbox.
resource.setrlimit(resource.RLIMIT_NOFILE, (1024, 1024))
resource.setrlimit(resource.RLIMIT_NPROC, (100, 100))

# Create proper environment variables.
os.environ['HOME'] = '/home'
os.umask(0)

# Set up necessary UNIX utilities.
try:
   os.mkdir('tmp', 0777)
   os.mkdir('home', 0777)
   os.mkdir('dev', 0555)

   os.mknod('dev/null',    0666 | stat.S_IFCHR, os.makedev(1, 3))
   os.mknod('dev/random',  0666 | stat.S_IFCHR, os.makedev(1, 8))
   os.mknod('dev/urandom', 0444 | stat.S_IFCHR, os.makedev(1, 9))

   # Add IPython cookes into the mix.
   os.makedirs(os.path.join('home', MAGIC_JSON_FILES), 0777)

   for item in os.listdir(os.path.join(HOME_DIR, MAGIC_JSON_FILES)):
      src = os.path.join(HOME_DIR, MAGIC_JSON_FILES, item)
      dst = os.path.join('home', MAGIC_JSON_FILES, item)
      
      shutil.copyfile(src, dst)
      os.chmod(dst, 0777)

except OSError:
   pass

# Spawn a cleanup daemon.
if os.fork() != 0:
   os.wait()
   
   # Kill all dangling processes.
   subprocess.call(['/usr/bin/killall', '-u', 'sandbox', '-9', '-w'])
   
   # Delete the sandbox.
   subprocess.call(['/bin/rm', '-rf', prefix])
   
   sys.exit(0)

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

if sys.argv[1] == 'main':
   # Run "main.py"
   
   sys.argv = ['main.py']
   
   import runpy
   runpy.run_module('main', run_name = '__main__')

elif sys.argv[1] == 'ipengine':
   # Run "ipengine"
   
   sys.argv = ['ipengine']
   
   import IPython.parallel.apps.ipengineapp
   IPython.parallel.apps.ipengineapp.launch_new_instance()
   