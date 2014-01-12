#!/usr/bin/env python
#
### Quota copier interface.
# 
# Running on the Git server, this script copies quota values from a YAML file
# to ZooKeeper.
#
# Author: Jeremy Archer <jarcher@uchicago.edu>
# Date: 12 January 2013
# 

# Import the pure-Python ZooKeeper implementation.
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

import argparse
import yaml

def main():
   # Prepare default configuration options.
   parser = argparse.ArgumentParser(
     description = 'Copies quotas from the given YAML file to ZooKeeper.')
   parser.add_argument('--zookeeper', action = 'append', required=True)
   parser.add_argument('--config', required=True)
   
   options = parser.parse_args()
   
   # Connect to ZooKeeper.
   zookeeper = KazooClient(
     hosts = ','.join(options.zookeeper)
   )
   
   zookeeper.start()
   
   # Read the YAML configuration file.
   with open(options.config) as fp:
      config_file = yaml.load(fp)
   
   # Process each type of limit from the configuration file.
   for resource, limits in config_file.items():
      
      # Register this resource in ZooKeeper.
      path = '/quota_limit/{resource}'.format(**locals())
      zookeeper.ensure_path(path)
      
      # List all available CNetIDs.
      already_tracked = zookeeper.get_children(path)
      
      # Update every CNetID we can know about.
      cnetids = set(limits.keys()) | set(already_tracked)
      
      for cnetid in cnetids:
         
         # See what quota is already present.
         cnetid_path = ('/quota_limit/{resource}/{cnetid}'.format(**locals())
                         .encode('utf-8'))
         limit = limits.get(cnetid, 0)
         
         try:
            # Update the actual quota value.
            if limit != 0:
               if cnetid in already_tracked:
                  zookeeper.set(cnetid_path, str(limit))
               else:
                  zookeeper.create(cnetid_path, str(limit))
            else:
               zookeeper.delete(cnetid_path)
            
         except KazooException, exc:
            print("{cnetid:30} {exc:r}".format(**locals()))
   
   # Clean up the connection to ZooKeeper.
   zookeeper.stop()

if __name__ == '__main__':
   main()