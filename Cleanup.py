#!/usr/local/env python
# vim:set expandtab ts=4 sw=4:

# Copyright (C) 2016 IHEP-CC

## Auth: Cheng ZhenJing. 7/3/2016

##
## Cleanup thread for the openstack HTCondor VCondor
## 
##

import sys
import os
import re
import time
import signal
import logging
import urllib
import urllib2
import threading
import traceback
import logging.handlers
from itertools import islice
from optparse import OptionParser
from decimal import *
from collections import defaultdict
import json

if sys.version_info[:2] < (2,5):
    print "You need at least Python 2.5 to run openstack Scheduler"
    sys.exit(1)

log_file = ""

import config as config
import utilities as utilities
from JClient import JClient as JClient
import cloud_management as cloud_management
import job_management as job_management
import openstackcluster
from openstackcluster import OpenStackCluster 
import Exception

log = utilities.get_vrmanager_logger()
ACTIVE_SET = [0,'Busy']
INACTIVE_SET = [1,'Idle']
ALL_STATUS_SET = [0,1,'Busy','Idle']

vr_request = {}
data_response = {}
vr_url=''


MIN_VM = 20
MAX_VM = 40
jc = None



##
## Functions
##


class Cleanup(threading.Thread):
    """
    Cleanup - Periodically cleanup vms in g_none group. 
            -  
    """

    def __init__(self,NoneGroup):
        threading.Thread.__init__(self, name=self.__class__.__name__)
        global log_file
        global GROUP_DICT
        log_file = config.log_file
        GROUP_DICT = config.GROUP_DICT
        logging.basicConfig(filename=log_file,format='%(asctime)s  [%(levelname)s] : %(module)s - %(message)s',level=logging.ERROR)
        print "none group is %s" % NoneGroup
        self.NoneGroup = NoneGroup
        self.quit = False
        self.sleep_tics = config.cleanup_interval
       


    def stop(self):
        log.debug("Waiting for cleanup loop to end")
        self.quit = True


    def run(self):
        log.info("Starting Cleanup Thread...")

        ResourcePool = cloud_management.ResourcePool()
        OpenstackCluster = openstackcluster.OpenStackCluster(name='',username='',password='',tenant_id='',auth_url='')
        while not self.quit:
            start_loop_time = time.time()

            """Try to execute command 'condor_status -l' and transform the output into machine objects."""
            try:
                condor_status_machinelist = ResourcePool.resource_query_local(self.NoneGroup)
            except Exception as e:
                log.error("Some error occured when trying to excute function ResourcePool.resource_query_local().")
            
            """Try to find vms running on openstack and transform the output into VM objects."""
            vms = ()
            vms_new = ()

            try:
                vms = OpenstackCluster.get_vms_local()
            except Exception as e:
                print e
                log.error("Some error occured when trying to excute function OpenstackCluster.get_vms_local().")
                #sys.exit(1)
                continue

            """Try to modify VM object's job attribute with results from command 'condor_status -l'."""
            try:
                vms_new = ResourcePool.update_vmslist_from_machinelist(vms,condor_status_machinelist)
                OpenstackCluster.vms = vms_new
            except Exception as e:
                log.error("Some error occured when trying to excute function ResourcePool.update_vmslist_from machinelist")
                print e

            num_vm_busy = OpenstackCluster.num_vms_by_group_activity(vms=vms_new,group=GROUP_DICT[self.NoneGroup],activity=ACTIVE_SET)
            print "num_vm_busy"
            print num_vm_busy

            num_vm_idle = OpenstackCluster.num_vms_by_group_activity(vms=vms_new,group=GROUP_DICT[self.NoneGroup],activity=INACTIVE_SET)
            print "num_vm_idle"
            print num_vm_idle

            num_vm_all = num_vm_busy+num_vm_idle
            num_vm_to_launch = 0
            num_vm_to_destroy = num_vm_idle
            print "Cleanup %d instance of %s!"% (num_vm_to_destroy, self.NoneGroup)

            if (num_vm_to_destroy > 0):
                try:
                    OpenstackCluster.vm_destroy_by_Group_JobActivity(count=num_vm_to_destroy,group=GROUP_DICT[self.NoneGroup],activity=INACTIVE_SET,vms=vms_new)
                except Exception as e:
                    log.error("Unable to destroy %d instances by method OpenstackCluster.vm_destroy_by_Group_JobActivity for group %s." 
                             % (num_vm_to_destroy,self.NoneGroup))
                    print e
            
 
            config.setup()
            if config.exit=='false':
                print 'exit'
                self.quit = True
            self.sleep_tics = config.cleanup_interval
            if (not self.quit) and self.sleep_tics > 0:
                print self.sleep_tics
                time.sleep(self.sleep_tics)

'''
def main():
    """Main Function of VRManager."""
    config.setup()

    service_threads = []

    #Create the Cleanup Thread
    cleaner = Cleanup()
    service_threads.append(cleaner)
    for thread in service_threads:
        thread.start()

    #for thread in service_threads:
        #thread.stop()

    for thread in service_threads:
        thread.join()

    return 0

'''

    

