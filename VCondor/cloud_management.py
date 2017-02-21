#!/user/bin/env python
# vim:set expandtab ts=4 sw=4:
    
# Copyright (C) 2016 IHEP-CC

## Auth: Cheng ZhenJing. 7/3/2016

import os
import re
import sys
import json
import time
import copy
import shlex
import string
import logging
import tempfile
import threading
import subprocess
import ConfigParser

from decimal import *
from collections import defaultdict

try:
    import cPickle as pickle
except:
    import pickle

import cluster_tools
#from openstackcluster
import openstackcluster
from openstackcluster import OpenStackCluster

import config as config

from utilities import determine_path
from utilities import get_or_none
from utilities import ErrTrackQueue
from utilities import splitnstrip
import utilities as utilities


log = None
##
##  CLASSES
##


class ResourcePool():

    """Stores and organizes a list of Cluster resources."""
    ## Instance variables
    resources = []
    machine_list = []
    prev_machine_list = []
    vm_machine_list = []
    prev_vm_machine_list = []
    master_list = []
    retired_resources = []
    config_file = ""

    def __init__(self,):

        global log
        logging.basicConfig(filename=config.log_file,format='%(asctime)s  [%(levelname)s] : %(module)s - %(message)s',level=logging.ERROR)
        log = utilities.get_vrmanager_logger()

    ## Instance methods
    def resource_query_local(self,group):
        """
        resource_query_local -- does a Query to the condor collector
        Returns a list of dictionaries with information about the machines
        registered with condor.
        """
        log.verbose("Querying Condor Collector with %s" % config.condor_status_command)
        print "group is %s" % group
        print "Querying Condor Collector with %s" % config.condor_status_command[group]
        #sys.exit(1)
        condor_status=condor_out=condor_err=""
        try:
            condor_status = config.condor_status_command[group]
            print "condor_status command is %s" % condor_status
            sp = subprocess.Popen(condor_status, shell=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (condor_out, condor_err) = sp.communicate(input=None)
	    
        except OSError:
            log.error("OSError occured while doing condor_status - will try again next cycle.")
            return []
        except:
            log.exception("Problem running %s, unexpected error: %s" % (string.join(condor_status, " "), condor_err))
            return []
	
        return self._condor_status_to_machine_list(condor_out)


    @staticmethod
    def _condor_status_to_machine_list(condor_status_output):
        """
        _condor_status_to_machine_list - Converts the output of
               condor_status -l to a list of dictionaries with the attributes
               from the Condor machine ad.
               returns [] is there are no machines
        """

        machines = []

        # Each classad is seperated by '\n\n'
        raw_machine_classads = condor_status_output.split("\n\n")
        # Empty condor pools give us an empty string or stray \n in our list
        raw_machine_classads = filter(lambda x: x != "" and x != "\n", raw_machine_classads)

        for raw_classad in raw_machine_classads:
            classad = {}
            classad_lines = raw_classad.splitlines()
            for classad_line in classad_lines:
                classad_line = classad_line.strip()
                (classad_key, classad_value) = classad_line.split(" = ", 1)
                classad_value = classad_value.strip('"')
                if classad_key in ['Machine','HardwareAddress','Activity','Name','Start','AccountingGroup','RemoteOwner','RemoteUser','JobId','GlobalJobId']:
                    classad[classad_key] = classad_value

            machines.append(classad)

        return machines


    def update_vmslist_from_machinelist(self, vms, machinelist):
	machine_num = len(machinelist)
        for machine in machinelist:
            try:
                hostname = resource_group_type = activity = owner = jobid = MacAddress = IpAddress =\
                     address_master = state = activity = vmtype = start_req = \
                     remote_owner = slot_type = total_slots = ""
                if machine.has_key('Machine'):
                    hostname = machine['Machine']
                if machine.has_key('Start'):
                    temp_group = machine['Start'].split('"',-1)
		    lenth = len(temp_group)
		    num = lenth
		    resource_group_list = []
		    while (num>1):
			num -= 2
			resource_group_list.append(temp_group[num])
		    resource_group_type = "|".join(str(i) for i in resource_group_list)	
		    
                if machine.has_key('Activity'):
                    activity = machine['Activity']
                if machine.has_key('RemoteOwner'):
                    owner = machine['RemoteOwner'].split('@')[0]
                if machine.has_key('GlobalJobId'):
                    jobid = machine['GlobalJobId']
                if machine.has_key('HardwareAddress'):
                    MacAddress = machine['HardwareAddress']
                if machine.has_key('MyAddress'):
                    IpAddress = machine['MyAddress'].split('<')[1].split(':')[0]

                #try:
                    #vm = self.get_vm_by_ip_or_mac(vms, MacAddress)
                #except:
                    #log.error("Unable to get vm by ipaddress or macaddress: %s" % MacAddress)
                    #log.error("I think some terrible error occured! You need to shut down VRManager at now!")
               
                try:
                    self.update_job_status_for_vm(vms,MacAddress,resource_group_type,resource_group_type,owner,activity,jobid,hostname)
                    self.update_job_status_for_vm(vms,IpAddress,resource_group_type,resource_group_type,owner,activity,jobid,hostname)
                except Exception, e:
                    print e
                    #log.error("Unable to update job status for vm: %s." % hostname)
                    #log.error("I think some terrible error occured! I will try it next cycle.")

            except Exception as e:
                log.error("Failed to update a VM Obj by condor_status command!")
        return vms
    
    
    def get_vm_by_ip_or_mac(self,vms,address=''):
        """Find the vm by ipaddress or macaddress and return the vm object.
        """
        for vm in vms:
            if (vm.ipaddress==address) or (vm.macaddress==address):
        	return vm
        #log.error("Cannot find a vm object by address=%s. " % address)
        return None

    def update_job_status_for_vm(self,vms,address,resource_group_type='',group='',owner='',activity='',jobid='',hostname=''):
        """For a certain vm object, update status of the job running on it."""
	for vm in vms:
	    if (vm.ipaddress==address) or (vm.macaddress==address):
        	try:
                    #print "update %s" % vm.ipaddress
                    #print group
                    #print activity
                    vm.resource_group_type = repr(resource_group_type)
            	    vm.group = repr(group)
            	    vm.owner = repr(owner)
            	    vm.activity = repr(activity)
            	    vm.jobid = repr(jobid)
            	    vm.hostname = repr(hostname)
       		except Exception, e:
            	    print e
                    log.error("Unable to update job status for a certain VM: %s %s" % (vm.id,vm.name))
		    return 0
		return 1
	#log.error("Cannot find a vm object by address=%s. " % address)
        return None


'''
class VMDestroyCmd(threading.Thread):
    """
    VMCmd - passing shutdown and destroy requests to a separate thread
    """

    def __init__(self,OpenstackCluster,count=0,group=[],activity=[],vms=[]):
        threading.Thread.__init__(self, name=self.__class__.__name__)
        self.OpenstackCluster = OpenstackCluster
        self.count = count
        self.group = group
        self.activity = activity
        self.vms = vms
        self.result = None
        self.init_time = time.time()
    def run(self):
        self.result = self.OpenstackCluster.vm_destroy_by_Group_JobActivity(count=self.count,
                group=self.group,activity=self.activity,vms=self.vms)
    def get_result(self):
        return self.result
    def get_vm(self):
        return self.vm
'''
