#!/user/bin/env python
# vim:set expandtab ts=4 sw=4:

# Copyright (C) 2016 IHEP-CC

## Auth: Cheng ZhenJing. 7/3/2016

import os
import re
import sys
import time
import uuid
import string
import shutil
import logging
import datetime
import tempfile
import subprocess
import threading

from subprocess import Popen
from urlparse import urlparse

import config
import utilities as utilities

logging.basicConfig(filename=config.log_file,format='%(asctime)s  [%(levelname)s] : %(module)s - %(message)s',level=logging.ERROR)

log = utilities.get_vrmanager_logger()

class VM:
    """
    A class for storing created VM information. Used to populate Cluster classes
    'vms' lists.
    Instance Variables
    The global VM states are:
       Starting - The VM is being created in the cloud
       Running  - The VM is running somewhere on the cloud (fully functional)
       Error    - The VM has been corrupted or is in the process of being destroyed
    States are defined in each Cluster subclass, in which a VM_STATES dictionary
    maps specific cloud software state to these global states.
    """



    def __init__(self,name="",uuid="",resource_group_type="",group="",owner="",activity="",
            jobid="",hostname="",ipaddress="",macaddress="",network="",clusteraddr="",clusterport="",
            cloudtype="openstack",image_name="",flavor="",cpucores=0,memory=0,
            storage=0,keep_alive=0):
        """
        Constructor

        name                    - (str) The displayname of the vm (arbitrary)
        uuid                      - (str) The id tag for the VM. In openstack it means uuid of a certain VM.
        resource_group_type     - (str) The condor resource_group_type attribute.For example,lhaaso/lhassonrun etc.
        group                   - (str) The group who 'owns' this VM.For example,lhasso,juno etc.
        owner                   - (str) The Owner of the job ruuning on vm.
        activity                - (str) Busy or Idle of the job.
        jobid                   - (str) GlobalJobId of the job.
        hostname                - (str) The first part of hostname given to VM
        ipaddress               - (str) The IP Address of the VM.
        macaddress              - (str) The mac Address of the VM.
        network                 - (str) The network of the VM
        clusteraddr             - (str) The address of the cluster hosting the VM
        clusterport             - (str) The port of the cluster hosting the VM
        cloudtype               - (str) The cloud type of the VM (Nimbus, OpenNebula, etc)
        image_name              - (str) The image_name of the VM.
        flavor                  - (str) The flavor/instance_type of the VM
        cpucores                - (int) The cpucores of the VM
        memory                  - (int) The memory used of the VM (MB)
        storage                 - (int) The storage used of the VM (GB)
        keep_alive              - (int) The max living time of a VM (minutes)
        proxy_file              - the proxy that was used to authenticate this VM's creation
        """
        self.name = name
        self.uuid = uuid
        self.resource_group_type = resource_group_type
        self.group = group
        self.owner = owner
        self.activity = activity
        self.jobid = jobid
        self.hostname = hostname
        self.ipaddress = ipaddress
        self.macaddress = macaddress
        self.network = network
        self.clusteraddr = clusteraddr
        self.clusterport = clusterport
        self.cloudtype = cloudtype
        self.image_name = image_name
        self.flavor = flavor
        self.cpucores = cpucores
        self.memory = memory
        self.storage = storage
        self.keep_alive = keep_alive

        # Set a status variable on new creation
        self.status = "Starting"
	"""
        global log
        log = logging.getLogger("VRManager")

        log.verbose("New VM Object - Name: %s, id: %s, owner: %s, group: %s, hostname: %s, ipaddress: %s, network: %s, clusteraddr: %s, \
            image_name: %s, flavor: %s, cpucores: %d, memory: %d, storage: %d" % (name,uuid,owner,group,hostname,ipaddress,network,clusteraddr,
            image_name,flavor,cpucores,memory,storage))
        log.info("New VM Object - Name: %s, id: %s, owner: %s, group: %s, hostname: %s, ipaddress: %s, network: %s, clusteraddr: %s, \
            image_name: %s, flavor: %s, cpucores: %d, memory: %d, storage: %d" % (name,uuid,owner,group,hostname,ipaddress,network,clusteraddr,
            image_name,flavor,cpucores,memory,storage))
       """

    def log(self):
        """Log the VM to the info level."""
        log.info("VM INFO - Name: %s, id: %s, resource_group_type: %s, group: %s, activity, jobid: %s, hostname: %s, ipaddress: %s, network: %s, clusteraddr: %s, \
            image_name: %s, flavor: %s, cpucores: %d, memory: %d, storage: %d" % (name,uuid,resource_group_type,group,activity,jobid,hostname,ipaddress,network,clusteraddr,
            image_name,flavor,cpucores,memory,storage))

    def log_dbg(self):
        """Log the VM to the debug level."""
        log.info("VM DEBUG - Name: %s, id: %s, resource_group_type: %s, group: %s, activity, jobid: %s, hostname: %s, ipaddress: %s, network: %s,clusteraddr: %s, \
            image_name: %s, flavor: %s, cpucores: %d, memory: %d, storage: %d" % (name,uuid,resource_group_type,group,activity,jobid,hostname,ipaddress,network,clusteraddr,
            image_name,flavor,cpucores,memory,storage))

    def get_vm_info(self):
        """Formatted VM information for use."""
        output = "%-15s %-30s %-15s %-10s %-15s %-15s %-15s %-15s %-15s %-15s %-10s %-10d %-10d %-10d" % (name,uuid,owner,group,activity,hostname,network,ipaddress,
                    clusteraddr, image_name,flavor,cpucores,memory,storage)
        return output

    def get_vm_info_header():
        """Formatted header for use output."""
        return "%-15s %-30s %-15s %-10s %-15s %-15s %-15s %-15s %-15s %-15s %-10s %-10d %-10d %-10d" % ("NAME","UUID","OWNER",
                    "GROUP","ACTIVITY","HOSTNAME","NETWORK","IPADDRESS","CLUSTERADDR","IMAGE_NAME","FLAVOR","CPUCORES","MEMORY","STORAGE")

    def get_vm_info_pretty(self):
        """Header + VM info formatted output."""
        output = self.get_vm_info_header()
        output += self.get_vm_info()
        return output


class NoResourcesError(Exception):
    """Exception raised for errors where not enough resources are available
    Attributes:
        resource -- name of resource that is insufficient
    """

    def __init__(self, resource):
        self.resource = resource

       
class ICluster(object):
    """
    The ICluster interface is the framework for implementing support for
    openstack. In general, you will need to override __init__ (be sure 
    to call super's init),vm_create, vm_poll, and vm_destroy
    """

    #vms_by_group = []
    #vms_by_group_activity = []

    def __init__(self,username,password,tenant_id,auth_url,name='openstack'):
        self.name = name
        self.username = username
        self.password = password
        self.tenant_id = tenant_id
        self.auth_url = auth_url
        self.vms = []
        self.vms_lock = threading.RLock()
        self.res_lock = threading.RLock()

        self.setup_logging()
        log.debug("New cluster %s create" % self.name)

    def __getstate__(self):
        pass

    def __setstate__(self, state):
        pass

    def __repr__(self):
        return self.name

    def setup_logging(self):
        """Fetch the global log object."""
        global log
        log = logging.getLogger("vrmanager")

    def log_cluster(self):
        """Print cluster information to the log."""
        pass

    def log(self):
        """Print a short form of cluster information to the log."""
        pass

    def log_vms(self):
        """Print the cluster 'vms' list (via VM print)."""
        if len(self.vms) == 0:
            log.info("CLUSTER %s has no running VMs..." % (self.name))
        else:
            log.info("CLUSTER %s running VMs:" % (self.name))
            for vm in self.vms:
                vm.log_short("\t")

    
    ## Support methods

   
    def slot_fill_ratio(self):
        pass

    def get_cluster_info_short(self):
        pass

    def get_cluster_vms_info(self):
        """Return information about running VMs on Cluster as a string."""
        if len(self.vms) == 0:
            return ""
        else:
            output = ""
            for vm in self.vms:
                output += "%s %-15s\n" % (vm.get_vm_info()[:-1], self.name)
            return output

    # VM manipulation methods
    #-!------------------------------------------------------------------
    # NOTE: In implementing subclasses of Cluster, the following method prototypes
    #       should be used (standrdize on these parameters)
    #-!------------------------------------------------------------------

    def vm_create(self,**args):
        log.debug('This method should be defined by all subclasses of Cluster\n')

    def vm_destroy(self, vm, return_resources=True, reason=""):
        log.debug('This method should be defined by all subclasses of Cluster\n')

    def vm_poll(self, vm):
        log.debug('This method should be defined by all subclasses of Cluster\n')


    ## Private VM methods
    
    def find_mementry(self, memory):
        pass

    def find_potential_mementry(self,memory):
        pass

    def resource_check_out(self,vm):
        pass

    def resource_return(self,vm):
        pass

    def _generate_next_name(self):
        pass









































 
