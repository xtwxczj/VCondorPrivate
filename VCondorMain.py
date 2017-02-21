#!/usr/local/env python
# vim:set expandtab ts=4 sw=4:

# Copyright (C) 2016 IHEP-CC

## Auth: Cheng ZhenJing. 7/3/2016

## Openstack HTCondor VRManager
##
## The main body for the openstack HTCondor VRManager, that encapsulates and organizes
## all openstack HTCondor VRManager functionally.
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
import gc
import objgraph
import logging.handlers
from itertools import islice
from optparse import OptionParser
from decimal import *
from collections import defaultdict
import json

if sys.version_info[:2] < (2,5):
    print "You need at least Python 2.5 to run openstack Scheduler"
    sys.exit(1)

if not './VCondor' in sys.path:
    sys.path.append('./VCondor')


import config as config
import utilities as utilities
from JClient import JClient as JClient
import cloud_management as cloud_management
import job_management as job_management
import openstackcluster
from openstackcluster import OpenStackCluster
from Cleanup import Cleanup
import Exception
import Mail

#log = utilities.get_vrmanager_logger()

'''
GROUP_SET = [['lhaasorun|lhaaso'],['juno']
#GROUP_DICT = [{'lhaaso','lhaasorun'}:'lhaaso',{'juno'}:'juno']
GROUP_DICT = {'LHAASO':['lhaasorun|lhaaso'],'JUNO':['juno']}
IMAGE_DICT = {'LHAASO':'cirros','JUNO':'cirros'} # use image name or image uuid
FLAVOR_DICT = {'LHAASO':'','JUNO':''} # use flavor uuid
NETWORK_DICT = {'LHAASO':'','JUNO':''} # use network name
'''

'''
GROUP_SET = [['lhaasorun|lhaaso']]
GROUP_DICT = {'LHAASO':['lhaasorun|lhaaso']}
IMAGE_DICT = {'LHAASO':'EOS-WN-cctest-40G-2'}
FLAVOR_DICT = {'LHAASO':'22c0ecc0-3b0a-4c7f-8b5f-f74540adc727'}
NETWORK_DICT = {'LHAASO':'V1086|V1087'}
NETWORK_QUOTA_DICT = {'V1086':252,'V1087':250}
'''

ACTIVE_SET = [0,'Busy']
INACTIVE_SET = [1,'Idle']
ALL_STATUS_SET = [0,1,'Busy','Idle']

vr_request = {}
data_response = {}
vr_url=''


MemLogger = None
MIN_VM = 20
MAX_VM = 40
jc = None
# Threaded Classes



##
## Functions
##

def has_value(DICT,VALUE):
    """Return whether a dictionary has a certain value."""
    for val in DICT.values():
        if (cmp(repr(val),repr(VALUE))==0):
            logger.info('Find %s in DICT %s' % (repr(VALUE),repr(DICT)))
            return True
    logger.error('Error:%s is not in DICT:%s' % (repr(VALUE),repr(DICT)))
    return False

def get_key_by_value(DICT,VALUE):
    """Return key by a certain value in a dictionary."""
    for key, val in DICT.items():
        if (cmp(repr(val),repr(VALUE))==0):
            logger.info('Find %s in DICT %s, return key %s' % (repr(VALUE),repr(DICT),key))
            return key
    logger.error('Error:%s is not in DICT:%s' % (repr(VALUE),repr(DICT)))
    return None

def get_request_by_group(group_name_list):
    """Get request string by input :resource_group_name"""
    if has_value(GROUP_DICT,group_name_list):
        ResourceGroup = get_key_by_value(GROUP_DICT, group_name_list)
        vr_request = {"ResID":ResourceGroup}
        logger.debug('Send Json Data: %s' % vr_request)
        return vr_request
    else:
        logger.error('Send a null Json Data to VR! System failed!')
        return 'null'

def get_vr_response(json_req,json_resp):
    """Send a json string to vr written by Li HaiBo and catch the responsei."""
    pass

def decode_vr_response(json_resp,name):
    """Decode a json reponse string by name like ResID or Min."""
    pass

#@profile
def loop_group(jc,group_name_list):
    """Main loop for every group_name_list"""

    #global _VRManagerAvail
    #global logger
    global MemLogger
    logger.info('Starting a thread:loop_group for %s' % group_name_list)
    num_vm_to_launch = 0
    num_vm_to_destroy = 0

    """Let's suppose we have not sent an email in the last loop."""
    _AbleToSendMail = True

    exit = False
    if config.exit=='true':
        exit = True
    else:
        exit = False

    ResourcePool = cloud_management.ResourcePool()
    OpenstackCluster = openstackcluster.OpenStackCluster(name='',username='',password='',tenant_id='',auth_url='')
    JobPool = job_management.JobPool(name=config.condor_pool[get_key_by_value(GROUP_DICT, group_name_list)])

    while(exit):
        ### collect garbage.
        gc.collect()
        objgraph.show_growth()
        objgraph.show_most_common_types(limit=50)

        config.setup()
        try:
            jc = JClient(host='192.168.86.3', port=27020, bufsize=1024, allow_reuse_addr=True)
        except Exception,e:
            logger.error("Unable to create JClient object or connect to vr.")
            logger.error(e)

        """Let's suppose VRManager is unavailable first."""
        _VRManagerAvail = False

    	"""Try to connect to vm_quota_control designed by Li HaiBo.""" 
        string_request = get_request_by_group(group_name_list)
        if (string_request!='null'):
            try:
                logger.debug('String_request sent to VRManager: %s' % string_request)
                result = jc.JSONFormatCheck(string_request, config.FormatKeysListSend, config.FormatTypeDictSend)
                if(result==0):
                    logger.error("Data_request:%s format is wrong! You shall check it carefully!" % string_request)
                    """Send a mail to Administrator, and set _AbleToSendMail false."""
                    if (_AbleToSendMail==True):
                        Mail.sendTextMail('WARNING!!!!!!!!!!!!!!!!!!!!!!',"Data_request:%s format is wrong! You shall check it carefully!"
                                 % string_request)
                        _AbleToSendMail = False
                        time.sleep(600)
                    continue
                data_response = jc.oneRequest(string_request)
                logger.error('RECV data:%s' % data_response)
                logger.debug('RECV data:%s' % data_response)
                result = jc.JSONFormatCheck(data_response, config.FormatKeysListRecv, config.FormatTypeDictRecv)
                if(result==0):
                    logger.error("Data_response:%s format is wrong! You can check Remote VRManager!" % data_response)
                    """Send a mail to Administrator, and set _AbleToSendMail false."""
                    if (_AbleToSendMail==True):
                        Mail.sendTextMail('WARNING!!!!!!!!!!!!!!!!!!!!!!',"Data_response:%s format is wrong! You can check Remote VRManager!" 
                                 % data_response)
                        _AbleToSendMail = False
                    time.sleep(600)
                    continue
                MIN_VM = int(data_response['MIN'])
                MAX_VM_TO_LAUNCH = int(data_response['AVAILABLE'])
                logger.debug("Data_response: MIN_VM is %d,MAX_VM_TO_LAUNCH is %d." % (MIN_VM,MAX_VM_TO_LAUNCH))

                """Set _AbleToSendMail true."""
                _AbleToSendMail = True
            #except Exception.SendNullException, e:
                #continue
            except Exception, e:
                #MIN_VM = 355
                #MAX_VM_TO_LAUNCH = 5
                #logging.error("Unable to decode data_response by VR. Use default MIN_VM=%d,MAX_VM_TO_LAUNCH=%d." % (MIN_VM,MAX_VM_TO_LAUNCH))
                logger.error("Unable to decode data_response by VR.")
                logger.error(e)
                print e
                """Send a mail to Administrator, and set _AbleToSendMail false."""
                if (_AbleToSendMail==True):
                    Mail.sendTextMail('WARNING!!!!!!!!!!!!!!!!!!!!!!',"Unable to decode data_response by VR.There Must be something wrong! You shall check it carefully.")
                    _AbleToSendMail = False
                logger.error('sleep 60')
                time.sleep(60)
                continue
            except StandardError,e:
                #MIN_VM = 355
                #MAX_VM_TO_LAUNCH = 5
                #logging.error("Unable to decode data_response by VR. Use default MIN_VM=%d,MAX_VM_TO_LAUNCH=%d." % (MIN_VM,MAX_VM_TO_LAUNCH))
                logger.error("Unable to decode data_response by VR.")
                logger.error(e)
                print e
                """Send a mail to Administrator, and set _AbleToSendMail false."""
                if (_AbleToSendMail==True):
                    Mail.sendTextMail('WARNING!!!!!!!!!!!!!!!!!!!!!!',"Unable to decode data_response by VR.There Must be something wrong! You shall check it carefully.")
                    _AbleToSendMail = False
                logger.error('sleep 60')
                time.sleep(60)
                continue
 

        else:
            logger.error("Unable to create request string which will be sent to vr. System exit.")
            time.sleep(600)
            #sys.exit(1)
            continue

        """Try to execute command 'condor_status -l' and transform the output into machine objects."""
        #ResourcePool = cloud_management.ResourcePool()
        try:
            condor_status_machinelist = ResourcePool.resource_query_local(get_key_by_value(GROUP_DICT, group_name_list))
        except Exception as e:
            logger.error("Some error occured when trying to excute function ResourcePool.resource_query_local().")
            #sys.exit(1)
            continue

        """Try to find vms running on openstack and transform the output into VM objects."""
        #OpenstackCluster = openstackcluster.OpenStackCluster(name='',username='',password='',tenant_id='',auth_url='')
        vms = ()
        vms_new = ()

        try:
            vms = OpenstackCluster.get_vms_local()
        except Exception as e:
            logger.error("Some error occured when trying to excute function OpenstackCluster.get_vms_local().")
    	    #sys.exit(1)
            continue

        """Try to modify VM object's job attribute with results from command 'condor_status -l'."""
        try:
            vms_new = ResourcePool.update_vmslist_from_machinelist(vms,condor_status_machinelist)
            OpenstackCluster.vms = vms_new
        except Exception as e:
            logger.error("Some error occured when trying to excute function ResourcePool.update_vmslist_from machinelist")

        #for vm in vms_new:
        
        """Try to figure out how many jobs for each group and those jobs' activity(idle or running). """
        #JobPool = job_management.JobPool(name='condor', condor_query_type='local')
        job_ads = JobPool.job_query(get_key_by_value(GROUP_DICT, group_name_list))




        vm_name = group_name_list[0] + "_VM"
        start = time.time()

        image_name = ''
        flavor_uuid = ''
        network_list = ''
        try:
            if  has_value(GROUP_DICT, group_name_list) and IMAGE_DICT.has_key(get_key_by_value(GROUP_DICT, group_name_list)):
                image_name = IMAGE_DICT[get_key_by_value(GROUP_DICT, group_name_list)]
            else:
                logger.error("Unable to find image for group:%s." % '|'.join(group_name_list))
        except Exception as e:
    	    logger.error(e)
            logger.error("Terrible fault occured. You must stop vrmanager now.")
            #return sys.exit(1)

        try:
            if has_value(GROUP_DICT, group_name_list) and FLAVOR_DICT.has_key(get_key_by_value(GROUP_DICT, group_name_list)):
                flavor_uuid = FLAVOR_DICT[get_key_by_value(GROUP_DICT, group_name_list)]
            else:
                logger.error("Unable to find flavor uuid for group:%s." % '|'.join(group_name_list))
                raise Exception("Unable to find flavor uuid for group:%s." % '|'.join(group_name_list))
        except Exception as e:
    	    logger.error(e)
            logger.error("Terrible fault occured. You must stop vrmanager now.")
            #return sys.exit(1)

        try:
            if has_value(GROUP_DICT, group_name_list) and NETWORK_DICT.has_key(get_key_by_value(GROUP_DICT, group_name_list)):
                network_list = NETWORK_DICT[get_key_by_value(GROUP_DICT, group_name_list)]
            else:
                logger.error("Unable to find network name for group:%s." % '|'.join(group_name_list))
                raise Exception("Unable to find network name for group:%s." % '|'.join(group_name_list))
        except Exception as e:
    	    logger.error(e)
            logger.error("Terrible fault occured. You must stop vrmanager now.")
            continue

        num_vm_busy = OpenstackCluster.num_vms_by_group_activity(vms=vms_new,group=group_name_list,activity=ACTIVE_SET)
        logger.info("Group: %s num_vm_busy: %s " % (group_name_list,num_vm_busy))

        num_vm_idle = OpenstackCluster.num_vms_by_group_activity(vms=vms_new,group=group_name_list,activity=INACTIVE_SET)
        logger.info("Group: %s num_vm_idle: %s " % (group_name_list,num_vm_idle))

        num_vm_all = num_vm_busy+num_vm_idle
        logger.info("Group: %s num_vm_all: %s " % (group_name_list,num_vm_all))

        num_job_idle = JobPool.get_jobcount_by_group_activity(jobs=job_ads,Group=group_name_list,JobStatus='idle')
        logger.info("Group: %s num_job_idle: %s " % (group_name_list,num_job_idle))

        num_job_running = JobPool.get_jobcount_by_group_activity(jobs=job_ads,Group=group_name_list,JobStatus='running')
        logger.info("Group: %s num_job_running: %s " % (group_name_list,num_job_running))

        if (num_job_idle>0)and(num_vm_idle>0)and(num_job_idle>=num_vm_idle):
            num_vm_busy = num_vm_busy+num_vm_idle
            num_job_idle = num_job_idle-num_vm_idle
            num_vm_idle = 0

        if (num_job_idle>0)and(num_vm_idle>0)and(num_job_idle<num_vm_idle):
            num_vm_busy = num_vm_busy+num_job_idle
            num_vm_idle = num_vm_idle-num_job_idle
            num_job_idle = 0
      
        num_vm_all = num_vm_busy+num_vm_idle
        print "num_vm_all_new"
        print repr(group_name_list)
        print num_vm_all
        print "num_vm_busy_new"
        print repr(group_name_list)
        print num_vm_busy
        print "num_vm_idle_new"
        print repr(group_name_list)
        print num_vm_idle
        print "num_job_idle_new"
        print repr(group_name_list)
        print num_job_idle

        MAX_VM = MAX_VM_TO_LAUNCH+num_vm_all
 
        if(num_vm_all<MIN_VM):
            print "num_vm_all<MIN_VM"
            if(num_vm_all+num_job_idle<MAX_VM):
                num_vm_to_launch = (num_job_idle if (num_job_idle+num_vm_all)>=MIN_VM else (MIN_VM-num_vm_all))
            else:
                num_vm_to_launch = MAX_VM-num_vm_all
            num_vm_to_destroy = 0

    
        if(num_vm_all > MAX_VM):
            print "num_vm_all > MAX_VM"
            num_vm_to_launch = 0
            if(num_vm_busy > MAX_VM):
                print "MAX_VM is %d, but num_vm_busy is %d. I should release some vms which run jobs for %s" % (MAX_VM,num_vm_busy,group_name_list)
                num_vm_to_destroy = num_vm_all-MAX_VM
            if(num_vm_busy > MIN_VM and num_vm_busy<=MAX_VM):
                num_vm_to_destroy = num_vm_idle
            else:
                num_vm_to_destroy = num_vm_all-MIN_VM

        if(num_vm_all>=MIN_VM) and (num_vm_all<=MAX_VM):
            print "(num_vm_all>MIN_VM) and (num_vm_all<MAX_VM)"
            if(num_job_idle > 0):
                num_vm_to_launch = (num_job_idle if num_job_idle<=(MAX_VM-num_vm_all) else (MAX_VM-num_vm_all))
            else:
                num_vm_to_launch = 0
	    num_vm_to_destroy = (num_vm_idle if num_vm_busy>=MIN_VM else (num_vm_all-MIN_VM))

        if(num_vm_to_launch > 0) and (num_vm_to_destroy>0):
            if(num_vm_to_destroy>=num_vm_to_launch):
                num_vm_to_destroy = num_vm_to_destroy-num_vm_to_launch 
                num_vm_to_launch = 0
            else:
                num_vm_to_launch = num_vm_to_launch-num_vm_to_destroy
                num_vm_to_destroy=0

        nova = OpenstackCluster.get_creds_nova()
        #free_vcpus = 0
        Host_free_vcpus_dict = {}
        for Host in config.GROUP_HOST_DICT[get_key_by_value(GROUP_DICT, group_name_list)]:
            try:
                all_vcpu = nova.hosts.get(Host)[0].to_dict()['resource']['cpu']
                used_vcpu = nova.hosts.get(Host)[1].to_dict()['resource']['cpu']
                free_vcpu = all_vcpu-used_vcpu
                Host_free_vcpus_dict[Host]=free_vcpu
            except Exception as e:
                logger.error("Unable to find vcpus for group %s: %s" % (group_name_list,e))
            except:
                logger.info("No physical machine for group %s ?" % group_name_list)
        print Host_free_vcpus_dict
        """ 
        if (num_vm_to_launch>free_vcpus):
            num_vm_to_launch = free_vcpus
            logger.error("Group %s vcpus_free is %s.There are not enough vcpus.I had to create vm on other group's physical machines" % (group_name_list,free_vcpus))
        """

	
	print "launch %d instance for group:%s!" % (num_vm_to_launch,group_name_list)
	print "destroy %d instance for group:%s!"% (num_vm_to_destroy,group_name_list)
        logger.error("launch %d instance for group:%s!" % (num_vm_to_launch,group_name_list))
        logger.error("destroy %d instance for group:%s!"% (num_vm_to_destroy,group_name_list))

        try:
            network_name_list = network_list.split('|')
            print "network_name_list is %s" % network_name_list
        except Exception,e:
            print e
            logger.error(e)
            return None

        num_vm_launched = 0
        CreateVmThread = []
        for vm_network in network_name_list:
            try:
                ip_quota = NETWORK_QUOTA_DICT[vm_network]-OpenstackCluster.num_vms_by_network(vms=vms_new,network=vm_network)
                print "net %s quota is %d" % (vm_network,ip_quota)
                num_vm_to_launch_by_net = (num_vm_to_launch if (num_vm_to_launch<=ip_quota) else ip_quota)
                print "num_vm_to_launch_by_net is %d" % num_vm_to_launch_by_net
                for num in range(0,num_vm_to_launch_by_net):
                    avail_zone='pvm'
                    for host in Host_free_vcpus_dict.keys():
                        if Host_free_vcpus_dict[host]>0:
                            avail_zone='pvm:'+host
                            Host_free_vcpus_dict[host] -= 1
                            break
                        else:
                            avail_zone='pvm'
                    t = threading.Thread(target=OpenstackCluster.vm_create,args=(vm_name,'',get_key_by_value(GROUP_DICT, group_name_list),image_name,flavor_uuid,avail_zone,vm_network,[],1,1))
                    CreateVmThread.append(t)
                    #OpenstackCluster.vm_create(vm_name=vm_name,resource_group_type='',group=get_key_by_value(GROUP_DICT, group_name_list),imageId=image_name,
                     #   instance_flavorId=flavor_uuid,availability_zone='pvm',vm_networkassoc=vm_network,max_count=1)
                num_vm_to_launch = num_vm_to_launch-num_vm_to_launch_by_net
                num_vm_launched = num_vm_launched+num_vm_to_launch_by_net
            except Exception as e:
                logger.error("Unable to lauch instances by method OpenstackCluster.vm_create for group %s." % '|'.join(group_name_list))
                logger.error(e)
                print e
        for tr in CreateVmThread:
            tr.start()
        for tr in CreateVmThread:
            tr.join()
        print "I have launched %d VMs" % num_vm_launched
                

        print 'can shu'
        print vm_name
        print image_name
        print flavor_uuid
        print network_list
        print group_name_list
        print INACTIVE_SET

        '''
        for num in range(0,num_vm_launched):
            try:
                print 'try to launch a VM'
                OpenstackCluster.vm_create(vm_name=vm_name,resource_group_type='',group='',imageId=image_name,
                    instance_flavorId=flavor_uuid,availability_zone='pvm',vm_networkassoc=network_list,max_count=1)
            except Exception as e:
                logger.error("Unable to lauch an instance by method OpenstackCluster.vm_create for group %s." % '|'.join(group_name_list))
                logger.error(e)
        '''

        if (num_vm_to_destroy>0 and num_vm_to_destroy<=num_vm_idle):
            try:
                OpenstackCluster.vm_to_g_none_by_Group_JobActivity(count=num_vm_to_destroy,group=group_name_list,activity=INACTIVE_SET,vms=vms_new)
                print "shut down %d vms which are idle!" % num_vm_to_destroy
            except Exception as e:
                logger.error("Unable to destroy %d instances by method OpenstackCluster.vm_to_g_none_by_Group_JobActivity for group %s." 
        			% (num_vm_to_destroy,'|'.join(group_name_list)))
                print e
        elif (num_vm_to_destroy>0 and num_vm_to_destroy>num_vm_idle):
            try:
                OpenstackCluster.vm_to_g_none_by_Group_JobActivity(count=num_vm_idle,group=group_name_list,activity=INACTIVE_SET,vms=vms_new)
                print "shut down %d vms which are idle!" % num_vm_idle
                OpenstackCluster.vm_to_g_none_by_Group_JobActivity(count=num_vm_to_destroy-num_vm_idle,group=group_name_list,activity=ACTIVE_SET,vms=vms_new)
                print "shut down %d vms which are busy!" % num_vm_to_destroy-num_vm_idle
            except Exception as e:
                logger.error("Unable to destroy %d instances by method OpenstackCluster.vm_to_g_none_by_Group_JobActivity for group %s." 
                    % (num_vm_to_destroy,'|'.join(group_name_list)))
                print e


        if config.exit=='true':
            exit = True
        else:
            exit = False

        end = time.time()
        if(end-start<300):
            print "I'm going to sleep %f seconds" % (300-(end-start))
            time.sleep(300-(end-start))
        else:
            time.sleep(0)

    return 0



def main():
    """Main Function of VRManager."""
    config.setup()

    global GROUP_SET
    global GROUP_DICT
    global IMAGE_DICT
    global FLAVOR_DICT
    global NETWORK_DICT
    global NETWORK_QUOTA_DICT
    global log_file
    global logger
    global MemLogger

    global _VRManagerAvail
    """Designed to make sure Administrator won't receive large amount of emails which describe the same problem by VCondor."""
    global _AbleToSendMail

    GROUP_SET = config.GROUP_SET
    GROUP_DICT = config.GROUP_DICT
    IMAGE_DICT = config.IMAGE_DICT
    FLAVOR_DICT = config.FLAVOR_DICT
    NETWORK_DICT = config.NETWORK_DICT
    NETWORK_QUOTA_DICT = config.NETWORK_QUOTA_DICT

    print config.log_file
    logger = logging.getLogger() 
    logger.setLevel(logging.DEBUG) 
    fh = logging.FileHandler(config.log_file)
    ch = logging.StreamHandler() 
    formatter = logging.Formatter('%(asctime)s  [%(levelname)s] : %(module)s - %(message)s')  
    fh.setFormatter(formatter)  
    ch.setFormatter(formatter) 
    logger.addHandler(fh)  
    logger.addHandler(ch) 

    MemLogger = logging.getLogger('MemLogger') 
    MemLogger.setLevel(logging.CRITICAL) 
    Memfh = logging.FileHandler('./log/MemLog.log')
    MemLogger.addHandler(Memfh)  

    #garbage collect
    gc.collect()
    MemLogger.critical(objgraph.show_growth())

    jc = None


    threads = []
    
    NoneGroupList = []
    NoneGroupList.append(config.NoneGroup)
    
    for group_name_list in GROUP_SET:
        print 'group_name_list is %s' % group_name_list
        if group_name_list==NoneGroupList:
            continue
        t = threading.Thread(target=loop_group,args=(jc,group_name_list,))
        threads.append(t)
    

    cleaner = Cleanup(NoneGroup='g_none')
    threads.append(cleaner)

    cleaner1 = Cleanup(NoneGroup='g_none1')
    threads.append(cleaner1)


    for tr in threads:
        tr.start()
    for tr in threads:
        tr.join()

    return 0


##
## Main Functionality
##

main()

    

