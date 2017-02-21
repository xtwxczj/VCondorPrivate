#!/usr/bin/env python
# ******************************************************
# Author       : Zhen-Jing CHENG
# Copyright (C)  2016 IHEP
# Last modified: 2016-11-07 17:18
# Email        : chengzj@ihep.ac.cn
# Filename     : RemoteCluster.py
# Description  : 
# ******************************************************

import os
import sys
import time
import uuid
import thread
import threading
import string
import shutil
import logging
import subprocess
import random
import shlex
import MySQLdb
from xmlrpclib import ServerProxy

import cluster_tools
import cloud_init_util
import config as config
import utilities as utilities
import JClient
from job_management import _attr_list_to_dict
from cStringIO import StringIO
import gzip

try:
    import novaclient.client as nvclient
    import novaclient.exceptions
except Exception as e:
    print "Unable to import novaclient - cannot use native openstack"
    print e
    sys.exit(1)

log = None


class RemoteCluster(cluster_tools.ICluster,cluster_tools.VM):
    global username
    global password
    vms = []
    vms_by_group = []
    vms_by_group_activity = []
    newVmUUID = {}

    ERROR = 1
    VM_STATES = {
            "BUILD" : "Starting",
            "ACTIVE" : "Running",
            "SHUTOFF" : "Shutdown",
            "SUSPENDED" : "Suspended",
            "PAUSED" : "Paused",
            "ERROR" : "Error",
    }
    security_groups = ['default']
    

    def __init__(self,username,password,tenant_id,auth_url,name='openstack cluster',):
    
        # Call super class's init
        cluster_tools.ICluster.__init__(self,name=name,username=username,password=password,tenant_id=tenant_id,auth_url=auth_url,)

        self.logger = logging.getLogger('main')
        self.fh = logging.FileHandler(config.log_file)
        self.fh.setLevel(logging.INFO)
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.INFO)
        self.formatter = logging.Formatter('%(asctime)s  [%(levelname)s] : %(module)s - %(message)s')
        self.fh.setFormatter(self.formatter)
        self.ch.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ch)

        self.LoggerForAdmin = logging.getLogger('ForAdmin')
        self.fhForAdmin = logging.FileHandler(config.admin_log_file)
        self.fhForAdmin.setLevel(logging.CRITICAL)
        self.fhForAdmin.setFormatter(self.formatter)
        self.LoggerForAdmin.addHandler(self.fhForAdmin)

        security_group = ["default"]
        self.security_group = security_group
        self.username = username if username else "admin"
        self.password = password if password else ""
        
    def sql_operate(self,sql):
        """ Execute some sql sentences."""
	try:
            d = {}
            d['host'] = os.environ['CLOUDFEE_DB_ADDR']
            d['user'] = os.environ['CLOUDFEE_DB_USER']
            d['passwd'] = os.environ['CLOUDFEE_DB_PWD']
            d['db'] = os.environ['CLOUDFEE_DB']
            conn = MySQLdb.connect(**d)
            cur = conn.cursor()
            cur.execute(sql) 
            conn.commit()
            cur.close()
            conn.close()
        except MySQLdb.Error as e:
            print e
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
            self.logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.logger.error(e)
	except Exception as e:
            print e	
            self.logger.error(e)    



    def __getstate__(self):
        pass

    def __setstate__(self, state):
        pass

    def get_vms_local(self):
        """ Create vms objects list from vm on Openstack at this moment."""
        pass

    def set_vms(self,vms):
        pass


    def clear_vms(self,vms):
        """ Try to clear the vm objects list.
        """
        del vms[:]

    def num_vms(self,vms):
        """Returns the number of VMs running on the cluster (in accordance
        to the vms[] list)
        """
        return len(vms)

    def num_vms_by_group(self,vms=[],group=[]):
        """Returns the number of VMS that run jobs of specific group.
        """
        vm_count = 0
        for vm in vms:
            if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
                vm_count += 1
        return vm_count

    def num_vms_by_group_activity(self,vms=[],group=[],activity=[]):
        """Returns the number of VMs that run jobs of specific group and
        specific activity.
        """
        vm_count = 0
        for vm in vms:
            #print vm.ipaddress, vm.group, vm.activity, group, activity
	    if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
	    	if (str(activity).find(vm.activity)>0 or str(vm.activity).find(str(activity))>0):
                    vm_count += 1
	    
        return vm_count

    def num_vms_by_network(self,vms=[],network=""):
        """Returns the number of VMS that belongs to a certain Network.
        """
        vm_count = 0
        for vm in vms:
            if (vm.network ==  network):
                vm_count += 1
        return vm_count

    def get_vm_by_id(self,uuid,vms):
        """Find the vm by id and return the vm object. 
        """
        for vm in vms:
            if str(vm.uuid) == uuid:
                return vm
        return None
    def get_vms_by_group(self,group=[],vms=[]):
        """Return the number of VMs which belongs to a certain VM.
        """
        vms_by_group = []
        for vm in vms:
            #if vm.group in group:
            if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
                vms_by_group.append(vm)
        return vms_by_group

    @staticmethod
    def get_vms_by_group_activity(group,activity,vms):
        """Return the number of VMs which belongs to a certain VM and a certain activity.
        """
        vms_by_group_activity = []
        for vm in vms:
            #if(repr(group).find(vm.group)>0):
            if(str(group).find(vm.group)>0 or str(vm.group).find(str(group))>0):
                #if (repr(activity).find(vm.activity)>0):
                if (str(activity).find(vm.activity)>0 or str(vm.activity).find(str(activity))>0):
                    vms_by_group_activity.append(vm)
        return vms_by_group_activity




    def vm_create(self,vm_name="", resource_group_type="", group="", imageId="",instance_flavorId="",
                availbility_zone="",vm_networkassoc="",securitygroup=[],min_count=1,max_count=1):
        """Submit a glidein job in remote site and try to hold a slot in HTCondor pool."""
        print 'submit a glidein job to remote site.'
        
        '''
        threads = []
        t = threading.Thread(target=self._vm_create,args=(nova,group,group+time.strftime('-%Y-%m-%dT%H:%M:%S'),imageobj,flavorobj,netid,availbility_zone))
        threads.append(t)
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        '''
        
        return 0
 
    def _vm_create(self, nova=None, group='', vm_name='', imageobj=None, flavorobj=None, netid=None, availbility_zone=''):
        instance = None
        print 'try to boot a new VM.'

        try:
            HostNum = len(config.GROUP_HOST_DICT[group])
        except Exception as e:
            HostNum = 0 
            print e

        """
        avail_zone = 'pvm'
        for Host in config.GROUP_HOST_DICT[group]:
            try:
                all_vcpu = nova.hosts.get(Host)[0].to_dict()['resource']['cpu']
                used_vcpu = nova.hosts.get(Host)[1].to_dict()['resource']['cpu']
                free_vcpu = all_vcpu-used_vcpu
                print 'free_vcpu is %s' % free_vcpu 
                if (free_vcpu>0):
                    avail_zone = 'pvm:'+Host
                    break
            except Exception as e:
                self.logger.error("Unable to find a Host for vm of group %s: %s" % (group,e))
                print e
                avail_zone = 'pvm'
        print 'avail_zone is %s' % avail_zone
        """
        self.logger.debug("Create a VM on host:%s for group:%s" % (availbility_zone,group))
        

        try:
            instance = nova.servers.create(name=vm_name,image=imageobj,flavor=flavorobj,nics=netid,availability_zone=availbility_zone,min_count=1,max_count=1)
        except novaclient.exceptions.OverLimit as e:
            self.logger.error("Unable to create VM without exceeded quota on %s: %s" % (self.name, e.message))
        except Exception as e:
            print e
            self.logger.error("Unhandled exception while creating vm on %s: %s" %(self.name, e))
            return -1
       
        if instance:
            time.sleep(180)	
            instance_id = instance.id
            instance = nova.servers.get(instance_id)
            print instance.interface_list()
            if len(instance.interface_list())==0:
                self.logger.error("New VM ID:%s cannot get a IP addr in 180 seconds.I had to shut it down!!" % instance_id)
                instance.delete()
                return -1

            try:
                temp_dict = getattr(instance, 'addresses')
                temp_list = temp_dict.keys()
                instance_ip = temp_dict[temp_list[0]][0]['addr']
                instance_mac = temp_dict[temp_list[0]][0]['OS-EXT-IPS-MAC:mac_addr']
            except Exception as e:
		print e
		instance_ip=""
		instance_mac=""
                self.logger.error("Unable to get ipaddress or macaddress from instance %s" % instance_id)
            #if not vm_keepalive and self.keep_alive: #if job didn't set a keep_alive use the clouds default
                    #vm_keepalive = self.keep_alive

            self.logger.info("Create an instance! %s %s " % (instance_id,instance_ip))
            time.sleep(10)
            try:
                cmd = 'curl \"http://vmctrl.ihep.ac.cn/set.php?h=%s&q=%s&s=S\"' % (instance.id,group)
                self.logger.error(cmd)
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
                returncode = sp.returncode
                self.logger.error("cmd:%s Out:%s" % (cmd,out))
                if (returncode!=0):
                    self.logger.error("Error running %s." % cmd)
                    sp = subprocess.Popen(cmd, shell=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (out, err) = sp.communicate(input=None)
                if (out!='%s S1' % group):
                    self.logger.error("Encounted an error when trying to communicate with vmctl. ip:%s cmd:%s . I had to shut it down" % (instance_ip,cmd))
                    self.LoggerForAdmin.critical('%s %s cmd:%s' % (instance_id,instance_ip,cmd))
                    instance.delete()
                    thread.exit_thread()
                    return 0
            except OSError:
                self.logger.error("OSError occured while doing condor_status - will try again next cycle.")
                return None
            except Exception, e:
                print e
                self.logger.exception("Problem running %s, unexpected error" % cmd)
                return None
            result = self._checkVmInCondor(vm_ip=instance_ip,group=group,tm=config.Timeout,HopeValue=0) 
            if(result!=0):
                self.logger.error("Attention,  administrator! Condor on VM IP:%s UUID:%s of group %s doesn't start to work in 10 minutes. \
                              I had to shut it down" % (instance_ip,instance_id,group))
                time.sleep(10)
                instance.delete()
                return 0
            else:
                result = self._checkVmInRightGroup(vm_ip=instance_ip, group=group, tm=config.Timeout)
                if(result!=0):
                    self.logger.error("Attention,administrator! Condor on NEW VM %s cannot change to group %s in 10 minutes.It's still in None Group!" % (instance_ip,group))
                    return 0

            try:
                cmd = '/usr/local/bin/check_nrpe -H 192.168.60.90 -c monitor_control -a "add %s %s %s" -t 15' \
                                    % (utilities.setHostnameByIp(instance_ip),instance_ip,group)           
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
                returncode = sp.returncode
                self.logger.info(cmd)
                sys.exit(1)
            except OSError:
                self.logger.error("OSError occured while doing cmd: %s"% cmd)
            except Exception, e:
                self.logger.exception("Problem running %s, unexpected error %s" % (cmd,e))

            #new_vm = cluster_tools.VM(name = vm_name, uuid=instance_id, 
                        #group = group, ipaddress = instance_ip, macaddress = instance_mac, network = vm_networkassoc, 
                        #image_name = imageobj.name, flavor = flavorobj.name)

            #self.vms.append(new_vm)
            '''
            try:
                cloud_name = os.environ['CLOUD_NAME']
                sql = "insert into group_vm_map set cloud_name='%s', vm_name='%s', vm_ip='%s', uuid='%s',group_name='%s', \
                        image_name='%s', vm_state='active', check_at=NOW()" % (cloud_name,vm_name,instance_ip,instance_id,group,imageobj.name)
                self.sql_operate(sql)
            except Exception as e:
                print e
            '''

            #return new_vm
        else:
            self.logger.error("Failed to create instance on %s" % self.name)
            return -1
        thread.exit_thread()


    def _checkVmInCondor(self, vm_ip='', group='', tm=600, HopeValue=0):
        """ Check a VM in condor pool or not."""
        cmd=out=err=""
        returncode = 1-HopeValue
        while (returncode!=HopeValue and tm>0):
            time.sleep(10)
            tm = tm-10
            try:
                cmd = config.condor_status_shortcmd[group] + " -format \"%%s\\n\" StartdIpAddr|grep '%s:'" % vm_ip
                print cmd
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
                returncode = sp.returncode
            except OSError:
                self.logger.error("OSError occured while doing condor_status - will try again next cycle.")
                return None
            except Exception, e:
                print e
                #self.logger.error("Problem running %s, unexpected error" % cmd)
                return None

        return returncode
            

    def _checkVmInRightGroup(self, vm_ip='', group='', tm=600):
        """ Check a VM's state of condor is in Right Group or not."""
        cmd=out=err=""
        returncode = -1
        while (returncode!=0 and tm>0):
            time.sleep(10)
            tm = tm-10
            try:
                cmd = config.condor_status_shortcmd[group] + " -format \"%%s@\" MyAddress -format \"%%s\\n\" Start |grep '%s:'|awk -F '@' '{print $2}'" % vm_ip
                print cmd
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
            except OSError:
                self.logger.error("OSError occured while doing condor_status - will try again next cycle.")
                return None
            except Exception, e:
                print e
                #self.logger.error("Problem running %s, unexpected error" % cmd)
                return None

            temp_group = out.split('"',-1)
            lenth = len(temp_group)
            num = lenth
            resource_group_list = []
            resource_group_type = ""
            group_list = []
            while (num>1):
                num -= 2
                resource_group_list.append(temp_group[num])
                resource_group_type = "|".join(str(i) for i in resource_group_list)
            group_list.append(resource_group_type)
            LocalGroup = utilities.get_key_by_value(config.GROUP_DICT,group_list)
            print '%s LocalGroup is %s,group is %s' % (vm_ip,LocalGroup,group)
            returncode = cmp(LocalGroup,group)
            self.logger.error('%s Local condor Start is %s,group is %s' % (vm_ip,group_list,config.GROUP_DICT[group]))
            if group_list==config.GROUP_DICT[group]:
                print '%s Local condor Start is %s,group is %s' % (vm_ip,group_list,config.GROUP_DICT[group])
                returncode = 0

        return returncode

 
    def vm_to_g_none_by_Group_JobActivity(self,count=0,group=[],activity=[],vms=[]):
        """ Destroy  VMs on Openstack which runs specific jobs in specifc activity of the group."""
        print group
        print activity
	vms_temp = OpenStackCluster.get_vms_by_group_activity(group,activity,vms)
        try:
            num_list = random.sample(range(len(vms_temp)),count)
        except Exception as e:
            print e
            self.logger.error("Encounting an error when trying to create some random numbers from 0 to %d." % len(vms_temp)-1)

        threads = []
        for number in num_list:
            try:
                #self.vm_destroy(vms_temp[number])
                #thread.start_new_thread(self.vm_destroy,(vms_temp[number],))
                t = threading.Thread(target=self.VM_to_g_none,args=(vms_temp[number],))
                threads.append(t)
            except Exception as e:
                print e
                self.logger.error("Encounting an error when trying to destroy some vms on Openstack which runs \
                            specific jobs in %s activity of the group %s." % (activity,group))
                return 0
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        return 1

    def VM_to_g_none(self,vm):
        """Move a VM to group g_none."""
        self.logger.info("Move a VM: %s Name: %s resource_group_type: %s group: %s to group g_none" % (vm.uuid,vm.name,vm.resource_group_type,vm.group))
        group_list = []
        group_list.append(vm.group.strip('\''))
        group = ''
        try:
            group = config.GROUP_DICT[config.GROUP_NONEGROUP_DICT[utilities.get_key_by_value(config.GROUP_DICT,group_list)]][0]
            cmd = 'curl \"http://vmctrl.ihep.ac.cn/set.php?h=%s&q=%s&s=%s\"' % (vm.uuid,group,'N')
            self.logger.error(cmd)
            self.logger.debug(cmd)
            print cmd
            sp = subprocess.Popen(cmd, shell=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (out, err) = sp.communicate(input=None)
            returncode = sp.returncode
            self.logger.error("cmd:%s Out:%s" % (cmd,out))
            if (returncode!=0):
                self.logger.error("Error running %s." % cmd)
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
            if (out!='%s %s1' % (group,'N')):
                self.logger.error("Encounted an error when trying to communicate with vmctl. ip:%s cmd:%s ." % (vm.ipaddress,cmd))
                self.LoggerForAdmin.critical('%s %s cmd:%s' % (vm.uuid,vm.ipaddress,cmd))
                time.sleep(10)
                thread.exit_thread()
                return 0
        except OSError:
            self.logger.error("OSError occured while doing curl.")
            return None
        except Exception, e:
            print e
            self.logger.error("Problem running %s, unexpected error" % cmd)
            return None
        result = self._checkVmInRightGroup(vm_ip=vm.ipaddress, 
                     group=config.GROUP_NONEGROUP_DICT[utilities.get_key_by_value(config.GROUP_DICT,group_list)], tm=600)
        print "result is %s" % result
        if (result!=0):
            self.logger.error("VM %s is not in right group. I hope it in group %s" % (vm.ipaddress,group))
            print "VM %s is not in right group. I hope it in group %s" % (vm.ipaddress,group)

        thread.exit_thread()


    def vm_destroy_by_Group_JobActivity(self,count=0,group=[],activity=[],vms=[]):
        """ Destroy  VMs on Openstack which runs specific jobs in specifc activity of the group."""
        print group
        print activity
	vms_temp = OpenStackCluster.get_vms_by_group_activity(group,activity,vms)
        print vms_temp
        try:
            num_list = random.sample(range(len(vms_temp)),count)
        except Exception as e:
            print e
            self.logger.error("Encounting an error when trying to create some random numbers from 0 to %d." % len(vms_temp)-1)

        threads = []
        for number in num_list:
            try:
                #self.vm_destroy(vms_temp[number])
                #thread.start_new_thread(self.vm_destroy,(vms_temp[number],))
                t = threading.Thread(target=self.vm_destroy,args=(vms_temp[number],group,))
                threads.append(t)
            except Exception as e:
                print e
                self.logger.error("Encounting an error when trying to destroy some vms on Openstack which runs \
                            specific jobs in %s activity of the group %s." % (activity,group))
                return 0
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        return 1


    def vm_destroy(self,vm,group):
        """ Destroy a VM on Openstack."""
        nova = self.get_creds_nova()
        import novaclient.exceptions
        #self.logger.info("Destroying VM: %s Name: %s resource_group_type: %s group: %s " % (vm.uuid,vm.name,vm.resource_group_type,vm.group))

        try:
            if vm.ipaddress in config.WhiteIpList:
                self.logger.error("Cannot delete this VM cause it's in VCondor white list.IP:%s" % vm.ipaddress)
                return 0
        except Exception, e:
            self.logger.error("Exception occured while trying to delete a VM.\n%s" % e)
            return 0

        if isinstance(group, list):
            group = group[0]
        try:
            cmd = 'curl \"http://vmctrl.ihep.ac.cn/set.php?h=%s&q=%s&s=C\"' % (vm.uuid,group)
            self.logger.error(cmd)
            self.logger.debug(cmd) 
            print cmd
            sp = subprocess.Popen(cmd, shell=True,
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (out, err) = sp.communicate(input=None)
            returncode = sp.returncode
            self.logger.error("cmd:%s Out:%s" % (cmd,out)) 
            if (returncode!=0):
                self.logger.error("Error running %s." % cmd)
                sp = subprocess.Popen(cmd, shell=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (out, err) = sp.communicate(input=None)
            if (out!='%s C1' % group):
                self.logger.error("Encounted an error when trying to communicate with vmctl. ip:%s cmd:%s ." % (vm.ipaddress,cmd))
                self.LoggerForAdmin.critical('%s %s cmd:%s' % (vm.uuid,vm.ipaddress,cmd))
                thread.exit_thread()
                return 0
        except OSError:
            self.logger.error("OSError occured while doing HTTP request to VMCTRL - will try again next cycle.")
            return None
        except Exception, e:
            print e
            self.logger.exception("Problem running %s, unexpected error" % cmd)
            return None

        print "vm.group is %s" % vm.group
        result = self._checkVmInCondor(vm_ip=vm.ipaddress,group=group,tm=config.Timeout,HopeValue=1) 
        if(result!=1):
            self.logger.error("Condor in vm IP:%s UUID:%s cannot be shutdown. So I cannot delete the VM." % (vm.ipaddress,vm.uuid))
            thread.exit_thread()
            return 1

        try:
            cmd = '/usr/local/bin/check_nrpe -H 192.168.60.90 -c monitor_control -a "del %s %s" -t 15'  \
                                % (utilities.setHostnameByIp(vm.ipaddress),vm.ipaddress)           
            sp = subprocess.Popen(cmd, shell=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (out, err) = sp.communicate(input=None)
            returncode = sp.returncode
            self.logger.info(cmd)
            #sys.exit(1)
        except OSError:
            self.logger.error("OSError occured while doing check_nrpe: %s" % cmd)
        except Exception, e:
            self.logger.exception("Problem running %s, unexpected error %s" % (cmd,e))

        try:
            instance = nova.servers.get(vm.uuid)
            time.sleep(10)
            instance.delete()
        except novaclient.exceptions.NotFound as e:
            self.logger.error("VM id: %s name: %s not found: removing from vrmanager" % (vm.uuid,vm.name))
        except:
            self.logger.error("Failed to log exception properly?")
            return 1

        #sql =  "update group_vm_map set vm_state = '%s' ,check_at=NOW() where uuid = '%s'" % ('deleted',vm.uuid)
        #self.sql_operate(sql)
	# Delete references to this VM
        try:
            with self.vms_lock:
                self.vms.remove(vm)
        except Exception as e:
            self.logger.error("Error removing from list: %s" % e)
            return 1
	
        #return 0
        thread.exit_thread()


    def vm_poll(self,vm):
        """Query Openstack for status information of VMs."""
        import novaclient.exceptions
        nova = self.get_creds_nova()
        instance = None
        try:
            instance = nova.servers.get(vm.uuid)
        except novaclient.exceptions.NotFound as e:
            self.logger.exception("VM %s not found : %s" % (vm.uuid,e))
            vm.status = self.VM_STATES['ERROR']
        except Exception as e:
            try:
                self.logger.error("Unexpected exception occurred polling vm %s: %s" % (vm.uuid, e))
            except:
                self.logger.error("Failed to log exception properly: %s" % vm.uuid)
        #find instance.status
        if instance and vm.status != self.VM_STATES.get(instance.status, "Starting"):
            vm.last_state_change = int (time.time())
            self.logger.debug("VM: %s.Changed from %s to %s." % (vm.name,vm.status, self.VM_STATES.get(instance.status, "Starting")))
        if instance and instance.status in self.VM_STATES.keys():
            vm.status = self.VM_STATES[instance.status]
        elif instance:
            vm.status = self.VM_STATES['ERROR']
        return vm.status
    
    '''        
    def sendIpaddr(self):
        jc1 = None
        print "hello"
        try:
            jc1 = JClient.JClient(host='192.168.86.9', port=27020, bufsize=1024, allow_reuse_addr=True)
            print jc1
            data_response = jc1.oneRequest(self.newVmIpaddr)
            print data_response
        except Exception,e:
            self.logger.error("Unable to create JClient object or connect to vr.")
            self.logger.error(e)
            print e
            #sys.exit(1)
    '''

    def vm_status_poll(self,vm):
        """Query Openstack for status information if a specific VM."""
        pass

        
    def get_creds_nova(self):
        """Get an auth token to Nova."""
        try:
            import novaclient.client as nvclient
            import novaclient.exceptions
        except Exception as e:
            print e
            self.logger.error(e)
            return 0
            #sys.exit(1)
        creds = self._get_nova_creds()
        nova = nvclient.Client('2',**creds)
	return nova
        """
        try:
            client = nvclient.Client('2',self.username,self.password,self.tenant_id,self.auth_url)
        except Exception as e:
            self.logger.error("Unable to create connection to %s: Reason: %s" % (self.name, e))
        return client
        """

    def _get_nova_creds(self):
        """Return a cred for function get_creds_nova. """
        d = {}
        d['username'] = os.environ['OS_USERNAME']
        d['api_key'] = os.environ['OS_PASSWORD']
        d['auth_url'] = os.environ['OS_AUTH_URL']
        d['project_id'] = os.environ['OS_TENANT_NAME']
        return d

    @staticmethod
    def get_mysql_conn(self):
	""" Get an auth conn to mysql."""
	try:
	    creds = self._get_mysql_creds()
	    conn = MySQLdb.connect(**creds)
	    return conn
	except MySQLdb.Error,e:
	    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	    self.logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
	except Exception as e:
	    print e
	    self.logger.error(repr(e))

    @staticmethod
    def _get_mysql_creds(self):
	""" Return a cred for function get_mysql_conn()."""
        d = {}
	d['host'] = os.environ['CLOUDFEE_DB_ADDR']
	d['user'] = os.environ['CLOUDFEE_DB_USER']
	d['passwd'] = os.environ['CLOUDFEE_DB_PWD']
	d['db'] = os.environ['CLOUDFEE_DB']
	return d

    def _find_network(self, name):
        nova = self.get_creds_nova()
        network = None
        try:
            networks = nova.networks.list()
            for net in networks:
                if net.label == name:
                    network = net
        except Exception as e:
            self.logger.error("Unable to list networks on %s Exception: %s" % (self.name, e))
        return network





 
        
