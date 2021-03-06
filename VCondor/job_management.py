#!/user/bin/env python
# vim:set expandtab ts=4 sw=4:

# Copyright (C) 2016 IHEP-CC

## Auth: Cheng ZhenJing. 7/3/2016

import os
import re
import sys
import shlex
import string
import logging
import datetime
import threading
import subprocess
from urllib2 import URLError
from StringIO import StringIO
from collections import defaultdict

import config as config
from utilities import determine_path
from utilities import splitnstrip
import utilities
from decimal import *


##
## LOGGING
##

log = None

def get_key_by_value(DICT,VALUE):
    """Return key by a certain value in a dictionary."""
    for key, val in DICT.items():
        if (cmp(repr(val),repr(VALUE))==0):
            return key
    log.error('Error:%s is not in DICT:%s' % (repr(VALUE),repr(DICT)))
    return None

##
## CLASSES
##

class Job:
    """
    Job Class - Represents a job as read from vrmanager


    """
    # A list of possible statuses for internal job representation
    RUNNING = "running"
    IDLE = "idle"
    statuses = (IDLE,RUNNING,0,0,0,0)

    def __init__(self, GlobalJobId="None", Owner="Default-User", Group="Default-Group", JobPrio=1,
            JobStatus=0, **kwargs):
        """
        Parameters:
        GlobalJobID  - (str) The ID of the job (via condor). Functions as name.
        Owner       - (str) The user that submitted the job to Condor
        Group       - (str) The group that the user belongs to
        JobPrio   - (int) The priority given in the job submission file (default = 1)
        JobStatus   - (int) The status of the job.(default = 0, idle = 1,running = 2)
        """

        global log
        log_file = "/var/lib/vrmanager/VRManager/log/job_management.log"
        logging.basicConfig(filename=log_file,format='%(asctime)s  [%(levelname)s] : %(module)s - %(message)s',level=logging.ERROR)
	log = utilities.get_vrmanager_logger()

        self.id = GlobalJobId
        self.Owner = Owner
        self.Group = Group
        self.JobPrio = int(JobPrio)
        self.JobStatus = int(JobStatus)

        """
        try:    
            self.high_priority = int(VMHighPriority)
        except:
            log.exception("VMHighPriority not int: %s" % VMHighPriority)
            raise ValueError
        """


        # Set the new job's status
        if self.JobStatus == 2:
            self.status = self.statuses[0]
        else:
            self.status = self.statuses[1]

    def __repr__(self):
        return "Job '%s' Status '%s'" % (self.id,self.JobStatus)

    

    def log(self):
        """Log a short string representing the job."""
        log.info("Job ID: %s, Owner: %s, Group: %s, JobPrio: %s, JobStatus: %s" % (self.id,self.Owner,self.Group,self.JobPrio,self.JobStatus))


    @staticmethod
    def get_id(self):
        """Return the job id (Condor job id)."""
        return self.id

    def get_priority(self):
        """Return the condor job priority of the job."""
        return self.priority

    def set_status(self, status):
        """Sets the job's status to the given string
        Parameters:
            status   - (str) A string indicating the job's new status.
        
        Note: Status must be one of Scheduled, Unscheduled
        """
        if (status not in self.statuses):
            log.error("Error: incorrect status '%s' passed. Status must be one of: %s" % (status, "".join(self.statuses, ", ")))
            return
        self.status = status

    
class JobPool(Job):
    """ A pool of all jobs. Stores all jobs until they complete. Keeps running and idle jobs.
    """

    # The job container that will hold and maintain the job instances.    
    job_container = None

    ## Condor Job Status mapping
    NEW      = 0
    IDLE     = 1
    RUNNING  = 2
    REMOVED  = 3
    COMPLETE = 4
    HELD     = 5
    ERROR    = 6

    jobs = []

    def __init__(self, name, condor_query_type=""):
        """Constructor for JobPool class
        
        Keyword arguments:
        name              - The name of the job pool being created
        condor_query_type - The method to use for querying condor
        job_query_type - The type to use for querying condor job
        
        """

        global log
        log = logging.getLogger("vrmanager")
        log.debug("New JobPool %s created" % name)

        self.name = name
        self.write_lock = threading.RLock()


        job_query_type = config.job_query_type[name]

        if job_query_type.lower() == "simplify":
            self.job_query = self.job_query_simplify
            self.get_jobcount_by_group_activity = self.get_jobcount_by_group_activity_simplify

        elif job_query_type.lower() == "detail":
            self.job_query = self.job_query_detail
            self.get_jobcount_by_group_activity = self.get_jobcount_by_group_activity_detail

        else:
            log.error("Can't use '%s' retrieval method. Using simplify method." % job_query_type)
            self.job_query = self.job_query_simplify
            self.get_jobcount_by_group_activity = self.get_jobcount_by_group_activity_simplify


    def get_jobcount_by_group_activity_detail(self, jobs, Group, JobStatus):
        """Return the number of jobs which are in certain status. """
        job_count = 0
        try:
            for job in jobs:
                if str(Group).find(str(job.Group))>0:
                    if(job.JobStatus>0):
                        if(Job.statuses[job.JobStatus-1]==JobStatus):
                            job_count += 1		
            return job_count
        except:
            return 0

    def get_jobcount_by_group_activity_simplify(self, jobs, Group, JobStatus):
        """Return the number of jobs which are in certain status. """
        job_count = 0
        group_list = Group
        cmd = ""
        try:
            cmd = config.job_query_cmd[config.condor_pool[get_key_by_value(config.GROUP_DICT, Group)]]+' -format \'%s \' JobStatus -format \'%s\\n\' AcctGroup'
        except Exception, e:
            print e
            return 0

        temp = ""
        try:
            for group in group_list:
                temp = '\|'.join (group.split('|'))
                #for sub_group in group.split('|'):
                    #temp = temp + '%s\|' % sub_group
        except:
            return 0

        cmd = cmd + '|grep \'%s\'' % temp 
        job_status = Job.statuses.index(JobStatus)+1
        cmd = cmd + '|grep %s' % job_status + '|wc -l'

        log.info("Querying job with command:%s" % cmd)
        try:
            sp = subprocess.Popen(cmd, shell=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (out, err) = sp.communicate(input=None)
            returncode = sp.returncode
            return int(out)
            #if out.isdigit():
                #return out
            #else:
                #log.error("Problem running command %s.\n Error:%s" % (cmd,out))
                #return 0
        except Exception, e:
            log.error("Problem running command %s: e" % (cmd,e))
            return 0
                      
    def job_query_simplify(self,group):
        """job_query_detail -- query and parse condor_q for job information."""
        pass

    def job_query_detail(self,group):
        """job_query_detail -- query and parse condor_q for job information."""
        log.verbose("Querying jobs with %s" % config.condor_q_command)
        try:
            condor_q = config.condor_q_command[group]
            print "condor_q is %s" % condor_q
            sp = subprocess.Popen(condor_q, shell=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (condor_out, condor_err) = sp.communicate(input=None)
            print condor_err
            returncode = sp.returncode
        except Exception, e:
            print e
            log.exception("Problem running %s, unexpected error %s" % (string.join(config.condor_q_command[group], " "),e))
            return None

        if returncode != 0:
            log.error("Got non-zero return code '%s' from '%s'. stderr was: %s" %
                              (returncode, string.join(condor_q, " "), condor_err))
            return None
        
        job_ads = self._condor_q_to_job_list(condor_out)
        self.last_query = datetime.datetime.now()
        return job_ads
    
    @staticmethod
    def _condor_q_to_job_list(condor_q_output):
        """
        _condor_q_to_job_list - Converts the output of condor_q
                to a list of Job Objects
                returns [] if there are no jobs
        """
    
        jobs = []

        # The first three lines look like:
        # \n\n\t-- Submitter: hostname : <ip> : hostname
        # we can just strip these.
        condor_q_output = re.sub('\n\n.*?Submitter:.*?\n', "", condor_q_output, re.MULTILINE)

        # Each classad is seperated by '\n\n'
        raw_job_classads = condor_q_output.split("\n\n")
        # Empty condor pools give us an empty string in our list
        raw_job_classads = filter(lambda x: x != "", raw_job_classads)

        for raw_classad in raw_job_classads:
            classad = {}
            classad_lines = raw_classad.splitlines()
            for classad_line in classad_lines:
                classad_line = classad_line.strip()
                try:
                    (classad_key, classad_value) = classad_line.split(" = ",1)
                except Exception, e:
                    print e
                    log.error("Failed to unpack results from line: %s" % classad_line)
                    continue
                classad_value = classad_value.strip('"')
                
                if classad_key in ["Owner","GlobalJobId","JobPrio","JobStatus","RemoteHost","JobStartDate","ServerTime"]:
                    classad[classad_key] = classad_value
		if classad_key in ["AcctGroup"]:
		    classad["Group"] = classad_value

            try:            
                jobs.append(Job(**classad))
            except ValueError:
                log.exception("Failed to add job: %s due to Value Errors in jdl." % classad["GlobalJobId"])
            except:
                log.exception("Failed to add job: %s due to unspecified exception." % classad["GlobalJobId"])
                                    
        return jobs

def _attr_list_to_dict(attr_list):
    """
    _attr_list_to_dict -- parse a string like: host:ami, ..., host:ami into a
    dictionary of the form:
    {
        host: ami
        host: ami
    }
    if the string is in the form "ami" then parse to format
    {
        default: ami
    }
    raises ValueError if list can't be parsed
    """

    attr_dict = {}
    for host_attr in attr_list.split(","):
        host_attr = host_attr.split(":")
        if len(host_attr) == 1:
            attr_dict["default"] = host_attr[0].strip()
        elif len(host_attr) == 2:
            attr_dict[host_attr[0].strip()] = host_attr[1].strip()
        else:
            raise ValueError("Can't split '%s' into suitable host attribute pair" % host_attr)

    return attr_dict
