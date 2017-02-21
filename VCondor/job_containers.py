from abc import ABCMeta, abstractmethod
from collections import defaultdict
import time
import threading
import logging
import config as config

# Use this global variable for logging
logging.basicConfig(filename=config.log_file,format='%(asctime)s  [%(levelname)s] : %(module)s - %(message)s',level=logging.ERROR)
log = utilities.get_vrmanager_logger()

#
# This is an abstract base class; do not intantiate directly.
#
# API documentation should go in here, as opposed to writing specific
# documentation for each concrete subclasses.
#
class Jobcontainer():
    __metaclass__ = ABCMeta

    # Use this lock if you require to threadsafe an operation.
    lock = None
    ## Condor Job Status mapping
    job_status_list = ['NEW', 'IDLE', 'RUNNING', 'REMOVED', 'COMPLETE', 'HELD', 'ERROR']
    def __init__(self):
        self.lock = threading.RLock()
        global log
        log = logging.getLogger("vrmanager")
        pass

    # Get a job by job id.
    # Return the job with the given job id, or None if the job does not exist in the container.
    @abstractmethod
    def get_job_by_id(self, jobid):
        pass

    # Add a job to the container.
    # If the job already exist, it will be replaced.
    @abstractmethod
    def add_job(self, job):
        pass

    # Remove a single job form the container.
    # If the job does not exist in the container, then nothing is done.
    @abstractmethod
    def remove_job(self, job):
        pass


    # Updates the status and remote host of a job (job.job_status attribute) 
    # in the container.
    # Returns True if the job was found in the container, False otherwise.
    @abstractmethod
    def update_job_status(self, jobid, status, remote):
        pass


#
# This class implements a job container based on hash tables.
#
class HashTableJobContainer(JobContainer):
    # class attribute
    all_jobs = None
    idle_jobs = None
    new_jobs = None
    jobs_by_group = None

    # constructor
    def __init__(self):
        JobContainer.__init__(self)
        self.all_jobs = {}
        self.idle_jobs = {}
        self.running_jobs = {}
        self.jobs_by_group = defaultdict(dict)
        log.verbose('HashTableJobContainer instance created.')

    # methods
    def __str__(self):
        return 'HashTableJobContainer [# of jobs: %d (running: %d idle %d) ]' % (len(self.all_jobs),
                    len(self.all_jobs)-len(self.idle_jobs),len(self.idle_jobs))
    
    def has_job(self, jobid):
        pass

    # Get a job by job id.
    # Return the job with the given job id, or None if the job does not exist in the container.
    def get_job_by_id(self, jobid):
        pass



    def update_job_status(self, jobid, resource_group_type, group, status, remote):
        pass
    



    def add_job(self,job):
        with self.lock:
            self.all_jobs[job.id] = job
            self.jobs_by_group[job.group][job.id] = job

            # Update scheduled/unscheduled maps too:
            if(job.status == "idle"):
                self.idle_jobs[job.id] = job
            else:
                self.sched_jobs[job.id] = job 

    def remove_job(self, job):
        with self.lock:
            if job.id in self.all_jobs:
                del self.all_jobs[job.id]
            if job.Group in self.jobs_by_group and (job.id in self.jobs_by_group[job.Group]):
                del self.jobs_by_group[job.Group][job.id]
                if len(self.jobs_by_group[job.Group]) == 0:
                    del self.jobs_by_group[job.Group]
            if job.id in self.idle_jobs:
                del self.idle_jobs[job.id]
            if job.id in self.running_jobs:
                del self.running_jobs[job.id]
            #log.debug('job %s removed from container' % job.id)



