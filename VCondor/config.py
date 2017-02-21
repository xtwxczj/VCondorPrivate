#!/usr/bin/env python
# vim:set expandtab ts=4 sw=4:

# Copyright (C) 2016 IHEP-CC

## Auth: Cheng ZhenJing. 7/3/2016

import os
import sys
from urlparse import urlparse
import ConfigParser

import utilities

# VRManager Options Module
# Set default valuse
condor_retrieval_method = {}
job_query_type = {}
job_query_cmd = {}
condor_pool = {}
condor_q_command = {}
condor_status_command = {}
condor_status_shortcmd = {}
Timeout = 600
cleanup_interval = 600
NoneGroup = 'g_none'
NoneGroup1 = 'g_none'
DEBUG_MODE = True

client_loop_interval = {}
log_level = ""
log_file = ""
admin_log_file = ""

exit = 'false'
vm_lifetime = 10080
vm_poller_interval = 5
job_poller_interval = 5
machine_poller_interval = 5
scheduler_interval = 5
vm_start_running_timeout = -1 # Unlimited time
vm_idle_threshold = 5 * 60 # 5 minute default
max_starting_vm = -1
max_destroy_threads = 10
max_keepalive = 60 * 60  # 1 hour default
log_location = None
log_format = "%(asctime)s - %(levelname)s - %(threadName)s - %(message)s"


WhiteIpList = []
GROUP_SET = []
GROUP_DICT = {}
GROUP_NONEGROUP_DICT = {}
IMAGE_DICT = {}
FLAVOR_DICT = {}
NETWORK_DICT = {}
NETWORK_QUOTA_DICT = {}
GROUP_HOST_DICT = {}


FormatKeysListSend = []
FormatTypeDictSend = {}
FormatKeysListRecv = []
FormatTypeDictRecv = {}



"fix case problem"
class MyConfigParser(ConfigParser.ConfigParser):  
    def __init__(self,defaults=None):  
        ConfigParser.ConfigParser.__init__(self,defaults=None)  
    def optionxform(self, optionstr):  
        return optionstr 


def condor_pool_config(config_file=None,condor=None):
    """setup condor pool using config file and condor name
    """
    global job_query_type
    global job_query_cmd

    if config_file.has_option(condor,"job_query_type"):
        job_query_type[condor] = config_file.get(condor,
                                                "job_query_type")
    else:
        print "Configuration file problem: %s: job_query_type OPTION must be " \
                  "configured." % condor
        sys.exit(1)

    if config_file.has_option(condor,"job_query_cmd"):
        job_query_cmd[condor] = config_file.get(condor,
                                                "job_query_cmd")
    else:
        print "Configuration file problem: %s: job_query_cmd OPTION must be " \
                  "configured." % condor
        sys.exit(1)


def resource_group_config(config_file=None,group=None):
    """setup condor command using config file and resource group name
    """
    global condor_retrieval_method
    global condor_pool
    global condor_q_command
    global condor_status_command
    global condor_status_shortcmd
    global client_loop_interval

    global vm_lifetime
    global cleanup_interval
    global vm_poller_interval
    global job_poller_interval
    global machine_poller_interval
    global scheduler_interval
 
    if config_file.has_option(group,"condor_retrieval_method"):
        condor_retrieval_method[group] = config_file.get(group,
                                                "condor_retrieval_method")
    else:
        print "Configuration file problem: %s: condor_retrieval_method OPTION must be " \
                  "configured." % group
        sys.exit(1)

    if config_file.has_option(group,"condor_pool"):
        condor_pool[group] = config_file.get(group,
                                                "condor_pool")
    else:
        print "Configuration file problem: %s: condor_pool OPTION must be " \
                  "configured." % group
        sys.exit(1)

    if config_file.has_option(group, "condor_q_command"):
        condor_q_command[group] = config_file.get(group,
                                                "condor_q_command")
    else:
        print "Configuration file problem: %s: condor_q_command SECTION must be " \
                  "configured." % group
        sys.exit(1)


    if config_file.has_option(group, "condor_status_command"):
        condor_status_command[group] = config_file.get(group,
                                                "condor_status_command")
    else:
        print "Configuration file problem: %s: condor_status_command SECTION must be " \
                  "configured." % group
        sys.exit(1)


    if config_file.has_option(group, "condor_status_shortcmd"):
        condor_status_shortcmd[group] = config_file.get(group,
                                                "condor_status_shortcmd")
        print condor_status_shortcmd
    else:
        print "Configuration file problem: %s: condor_status_shortcmd SECTION must be " \
                  "configured." % group
        sys.exit(1)


    if config_file.has_option(group, "client_loop_interval"):
        try:
            client_loop_interval[group] = config_file.getint(group, "client_loop_interval")
        except ValueError:
            print "Configuration file problem: client_loop_interval must be an " \
                  "integer value."
            sys.exit(1)
    else:
        print "Configuration file problem: %s: client_loop_interval SECTION must be " \
                  "configured." % group
        sys.exit(1)



def setup(path=None):
    """setup VCondor using config file
    setup will look for a configuration file specified on the command line
    or in ~/.VCondor.conf or /etc/VCondor.conf
    """

    global Timeout
    global cleanup_interval
    global None_Group
    global DEBUG_MODE

    global exit
    global log_level
    global log_location
    global log_file
    global admin_log_file
    global log_location_cloud_admin
    global admin_log_comments
    global log_stdout
    global log_syslog
    global log_max_size
    global log_format

    global WhiteIpList
    global GROUP_SET
    global GROUP_DICT
    global GROUP_NONEGROUP_DICT
    global IMAGE_DICT
    global FLAVOR_DICT
    global NETWORK_DICT
    global NETWORK_QUOTA_DICT
    global GROUP_HOST_DICT

    global FormatKeysListSend
    global FormatTypeDictSend
    global FormatKeysListRecv
    global FormatTypeDictRecv




    homedir = os.path.expanduser('~')

    #Find config file
    if not path:
        if os.path.exists(homedir + "/.VRManager/VCondor.conf"):
            path = homedir + "/.VRManager/VCondor.conf"
            print path
        elif os.path.exists("/etc/VRManager/VCondor.conf"):
            path = "/etc/VRManager/VCondor.conf"
            print path
        elif os.path.exists("/usr/local/share/VRManager/VCondor.conf"):
            path = "/usr/local/share/VRManager/VCondor.conf"
            print path
        elif os.path.exists("./VCondor.conf"):
            path = "./VCondor.conf"
            print path
        else:
            print >> sys.stderr, "Configuration file problem: There doesn't " \
                  "seem to be a configuration file. " \
                  "You can specify one with the --config-file parameter, " \
                  "or put one in ~/.VRManager/VCondor.conf or "\
                  "/etc/VRManager/VCondor.conf"
            sys.exit(1)

    #Read config file
    config_file = MyConfigParser()
    try:
        config_file.read(path)
    except IOError:
        print >> sys.stderr, "Configuration file problem: There was a " \
              "problem reading %s. Check that it is readable," \
              "and that it exists. " % path
        raise
    except ConfigParser.ParsingError:
        print >> sys.stderr, "Configuration file problem: Couldn't " \
              "parse your file. Check for spaces before or after variables."
        raise
    except:
        print "Configuration file problem: There is something wrong with " \
              "your config file."
        raise


    _group_list = []
    if config_file.has_section("GroupDict"):
        _group_list = config_file.options("GroupDict")

    try:
        for _group in _group_list:
            resource_group_config(config_file,_group)
    except Exception, e:
        print e
        sys.exit(1)


    if config_file.has_section("CondorPool"):
        _condor_list = config_file.options("CondorPool")

    try:
        for _condor in _condor_list:
            condor_pool_config(config_file,config_file.get("CondorPool",_condor))
    except Exception, e:
        print e
        sys.exit(1)

    '''
    if config_file.has_option("global", "scheduler_interval"):
        try:
            scheduler_interval = config_file.getint("global", "scheduler_interval")
        except ValueError:
            print "Configuration file problem: scheduler_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "vm_poller_interval"):
        try:
            vm_poller_interval = config_file.getint("global", "vm_poller_interval")
        except ValueError:
            print "Configuration file problem: vm_poller_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "job_poller_interval"):
        try:
            job_poller_interval = config_file.getint("global", "job_poller_interval")
        except ValueError:
            print "Configuration file problem: job_poller_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "machine_poller_interval"):
        try:
            machine_poller_interval = config_file.getint("global", "machine_poller_interval")
        except ValueError:
            print "Configuration file problem: machine_poller_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "cleanup_interval"):
        try:
            cleanup_interval = config_file.getint("global", "cleanup_interval")
        except ValueError:
            print "Configuration file problem: cleanup_interval must be an " \
                  "integer value."
            sys.exit(1)
    '''


    if config_file.has_option("global", "Timeout"):
        try:
            Timeout = config_file.getint("global", "Timeout")
        except:
            print "Configuration file problem: global Timeout must be an " \
                  "integer value."
            sys.exit(1)
 
    if config_file.has_option("global", "cleanup_interval"):
        try:
            cleanup_interval = config_file.getint("global", "cleanup_interval")
        except ValueError:
            print "Configuration file problem: cleanup_interval must be an " \
                  "integer value."
            sys.exit(1)

    if config_file.has_option("global", "nonegroup"):
        try:
            NoneGroup = config_file.get("global", "nonegroup")
        except:
            print "Configuration file problem: nonegroup " 
            sys.exit(1)

    if config_file.has_option("global", "DEBUG_MODE"):
        try:
            DEBUG_MODE = config_file.get("global", "DEBUG_MODE")
        except:
            print "Configuration file problem: DEBUG_MODE " 
            sys.exit(1)
 
    if config_file.has_option("global", "exit"):
        try:
            exit = config_file.get("global", "exit")
        except:
            print "Configuration file problem: exit " 
            sys.exit(1)


    # Default Logging options
    if config_file.has_option("logging", "log_level"):
        log_level = config_file.get("logging", "log_level")

    if config_file.has_option("logging", "log_location"):
        log_location = os.path.expanduser(config_file.get("logging", "log_location"))

    if config_file.has_option("logging", "log_file"):
        log_file = os.path.expanduser(config_file.get("logging", "log_file"))

    if config_file.has_option("logging", "admin_log_file"):
        admin_log_file = os.path.expanduser(config_file.get("logging", "admin_log_file"))


    if config_file.has_option("logging", "log_format"):
        log_format = config_file.get("logging", "log_format", raw=True)


    # Group options
    if config_file.has_section("GroupDict"):
        _group_list = config_file.options("GroupDict")
        print "list is %s" % _group_list
    else:
        _group_list = []

    if (_group_list!=[]):

        GROUP_SET = []
        GROUP_DICT = {}
        for _ResourceGroup in _group_list:
            _Group = config_file.get("GroupDict", _ResourceGroup)
            temp = []
            temp.append(_Group)
            GROUP_DICT[_ResourceGroup] = temp
            GROUP_SET.append(temp)
        print 'GROUP_SET is %s' % GROUP_SET
        print GROUP_DICT

    else:
        print "Configuration file problem: GroupDict must be " \
                  "configured."
        sys.exit(1)

    # Group NoneGroup Map options
    if config_file.has_section("GroupMap"):
        _group_list = config_file.options("GroupMap")
        print "list is %s" % _group_list
    else:
        _group_list = []

    if (_group_list!=[]):
        GROUP_NONEGROUP_DICT = {}
        for _ResourceGroup in _group_list:
            _Group = config_file.get("GroupMap", _ResourceGroup)
            GROUP_NONEGROUP_DICT[_ResourceGroup] = _Group
        print GROUP_NONEGROUP_DICT

    else:
        print "Configuration file problem: GroupMap must be " \
                  "configured."
        sys.exit(1)

        

    # Image options
    if config_file.has_section("Image"):
        _group_list = config_file.options("Image")
    else:
        _group_list = []

    if (_group_list!=[]):

        IMAGE_DICT = {}
        for _ResourceGroup in _group_list:
            _Image = config_file.get("Image", _ResourceGroup)
            IMAGE_DICT[_ResourceGroup] = _Image
        print IMAGE_DICT

    else:
        print "Configuration file problem: IMAGE SECTION must be " \
                  "configured."
        sys.exit(1)


    # Flavor options
    if config_file.has_section("Flavor"):
        _group_list = config_file.options("Flavor")
    else:
        _group_list = []

    if (_group_list!=[]):

        FLAVOR_DICT = {}
        for _ResourceGroup in _group_list:
            _Flavor = config_file.get("Flavor", _ResourceGroup)
            FLAVOR_DICT[_ResourceGroup] = _Flavor
        print FLAVOR_DICT

    else:
        print "Configuration file problem: FLAVOR SECTION must be " \
                  "configured."
        sys.exit(1)


    # Network options
    if config_file.has_section("Network"):
        _group_list = config_file.options("Network")
    else:
        _group_list = []

    if (_group_list!=[]):

        NETWORK_DICT = {}
        for _ResourceGroup in _group_list:
            _Network = config_file.get("Network", _ResourceGroup)
            NETWORK_DICT[_ResourceGroup] = _Network
        print NETWORK_DICT

    else:
        print "Configuration file problem: NETWORK SECTION must be " \
                  "configured."
        sys.exit(1)


    # Network quota
    if config_file.has_section("Network-quota"):
        _netlist = config_file.options("Network-quota")
    else:
        _netlist = []

    if (_netlist!=[]):

        NETWORK_QUOTA_DICT = {}
        for _net in _netlist:
            _quota = 0
            try:
                _quota = config_file.getint("Network-quota",_net)
            except ValueError, e:
                print "Configuration file problem: Network quota must be an " \
                  "integer value."
                sys.exit(1)
            NETWORK_QUOTA_DICT[_net] = _quota
        print NETWORK_QUOTA_DICT

    else:
        print "Configuration file problem: NETWORK-Quota SECTION must be " \
                  "configured."
        sys.exit(1)


    # Group host options
    if config_file.has_section("Host"):
        _group_list = config_file.options("Host")
    else:
        _group_list = []

    if (_group_list!=[]):

        GROUP_HOST_DICT = {}
        for _ResourceGroup in _group_list:
            try:
                _Host_list = config_file.get("Host", _ResourceGroup).split('|')
                GROUP_HOST_DICT[_ResourceGroup] = _Host_list
            except Exception as e:
                print e
                sys.exit(1)

        print GROUP_HOST_DICT

    else:
        print "Configuration file problem: HOST SECTION must be " \
                  "configured."
        sys.exit(1)


    # White IP list options
    if config_file.has_option("WhiteIpList", "KeptIP"):
        try:
            WhiteIpList = config_file.get("WhiteIpList", "KeptIP").split('|')
            print WhiteIpList
        except:
            print "Configuration file problem: WhiteIpList[KeptIP] " 
            sys.exit(1)

    # JSONFormatCheck
    if config_file.has_section("JSONFormatCheck-SentToQuotaControl"):
        if "FormatKeysList" not in config_file.options("JSONFormatCheck-SentToQuotaControl"):
            print "Configuration file problem: FormatKeysList OPTION must be " \
                  "configured IN SECTION JSONFormatCheck-SentToQuotaControl."
            sys.exit(1)

        try:
            FormatKeysListSend = config_file.get("JSONFormatCheck-SentToQuotaControl", "FormatKeysList").split(',')
            for key in FormatKeysListSend:
                _type = config_file.get("JSONFormatCheck-SentToQuotaControl", key)
                FormatTypeDictSend[key] = _type
            print FormatKeysListSend
            print FormatTypeDictSend
        except Exception, e:
            print e
            print "Error: Cannot get information from JSONFormatCheck-SentToQuotaControl in config file"
            sys.exit(1)

    else:
        print "Configuration file problem: JSONFormatCheck-SentToQuotaControl SECTION must be " \
                  "configured."
        sys.exit(1)


    if config_file.has_section("JSONFormatCheck-RecvFromQuotaControl"):
        if "FormatKeysList" not in config_file.options("JSONFormatCheck-RecvFromQuotaControl"):
            print "Configuration file problem: FormatKeysList OPTION must be " \
                  "configured IN SECTION JSONFormatCheck-RecvFromQuotaControl."
            sys.exit(1)

        try:
            FormatKeysListRecv = config_file.get("JSONFormatCheck-RecvFromQuotaControl", "FormatKeysList").split(',')
            for key in FormatKeysListRecv:
                _type = config_file.get("JSONFormatCheck-RecvFromQuotaControl", key)
                FormatTypeDictRecv[key] = _type
            print FormatKeysListRecv
            print FormatTypeDictRecv
        except Exception, e:
            print e
            print "Error: Cannot get information from JSONFormatCheck-RecvFromQuotaControl in config file"
            sys.exit(1)

    else:
        print "Configuration file problem: JSONFormatCheck-RecvFromQuotaControl SECTION must be " \
                  "configured."
        sys.exit(1)


if __name__ == '__main__':
    setup()
