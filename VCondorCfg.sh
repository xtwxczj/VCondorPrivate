#!/bin/bash

echo  "############################################################################"
echo  "###Welcome to VCondor! First you need to input configuration information.###"
echo  "############################################################################"

step=0
echo  "Please input 'y' to continue, or 'n' to exit."
echo -n ">> "
read flag
if [ $flag = 'n' ]
then
    exit
fi
if [ $flag = 'y' ]
then
    ((step=step+1))
fi



while [ $step -gt 0 ]
do

    if [ $step -eq 1 ]
    then
	echo  "Please input VCondor Timeout for VM launching. Timeout is the longest waiting time(seconds) for virtual machines to start."
        echo  " The default value is 600 seconds. For example, if a VM cannot start successfully and be added to condor pool in 600 seconds, VCondor think there is something wrong with the VM and shut the VM down."
        echo  "You can input 'b' to go back to the last step."
        echo -n ">> "
        read Timeout

        if [ $Timeout = 'b' ]
        then
            ((step=step-1))
            continue
        fi        
	sed -i s/"^Timeout:.*"/"Timeout: $Timeout"/g ./VCondor.conf.1 
    fi


    if [ $step -eq 2 ]
    then
        echo  "VmSchedule_interval is the number of seconds between VM scheduling cycles. Increasing"
        echo  "      this value will lower the load on the system, and decreasing it will improve"
        echo  "      responsiveness. The default value is 600 seconds."
        echo  "You can input 'b' to go back to the last step."
        echo -n ">> "
        read VmSchedule_interval

        if [ $VmSchedule_interval = 'b' ]
        then
            ((step=step-1))
            continue
        fi        
	sed -i s/"^VmSchedule_interval:.*"/"VmSchedule_interval: $VmSchedule_interval"/g ./VCondor.conf.1 
    fi


    if [ $step -eq 3 ]
    then
	echo  "VCondor needs to work together with VMQuota. VMQuota is the component which controls"
	echo  "      resource quotas of experiment groups. You must set Ip of VMQuota"
	echo  "      before you start VCondor."
        echo  "You can input 'b' to go back to the last step."
        echo -n ">> "
        read Ip

        if [ $Ip = 'b' ]
        then
            ((step=step-1))
            continue
        fi        
	sed -i s/"^Ip:.*"/"Ip: $Ip"/g ./VCondor.conf.1 
    fi


    if [ $step -eq 4 ]
    then
	echo  "You must set port of VMQuota!"
        echo  "You can input 'b' to go back to the last step."
        echo -n ">> "
        read Port

        if [ $Port = 'b' ]
        then
            ((step=step-1))
            continue
        fi        
	sed -i s/"^Port:.*"/"Port: $Port"/g ./VCondor.conf.1 
    fi


    if [ $step -eq 5 ]
    then
	echo  "log_level specifies how much information from VCondor to log."
	echo  ""
	echo  "      Choose from  DEBUG, INFO, WARNING, ERROR and CRITICAL"
	echo  "       The default is INFO"
	echo  ""
	echo  "       WARNING!!! - DO NOT USE JOBS OR DEBUG WITH VERY LARGE NUMBERS OF JOBS"
        echo  "You can input 'b' to go back to the last step."
        echo -n ">> "
        read log_level

        if [ $log_level = 'b' ]
        then
            ((step=step-1))
            continue
        fi        
	sed -i s/"^log_level:.*"/"log_level: $log_level"/g ./VCondor.conf.1 
    fi


    if [ $step -eq 6 ]
    then
	echo ""
    fi

done


if [ $step -eq 0 ]
then
    echo 'abc'
fi

echo -n "Please input VCondor log level(ERROR|INFO|DEBUG): "
read log_level
sed -i s/"^DEBUG_MODE.*"/"DEBUG_MODE: $log_level"/g ./VCondor.conf.bak 

echo -n "Please input average time "

echo -n "enter your name:"
read name
echo "hello $name, welcome to my program."
