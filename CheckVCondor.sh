#!/bin/bash
OK=0
WARN=1
CRITICAL=2
UNKNOWN=3

OK_name=''
Fail_name=''
Config_file='./VCondor.conf'


echo -n 'Checking for VCondor running status..'

_line_number=`grep -n '\[GroupDict\]' "${Config_file}" |awk -F ':' '{print $1}'`
line=''
exit_code=0
GroupNum=0

while [ $exit_code -eq 0 ]
do
    line=`cat "$Config_file"|sed -n "${_line_number}p"|grep -v '^#'|grep ':'`
    if [ $? -eq 0 ]
    then
        ((GroupNum=GroupNum+1))
    fi
    ((_line_number=_line_number+1))
    line=`cat "$Config_file"|sed -n "${_line_number}p"|grep -v '\['`
    exit_code=$?
    if [ $exit_code -ne 0 ]
    then
        break
    fi
done

VCondorThreadNum=`pstree -p \`ps aux |grep 'python VCondorMain.py'|grep -v 'grep'|sed -n '1,1p'|awk '{print $2}'\`|wc -l`
if [ $VCondorThreadNum -lt $GroupNum ]
then
    echo ' Failed'
    Fail_name="${Faile_name} \n VCondor's threads running status is wrong!"
else
    echo ' OK'
    OK_name=''
fi


echo -n 'Checking for vmquota service status..'

Ip=`cat "$Config_file"|grep '^Ip'|awk -F '"' '{print $2}'`
Port=`cat "$Config_file"|grep '^Port'|awk -F '"' '{print $2}'`
echo -e "\n"|telnet ${Ip} ${Port} 2>/dev/null|grep Connected>/dev/null 2>&1
if [ $? -eq 0 ]
then
    echo ' OK'
    OK_name=''
else
    echo ' Failed'
    Fail_name="${Fail_name} \n vmquota service"
fi


echo -n 'Checking for condor server status and VCondor JobMonitor component..'

condor_collector_status=0
condor_schedd_status=0
condor_status_command=`cat ${Config_file}|grep '^condor_status_'|awk -F ': ' '{print $2}'`
echo "$condor_status_command">./out_file_czj
while read i
do
    $i>/dev/null 2>&1
    if [ $? -ne 0 ]
    then
        condor_collector_status=1
        Fail_name="${Fail_name} \n ${i}"
    fi
done < ./out_file_czj

condor_q_command=`cat ${Config_file}|grep '^condor_q_'|awk -F ': ' '{print $2}'`
echo "$condor_q_command" | while read i
do
    $i>/dev/null 2>&1
    if [ $? -ne 0 ]
    then
        condor_schedd_status=1
        Fail_name="${Fail_name} \n ${i}"
    fi
done

if [ $condor_collector_status -eq 0 ]&&[ $condor_schedd_status -eq 0 ]
then
    echo ' OK'
    OK_name=''
else
    echo ' Failed'
    echo 'Error occured when trying to excute those commands:'
    echo -e "${Fail_name}"|while read i
    do
        echo $i
    done
    Fail_name=''
fi


echo -n 'Checking for VCondor VM Manager component..'

cat "$Config_file"|grep '\[openstack\]'>/dev/null 2>&1
if [ $? -eq 0 ]
then
        OS_USERNAME=`cat "$Config_file"|grep '^OS_USERNAME'|awk -F '"' '{print $2}'`
        OS_PASSWORD=`cat "$Config_file"|grep '^OS_PASSWORD'|awk -F '"' '{print $2}'`
        OS_AUTH_URL=`cat "$Config_file"|grep '^OS_AUTH_URL'|awk -F '"' '{print $2}'`
	OS_TENANT_NAME=`cat "$Config_file"|grep '^OS_TENANT_NAME'|awk -F '"' '{print $2}'`

	curl -s -X POST ${OS_AUTH_URL}/tokens -H "Content-Type: application/json" -d '{"auth": {"tenantName": "'"${OS_TENANT_NAME}"'", "passwordCredentials":{"username": "'"${OS_USERNAME}"'", "password": "'"${OS_PASSWORD}"'"}}}'|grep '{"error"'>/dev/null 2>&1
	if [ $? -ne 0 ]
	then
        	echo ' OK'
        	OK_name=''
	else
    	        echo ' Failed'
    	        echo 'You should check the auth of openstack'
	fi
fi

cat "$Config_file"|grep '\[opennebula\]'>/dev/null 2>&1
if [ $? -eq 0 ]
then
	Authentication_method=`cat "$Config_file"|grep '^Authentication method'|awk -F '"' '{print $2}'`
	EndPoint=`cat "$Config_file"|grep '^EndPoint'|awk -F '"' '{print $2}'`
	Username=`cat "$Config_file"|grep '^Username'|awk -F '"' '{print $2}'`
	Password=`cat "$Config_file"|grep '^Password'|awk -F '"' '{print $2}'`

	occi -s -e ${EndPoint} -n basic -u "${Username}" -p "${Password}" -a list -r compute>/dev/null 2>&1
	if [ $? -eq 0 ]
	then
        	echo ' OK'
        	OK_name=''
	else
    	        echo ' Failed'
    	        echo 'You should check the auth of opennebula'
	fi
fi




