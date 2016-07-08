#!/bin/bash 
 env |grep HADOOPHOST > /tmp/hadoophost

 sed 's/^.*=//g' /tmp/hadoophost>/tmp/result

 cat /tmp/result >> /etc/hosts
 
 java -jar ./app.jar
