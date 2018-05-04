#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ $# -lt 1 ];
then
        echo "USAGE: $0 [-daemon] connect-distributed.properties"
        exit 1
fi

base_dir=$(dirname $0)
logDir="$base_dir/../logs"
logFile="$logDir/connect-distributed.log"
KAFKA_CONNECT_CONF="$@"
pid="/opt/mapr/pid/kafka-connect.pid"

if [ "x$KAFKA_LOG4J_OPTS" = "x" ]; then
    export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../config/connect-log4j.properties"
fi

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
  export KAFKA_HEAP_OPTS="-Xms256M -Xmx2G"
fi

EXTRA_ARGS=${EXTRA_ARGS-'-name connectDistributed'}

COMMAND=$1
case $COMMAND in
  -daemon)
    EXTRA_ARGS="-daemon "$EXTRA_ARGS
    shift
    ;;
  *)
    ;;
esac
logFile="$base_dir/../logs/connect-distributed.log"
KAFKA_CONNECT_CONF="$@"
# create logs directory
if [ ! -d "$logDir" ]; then
  mkdir -p "$logDir"
fi

if [ -f ${KAFKA_CONNECT_CONF} ]; then
	CONF_STREAM_DEFAULT=$(sed -n "/config.storage.topic/p"  ${KAFKA_CONNECT_CONF} | grep "=/var/mapr/.__mapr_connect:configs$")
	OFFSET_STREAM_DEFAULT=$(sed -n "/offset.storage.topic/p"  ${KAFKA_CONNECT_CONF} | grep "=/var/mapr/.__mapr_connect:offsets$")
fi

if [ $CONF_STREAM_DEFAULT ] || [ $OFFSET_STREAM_DEFAULT ]; then
        nowC=$(date +%s)
        while [  1 -eq 1 ]; do
	        ret=0
	        { maprcli volume info -name mapr.var; ret=$?; } >> /dev/null
            if [ $ret -eq 0 ]; then
                # volume exist. Try to create stream.
                break;
            fi

            # waiting for valume to be created
            realNow=$(date +%s)
            timeDiff="$(( $realNow - $nowC ))"
            if [ "$timeDiff" -gt 12 ]; then
 		        now=`date +%Y-%m-%d\ %H:%M:%S.$(( $(date +%-N) / 1000000 ))`
                echo "[$now] ERROR Kafka connect can not create default streams. Volume was not created." >> $logFile
                exit 1
            fi
            now=`date +%Y-%m-%d\ %H:%M:%S.$(( $(date +%-N) / 1000000 ))`
            echo "[$now] INFO Waiting 10 sec for /var/mapr to be created" >> $logFile
            sleep 10
        done
        now=`date +%Y-%m-%d\ %H:%M:%S.$(( $(date +%-N) / 1000000 ))`
	    echo "[$now] INFO Creating stream /var/mapr/.__mapr_connect if it does not already exist" >> $logFile
	    output="$(maprcli stream create -path /var/mapr/.__mapr_connect -ttl 0 2>&1)"
	    if [[ $output == *"Permission denied"* ]]; then
            now=`date +%Y-%m-%d\ %H:%M:%S.$(( $(date +%-N) / 1000000 ))`
		    echo "[$now] ERROR You do not have permission to create streams for storage.topic. Please fix the permission issue or change default values of config.storage.topic and offset.storage.topic in connect-distributed.properties" >> $logFile
		    exit 1
	    fi
	    maprcli stream topic create -path /var/mapr/.__mapr_connect -topic configs > /dev/null 2>&1
        maprcli	stream topic create -path /var/mapr/.__mapr_connect -topic offsets > /dev/null 2>&1
        maprcli	stream topic create -path /var/mapr/.__mapr_connect -topic status > /dev/null 2>&1
fi

# Add connect plugins to classpath
CONNECTORS_CLASSPATH=""
for jar in /opt/mapr/kafka-connect-*/kafka-connect-*/share/java/kafka-connect-*/*.jar
do
	CONNECTORS_CLASSPATH="$CONNECTORS_CLASSPATH:$jar"
done
export CONNECTORS_CLASSPATH

exec $(dirname $0)/kafka-run-class.sh $EXTRA_ARGS org.apache.kafka.connect.cli.ConnectDistributed "$@" & echo $! > $pid
