#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This file is sourced by mapr-config.sh to provide get_kafka_jars()
# and get_kafka_external_jars() . Assumes MAPR_HOME and MAPR_LIB are
# set, and get_files_in_folder() is defined
# (This code was extracted from the Core mapr-config.sh in order to
#  separate non-core knowledge, and placing it with the Eco component)

export KAFKA_VERSION=`cat ${MAPR_HOME}/kafka/kafkaversion`
export KAFKA_HOME="${MAPR_HOME}/kafka/kafka-${KAFKA_VERSION}"

# get Apache Kafka jars
get_kafka_jars() {

KAFKA_MAPR_JARS=$(get_files_in_folder ${KAFKA_HOME}/libs\
    "mapr-eco-tools-*.jar" "slf4j-reload4j*.jar")

echo $KAFKA_MAPR_JARS
}

# Add kafka schema registry related jars
get_kafka_external_jars() {
  get_files_in_folder $MAPR_LIB\
    "kafka-connect-avro-converter*.jar"\
    "kafka-avro-serializer*.jar"\
    "kafka-connect-protobuf-converter*.jar"\
    "kafka-protobuf-serializer*.jar"\
    "kafka-connect-json-schema-converter*.jar"\
    "kafka-json-schema-serializer*.jar"\
    "kafka-schema-registry-client*.jar"
}

