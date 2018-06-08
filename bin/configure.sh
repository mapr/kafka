#!/bin/bash
#######################################################################
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
#######################################################################
#
# Configure script for Kafka connect
#
# This script is normally run by the core configure.sh to setup Kafka 
# connect during install. If it is run standalone, need to correctly 
# initialize the variables that it normally inherits from the master
# configure.sh
#######################################################################


RETURN_SUCCESS=0
RETURN_ERR=1



# Initialize API and globals

MAPR_HOME=${MAPR_HOME:-/opt/mapr}

. ${MAPR_HOME}/server/common-ecosystem.sh  2> /dev/null
{ set +x; } 2>/dev/null

initCfgEnv

if [ $? -ne 0 ] ; then
  echo '[ERROR] MAPR_HOME seems to not be set correctly or mapr-core not installed.'
  exit ${RETURN_ERR}
fi

KAFKA_CONNECT_PORT=8083
KAFKA_HOME=${MAPR_HOME}/kafka/kafka-1.1.1/
KAFKA_CONNECT_HOME=${MAPR_HOME}/kafka-connect*/kafka-connect*
KAFKA_CONNECT_HDFS_HOME=${MAPR_HOME}/kafka-connect-hdfs/kafka-connect-hdfs-4.1.0/
KAFKA_CONNECT_JDBC_HOME=${MAPR_HOME}/kafka-connect-jdbc/kafka-connect-jdbc-4.1.0/
WARDEN_KAFKA_CONNECT_DEST_CONF=${MAPR_HOME}/conf/conf.d/warden.kafka-connect.conf
WARDEN_KAFKA_CONNECT_FILE=${KAFKA_HOME}/config/warden.kafka-connect.conf
KAFKA_CONNECT_PROPERTIES=${KAFKA_CONNECT_PROPERTIES:-${KAFKA_HOME}/config/connect-distributed.properties}
KAFKA_VERSION_FILE=${MAPR_HOME}/kafka/kafkaversion
DAEMON_CONF=${MAPR_HOME}/conf/daemon.conf
VERSION=1.1.1
MAPR_RESTART_SCRIPTS_DIR=${MAPR_RESTART_SCRIPTS_DIR:-${MAPR_HOME}/conf/restart}
KAFKA_CONNECT_RESTART_SRC=${KAFKA_CONNECT_RESTART_SRC:-${MAPR_RESTART_SCRIPTS_DIR}/kafka-connect-4.0.0.restart}



# SSL-specific
KAFKA_CONNECT_CERTIFICATES_DIR=${KAFKA_CONNECT_CERTIFICATES_DIR:-${KAFKA_HOME}/config}
KAFKA_CONNECT_CERT=${KAFKA_CONNECT_CERT:-${KAFKA_CONNECT_CERTIFICATES_DIR}/cert.pem}
KAFKA_CONNECT_DEST_STORE=${KAFKA_CONNECT_DEST_STORE:-${KAFKA_CONNECT_CERTIFICATES_DIR}/keystore.p12}
KAFKA_CONNECT_DEST_STORE_PASSWD=${KAFKA_CONNECT_DEST_STORE_PASSWD:-'mapr123'}
KAFKA_CONNECT_OPENSSL_KEY=${KAFKA_CONNECT_OPENSSL_KEY:-${KAFKA_CONNECT_CERTIFICATES_DIR}/key.pem}

MAPR_CLDB_SSL_KEYSTORE=${MAPR_CLDB_SSL_KEYSTORE:-${MAPR_HOME}/conf/ssl_keystore}
MAPR_CLDB_SSL_TRUSTSTORE=${MAPR_CLDB_SSL_TRUSTSTORE:-${MAPR_HOME}/conf/ssl_truststore}
KAFKA_CONNECT_MAPR_CLDB_SSL_KEYSTORE_PASSWD=${KAFKA_CONNECT_MAPR_CLDB_SSL_KEYSTORE_PASSWD:-'mapr123'}
KAFKA_CONNECT_MAPR_CLDB_SSL_TRUSTSTORE_PASSWD=${KAFKA_CONNECT_MAPR_CLDB_SSL_TRUSTSTORE_PASSWD:-'mapr123'}

KAFKA_CONNECT_CREDENTIALS_FILE="/user/${MAPR_USER}/kafkaconnect.jceks"
KAFKA_CONNECT_CREDENTIALS_PROP="jceks://maprfs/user/${MAPR_USER}/kafkaconnect.jceks"

function write_version_file() {
    if [ -f ${KAFKA_VERSION_FILE} ]; then
        rm -f ${KAFKA_VERSION_FILE}
    fi
    echo ${VERSION} > ${KAFKA_VERSION_FILE}
}

function change_permissions() {
    if [ -f $DAEMON_CONF ]; then
        MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
        MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)

        if [ ! -z "$MAPR_USER" ]; then
            chown -R ${MAPR_USER} ${KAFKA_HOME}
            chown ${MAPR_USER} ${MAPR_CONF_DIR}
            if ls ${KAFKA_CONNECT_HOME}  1> /dev/null 2>&1; then
                chown -R ${MAPR_USER} ${KAFKA_CONNECT_HOME}
            fi
        fi

		if [ ! -z "$MAPR_GROUP" ]; then
            chgrp -R ${MAPR_GROUP} ${KAFKA_HOME}
            chgrp ${MAPR_GROUP} ${MAPR_CONF_DIR}
            if ls ${KAFKA_CONNECT_HOME}  1> /dev/null 2>&1; then
                chgrp -R ${MAPR_GROUP} ${KAFKA_CONNECT_HOME}
            fi
        fi
        chmod -f u+x ${KAFKA_HOME}/bin/*
        chmod 640 ${KAFKA_CONNECT_PROPERTIES}
    fi
}

function setup_warden_config() {
    if [ -f ${WARDEN_KAFKA_CONNECT_DEST_CONF} ]; then
        rm -f ${WARDEN_KAFKA_CONNECT_DEST_CONF}
    fi
    cp ${WARDEN_KAFKA_CONNECT_FILE} ${WARDEN_KAFKA_CONNECT_DEST_CONF}
    chown ${MAPR_USER} ${WARDEN_KAFKA_CONNECT_DEST_CONF}
    chgrp ${MAPR_GROUP} ${WARDEN_KAFKA_CONNECT_DEST_CONF}
}

function configure_insecure_mode() {
    logInfo 'This is initial run of Kafka Connect configure.sh';
    write_version_file
    create_standard_properties_file
    change_permissions
    setup_warden_config
    return 0
}

function configure_secure_mode() {
    logInfo 'This is initial run of Kafka Connect configure.sh';
    write_version_file
    if ! enable_ssl; then
        return 1
    fi
    change_permissions
    setup_warden_config
    return 0
}


function create_properties_file_with_ssl_config() {
		if ! hadoop fs -test -f "$KAFKA_CONNECT_CREDENTIALS_FILE"; then
			su "$MAPR_USER" -c hadoop credential create "ssl.keystore.password" -value "$KAFKA_CONNECT_MAPR_CLDB_SSL_KEYSTORE_PASSWD" -provider "$KAFKA_CONNECT_CREDENTIALS_PROP"
			su "$MAPR_USER" -c hadoop credential create "ssl.key.password" -value "$KAFKA_CONNECT_MAPR_CLDB_SSL_KEYSTORE_PASSWD" -provider "$KAFKA_CONNECT_CREDENTIALS_PROP"
	    fi
        cat >>${KAFKA_CONNECT_PROPERTIES} <<-EOL
		listeners=https://0.0.0.0:${KAFKA_CONNECT_PORT}
		ssl.keystore.location=${MAPR_CLDB_SSL_KEYSTORE}
		connect.hadoop.security.credential.provider.path=${KAFKA_CONNECT_CREDENTIALS_PROP}
		EOL
}


function create_standard_properties_file() {
        TMP_CONFIG=$(sed '/^listeners/d' ${KAFKA_CONNECT_PROPERTIES} | sed '/^ssl.key/d')
        echo "$TMP_CONFIG" > ${KAFKA_CONNECT_PROPERTIES}
}

function generate_cert_and_key() {
    { ALIAS="$(getClusterName)"; } 2>/dev/null
    keytool -v -exportcert -alias "${ALIAS}" -keystore "${MAPR_CLDB_SSL_TRUSTSTORE}" -rfc -file "${KAFKA_CONNECT_CERT}" \
        -storepass "${KAFKA_CONNECT_MAPR_CLDB_SSL_TRUSTSTORE_PASSWD}" 2>/dev/null
    if [ $? -ne 0 ] ; then
        logWarn 'Warning: No certificate has been generated due to keytool error.'
        return 1
    fi

    keytool -importkeystore -noprompt \
        -srckeystore "${MAPR_CLDB_SSL_KEYSTORE}" -destkeystore "${KAFKA_CONNECT_DEST_STORE}" \
        -srcstoretype JKS -deststoretype PKCS12 \
        -srcstorepass "${KAFKA_CONNECT_MAPR_CLDB_SSL_KEYSTORE_PASSWD}" \
        -deststorepass "${KAFKA_CONNECT_DEST_STORE_PASSWD}" 2>/dev/null

    if [ $? -ne 0 ] ; then
        logWarn 'Warning: No keystore has been imported due to keytool error.'
        return 1
      else
        logInfo "Keystore has been imported from JKS to PKCS12 at '${KAFKA_CONNECT_DEST_STORE}'"
    fi

    openssl pkcs12 -in "${KAFKA_CONNECT_DEST_STORE}" -nodes -nocerts -out "${KAFKA_CONNECT_OPENSSL_KEY}" \
        -passin "pass:${KAFKA_CONNECT_DEST_STORE_PASSWD}" 2>/dev/null
    if [ $? -ne 0 ] ; then
        logWarn 'Warning: No PKCS12 has been converted due to openssl error.'
        return 1
      else
        logInfo "PKCS12 has been converted to pem using OpenSSL at '${KAFKA_CONNECT_OPENSSL_KEY}'"
    fi

    return 0
}

function enable_ssl() {
    if ! check_mapr_cldb_keystore; then
        return 1
    fi

    if ! check_mapr_cldb_truststore; then
        return 1
    fi
    create_properties_file_with_ssl_config

    if ! generate_cert_and_key; then
        return 1
    fi

    chown -R ${MAPR_USER} ${KAFKA_CONNECT_CERTIFICATES_DIR}
    chgrp -R ${MAPR_GROUP} ${KAFKA_CONNECT_CERTIFICATES_DIR}

    return 0
}

function check_mapr_cldb_keystore() {
    if [ ! -f ${MAPR_CLDB_SSL_KEYSTORE} ]; then
        logErr "Error: Can not enable Kafka Connect SSL since MapR keystore file '$MAPR_HOME/conf/ssl_keystore' does not" \
        "exist. It seems that cluster is configured in non-secure way."
        return 1
    fi

    return 0
}

function check_mapr_cldb_truststore() {
    if [ ! -f ${MAPR_CLDB_SSL_TRUSTSTORE} ]; then
        logErr "Error: Can not enable Kafka Connect SSL since MapR truststore file '$MAPR_CLDB_SSL_TRUSTSTORE' does not" \
        "exist. It seems that cluster is configured in non-secure way."
        return 1
    fi

    return 0
}

function stopService(){
	logInfo "Stopping Kafka Connect..."
	${KAFKA_HOME}/bin/connect-distributed-stop 2>/dev/null
}

# Parse options

USAGE="usage: $0 [-h] [-R]"


{ OPTS=`getopt -n "$0" -a -o suhR --long secure,unsecure,help,EC -- "$@"`; } 2>/dev/null
eval set -- "$OPTS"

isSecure=false
custom=false
for i in "$@" ; do
  case "$i" in
    --secure)
      isSecure=true;
      shift 1;;
    --unsecure)
      isSecure=false;
      shift ;;
    --R)
      isOnlyRoles=1;
      shift ;;
    -cs | --customSecure)
        logInfo '--cs'
    	if  [ -f "$KAFKA_CONNECT_JDBC_HOME/conf/.not_configured_yet" ] ; then
	    	isSecure=true
		fi

		if [ -f "$KAFKA_CONNECT_HDFS_HOME/conf/.not_configured_yet" ]  ; then
		    isSecure=true
		fi
        SECURE=false;
        custom=true;
    	shift ;;
    --h)
      echo "${USAGE}"
      exit ${RETURN_SUCCESS}
      ;;
    --EC)
      ecosystemParams="$2"
      shift 2;;
    --)
      shift; break;;
    *)
      # Invalid arguments passed
      break;;

  esac
done


# remove state file
if  [ -f "$KAFKA_CONNECT_JDBC_HOME/conf/.not_configured_yet" ] ; then
    rm -f "$KAFKA_CONNECT_JDBC_HOME/conf/.not_configured_yet"
fi

if [ -f "$KAFKA_CONNECT_HDFS_HOME/conf/.not_configured_yet" ]  ; then
    rm -f "$KAFKA_CONNECT_HDFS_HOME/conf/.not_configured_yet"
fi

if ${isSecure}; then
    num=3
    IS_SECURE_CONFIG=$(grep -e ssl.key -e listeners -e connect.hadoop ${KAFKA_CONNECT_PROPERTIES} | wc -l)
    if [ ${IS_SECURE_CONFIG} -lt ${num} ]; then
        if configure_secure_mode; then
            logInfo 'Kafka Connect successfully configured to run in secure mode.'
            stopService
        else
            logErr 'Error: Errors occurred while configuring Kafka Connect to run in secure mode.'
            exit ${RETURN_ERR}
        fi
    else
        change_permissions
        setup_warden_config
	    logInfo 'Kafka Connect has been already configured to run in secure mode.'
    fi
else
    setup_warden_config
    change_permissions
    echo
    if ${custom}; then
        exit ${RETURN_SUCCESS}
    fi
    if  grep -q ssl ${KAFKA_CONNECT_PROPERTIES}; then
       configure_insecure_mode
       stopService
       logInfo 'Kafka Connect successfully configured to run in unsecure mode.'
    fi
fi

exit ${RETURN_SUCCESS}
