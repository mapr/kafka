%undefine __check_files

summary:     Ezmeral Ecosystem Pack: Kafka
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise, <ezmeral_software_support@hpe.com>
name:        mapr-kafka
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       MapR
buildarch:   noarch
requires:    mapr-client >= 6.1.9
conflicts:   mapr-core < 6.1.9
AutoReqProv: no


%description
Ezmeral Ecosystem Pack: Kafka Package
Tag: __RELEASE_BRANCH__
Commit: __GIT_COMMIT__


%clean
echo "NOOP"


%files
__PREFIX__/kafka

%pre
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "pre install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :
if [ "$1" = "2" ]; then
    if [ -f __PREFIX__/kafka/kafkaversion ]; then
        OLD_VERSION=$(cat __PREFIX__/kafka/kafkaversion)
        OLD_TIMESTAMP=$(rpm -qi mapr-kafka | awk -F': ' '/Version/ {print $2}')

        OLD_TIMESTAMP_FILE="%{_localstatedir}/lib/rpm-state/mapr-kafka-old-timestamp"
        OLD_VERSION_FILE="%{_localstatedir}/lib/rpm-state/mapr-kafka-old-version"

        STATE_DIR="$(dirname $OLD_TIMESTAMP_FILE)"
        if [ ! -d "$STATE_DIR" ]; then
            mkdir -p "$STATE_DIR"
        fi

        echo "$OLD_TIMESTAMP" > "$OLD_TIMESTAMP_FILE"
        echo "$OLD_VERSION" > "$OLD_VERSION_FILE"

        mkdir -p __PREFIX__/kafka/kafka-$OLD_TIMESTAMP/config
        cp -r __PREFIX__/kafka/kafka-$OLD_VERSION/config/* __PREFIX__/kafka/kafka-$OLD_TIMESTAMP/config/
        DAEMON_CONF=__PREFIX__/conf/daemon.conf

        if [ -f "$DAEMON_CONF" ]; then
            MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
            MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)
            if [ ! -z "$MAPR_USER" ]; then
                chown -R ${MAPR_USER}:${MAPR_GROUP} __PREFIX__/kafka/kafka-$OLD_TIMESTAMP
            fi
        fi
    fi
fi

%post
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "post install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ -f __PREFIX__/kafka/kafkaversion ]; then
  rm -f __PREFIX__/kafka/kafkaversion
fi
echo "__VERSION_3DIGIT__" > __PREFIX__/kafka/kafkaversion
rm -f __INSTALL_3DIGIT__/libs/log4j*.jar
rm -f __PREFIX__/lib/kafka-clients*.jar
rm -f __PREFIX__/lib/kafka-streams*.jar
rm -f __PREFIX__/lib/kafka-eventstreams*.jar
rm -f __PREFIX__/lib/rocksdbjni*.jar
newJar=$(find __INSTALL_3DIGIT__/libs/   -printf '%T+ %p\n'   | sort -r | grep kafka-clients |  head -n 1 |  awk '{ print $2 }')
ln -sf $newJar  __PREFIX__/lib/.
newJarStreams=$(find __INSTALL_3DIGIT__/libs/   -printf '%T+ %p\n'   | sort -r | grep kafka-streams-__VERSION_3DIGIT__ |  head -n 1 |  awk '{ print $2 }')
ln -sf $newJarStreams  __PREFIX__/lib/.
newJarEventstreams=$(find __INSTALL_3DIGIT__/libs/   -printf '%T+ %p\n'   | sort -r | grep kafka-eventstreams |  head -n 1 |  awk '{ print $2 }')
ln -sf $newJarEventstreams  __PREFIX__/lib/.
newJarRocksDb=$(find __INSTALL_3DIGIT__/libs/   -printf '%T+ %p\n'   | sort -r | grep rocksdbjni |  head -n 1 |  awk '{ print $2 }')
ln -sf $newJarRocksDb  __PREFIX__/lib/.

if [ ! -d /tmp/kafka-streams ]; then
    mkdir /tmp/kafka-streams
    chmod 1777 /tmp/kafka-streams
fi

if [ ! -d __INSTALL_3DIGIT__/logs ]; then
    mkdir __INSTALL_3DIGIT__/logs
fi
chmod 1777 __INSTALL_3DIGIT__/logs

#
# change permissions
#
DAEMON_CONF=__PREFIX__/conf/daemon.conf
if [ -f "$DAEMON_CONF" ]; then
    MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
    MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)
    if [ ! -z "$MAPR_USER" ]; then
        chown -R ${MAPR_USER} __INSTALL_3DIGIT__
    fi
    if [ ! -z "$MAPR_GROUP" ]; then
        chgrp -R ${MAPR_GROUP} __INSTALL_3DIGIT__
    fi
fi

%preun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ "$1" -eq "0" ]; then
  rm -f __INSTALL_3DIGIT__/libs/log4j*.jar
  rm -f __PREFIX__/lib/kafka-clients*.jar
  rm -f __PREFIX__/lib/kafka-eventstreams-*.jar
  rm -f __PREFIX__/lib/kafka-streams-*.jar
  rm -f __PREFIX__/lib/rocksdbjni*.jar
  rm -rf __INSTALL_3DIGIT__/logs/
  rm -rf  __PREFIX__/kafka/kafkaversion
fi



%postun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "postun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

%posttrans
# $1 -eq 0 install
# $1 -eq 0 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "posttrans install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

OLD_TIMESTAMP_FILE="%{_localstatedir}/lib/rpm-state/mapr-kafka-old-timestamp"
OLD_VERSION_FILE="%{_localstatedir}/lib/rpm-state/mapr-kafka-old-version"

# This files will exist only on upgrade
if [ -e "$OLD_TIMESTAMP_FILE" ] && [ -e "$OLD_VERSION_FILE" ]; then
    OLD_TIMESTAMP=$(cat "$OLD_TIMESTAMP_FILE")
    OLD_VERSION=$(cat "$OLD_VERSION_FILE")

    rm "$OLD_TIMESTAMP_FILE" "$OLD_VERSION_FILE"

    # Remove directory with old version
    NEW_VERSION=$(cat __PREFIX__/kafka/kafkaversion)

    if [ "$OLD_VERSION" != "$NEW_VERSION" ]; then
        rm -rf "__PREFIX__/kafka/kafka-${OLD_VERSION}"
    fi
fi

