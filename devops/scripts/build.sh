#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"
. "${SCRIPT_DIR}/_utils.sh"

build_kafka() {
  ./gradlewAll ${KAFKA_GRADLE_ARGS}

  mkdir -p "${BUILD_ROOT}/build"

  scala_version="$(awk -F= '$1 == "scalaVersion" { print $2 }' gradle.properties | cut -d. -f1-2)"
  artifact_version="$(awk -F= '$1 == "version" { print $2 }' gradle.properties)"
  tgz_name="./core/build/distributions/kafka_${scala_version}-${artifact_version}.tgz"

  tar xvf ${tgz_name} --strip-components=1 -C "${BUILD_ROOT}/build"
}

main() {
  echo "Cleaning '${BUILD_ROOT}' dir..."
  rm -rf "$BUILD_ROOT"

  if [ "$DO_DEPLOY" = "true" ] && [ "$OS" = "redhat" ]; then
    echo "Deploy is enabled"
    export KAFKA_GRADLE_ARGS="-PmavenUrl=${MAPR_MAVEN_REPO} -PmavenUsername=${MAPR_MAVEN_USER} -PmavenPassword=${MAPR_MAVEN_PASS} -PskipSigning=true ${KAFKA_GRADLE_ARGS} publish"
  fi

  echo "Building project..."
  build_kafka

  echo "Preparing directory structure..."
  setup_role "mapr-kafka"

  setup_package "mapr-kafka"

  echo "Building packages..."
  build_package "mapr-kafka"

  echo "Resulting packages:"
  find "$DIST_DIR" -exec readlink -f {} \;
}

main
