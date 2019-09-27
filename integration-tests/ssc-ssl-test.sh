#!/usr/bin/env bash

set -e

HOST=localhost
PORT=9543
PROJECT_PATH="../2-collectors/scala-stream-collector/"
CERT_FILE="server.p12"

function version(){
    field="${1}"
    format="${2}"
    sed -n "s/${field}.*\"\(${format}\)\"\,/\1/p" "${PROJECT_PATH}/build.sbt" | xargs
}

TIMEOUT=$(test -z ${TRAVIS} && echo 2 || echo 10)
SSC_VERSION=$(version "version" "[0-9]*\.[0-9]*\.[0-9]*")
SCALA_VERSION=$(version "scalaVersion" "[0-9]*\.[0-9]*\)\(\.[0-9]*")
BIN_PATH="${PROJECT_PATH}/stdout/target/scala-${SCALA_VERSION}/snowplow-stream-collector-stdout-${SSC_VERSION}.jar"
echo $BIN_PATH

function mk_cert(){
    echo "Setting up certs"
    openssl req \
         -x509 \
         -newkey rsa:4096 \
         -keyout collector_key.pem \
         -out collector_cert.pem \
         -days 3650 \
         -nodes \
         -subj "/C=UK/O=Snowplow Analytics Ltd/OU=Technical Operations/CN=localhost"
    echo "Converting certs to PKCS12"
    openssl pkcs12 \
         -export \
         -out "${CERT_FILE}" \
         -inkey collector_key.pem \
         -in collector_cert.pem \
         -passout pass:pass
    rm *.pem 
}

function scd(){
    cd "${1}" || exit
}

function build(){
    test -f "${BIN_PATH}" || (scd ${PROJECT_PATH}; sbt "project stdout" assembly &>/dev/null; scd -)
}

function run(){
    config="${1}"
    env CERT_FILE="${CERT_FILE}" java -jar ${BIN_PATH} --config ${config} &
    timeout ${TIMEOUT} bash -c 'until printf "" 2>>/dev/null >>/dev/tcp/$0/$1; do sleep 1; done' ${HOST} ${PORT}
    sleep 1
}

function ping() {
    with_ssl="${1}"
    validate=$(test "${1}" && echo "-k" || echo "")
    prefix=$(test "${1}" && echo "https" || echo "http")
    curl ${validate} -I ${prefix}://${HOST}:${PORT}/health &>/dev/null
}

function stop(){
    kill -9 "$1"
    test -f "${CERT_FILE}" && rm "${CERT_FILE}"
}
function verify(){
    testcase="${1}"
    safe="${2}"
    echo "[TEST ${testcase}] Running"
    build && run "${testcase}" && RUN_ID=$(jobs -p) && echo "+ Started ${RUN_ID}"
    ping "${2}" && echo "+ OK" || (echo "- FAILED" && stop "${RUN_ID}" && exit 1)
    stop "${RUN_ID}" && echo "[TEST ${testcase}] Stopped ${RUN_ID}" || echo "Failed to stop ${RUN_ID}"
}


verify "no-ssl.hocon"
mk_cert
verify "with-ssl.hocon" true
