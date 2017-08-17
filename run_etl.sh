#!/bin/bash -eux

TEMP_LOCATION=$1
TEMPLATE_LOCATION=$2
TRANSFORM_INPUT=$3
TRANSFORM_OUTPUT=$4
LOAD_INPUT=$5
LOAD_OUTPUT=$6

sbt "runMain com.okumin.etl.AccessLogEtl --runner=DataflowRunner --project=okumin-prod --zone=asia-northeast1-a --network=primary --subnetwork=regions/asia-northeast1/subnetworks/asia-northeast1-default --diskSizeGb=16 --workerMachineType=n1-standard-1 --maxNumWorkers=1 --streaming=true --tempLocation=${TEMP_LOCATION} --templateLocation=${TEMPLATE_LOCATION} --transformInput=${TRANSFORM_INPUT} --loadInput=${LOAD_INPUT}"

gcloud beta dataflow jobs run \
  okumin-com-access-log-etl-v1 \
  --gcs-location ${TEMPLATE_LOCATION} \
  --parameters transformOutput=${TRANSFORM_OUTPUT},loadOutput=${LOAD_OUTPUT}
