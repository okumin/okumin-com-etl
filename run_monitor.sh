#!/bin/bash -eux

TEMP_LOCATION=$1
TEMPLATE_LOCATION=$2
MONITOR_INPUT=$3
MONITOR_OUTPUT=$4

sbt "runMain com.okumin.etl.AccessLogMonitor --runner=DataflowRunner --project=okumin-prod --zone=asia-northeast1-a --network=primary --subnetwork=regions/asia-northeast1/subnetworks/asia-northeast1-default --diskSizeGb=16 --workerMachineType=n1-standard-1 --maxNumWorkers=1 --streaming=true --tempLocation=${TEMP_LOCATION} --templateLocation=${TEMPLATE_LOCATION} --monitorInput=${MONITOR_INPUT}"

gcloud beta dataflow jobs run \
  okumin-com-access-log-monitor-v1 \
  --gcs-location ${TEMPLATE_LOCATION} \
  --parameters monitorOutput=${MONITOR_OUTPUT}
