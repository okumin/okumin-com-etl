#!/bin/bash -eux

TRIGGER_TOPIC=$1
STAGE_BUCKET=$2

cd ./gcf/alert

# Currently only us-central1 is supported
gcloud beta functions deploy subscribe_alert \
  --trigger-topic ${TRIGGER_TOPIC} \
  --region us-central1 \
  --stage-bucket ${STAGE_BUCKET}
