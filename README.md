# Streaming to BigQuery
POC/tutorial code - stream to BigQuery using Pub/Sub and Dataflow

## Running the code from GCP shell

### Copy over code and install required libraries
gsutil cp gs://cb-dflow-poc/* .
sudo pip install apache-beam[gcp] 
sudo pip install -U pip
sudo pip install Faker==1.0.2
BUCKET=cb-dflow-poc
PROJECT=gcp-stl

### Kick off Dataflow task
python unittest2.py \
--region us-central1 \
--runner DataFlow \
--project $PROJECT \
--temp_location $BUCKET/tmp --streaming

### Publish desired number of messages to topic (kill job when you have enough messages)
python publish_message.py





