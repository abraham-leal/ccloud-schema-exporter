#!/bin/bash
# author: aleal@confluent.io @ github.com/abraham-leal
set -e

## Check for provided arguments
if (( "$#" < 1 )); then
    echo "Illegal number of parameters."
    echo
    echo "Usage: 
        $0 <schemaRegistryUrl> [Optional: Beginning Schema ID, Default: 10,000] [Optional: Basic Auth Credentials in form of <USERNAME>:<PASSWORD>]
        
    Example: 
        $0 http://localhost:8081 500 userAdmin:adminPassword
    "
    exit
fi

## Capture Schema Registry URL
SRURL_INPUT=$1
if [[ $SRURL_INPUT == *'/' ]]
then
SRURL=$(echo $SRURL_INPUT | sed 's/.$//')
else
SRURL=$SRURL_INPUT
fi

## Default beginning ID if not set
if [[ -z $2 ]]
then 
echo "No beginning ID provided, starting at 10000"
SRBEGINNING=10000
else
echo "Starting SR at "$2
SRBEGINNING=$2
fi

## Default beginning ID if not set
if [[ -z $3 ]]
then 
echo "No auth provided, proceeding without it"
AUTH=""
else
AUTH=$3
USERNAME=$(echo $AUTH | sed 's/:.*//')
echo "Using username: "$USERNAME
fi

DUMMY_SUBJECT="dummy"
DUMMY_SUBJECT_2="dummy2"
SUBJECTMODE_URL=$SRURL'/mode/'$DUMMY_SUBJECT
IMPORT_PAYLOAD='{"mode": "IMPORT"}'
READWRITE_PAYLOAD='{"mode": "IMPORT"}'
DUMMYSCHEMA_IMPORT="{\"schema\": \"{\\\"type\\\": \\\"bytes\\\"}\", \"id\": $SRBEGINNING, \"version\": 1}"
DUMMY_SCHEMA='{"schema": "{\"type\": \"string\"}"}'
SUBJECTSCHEMA_ENDPOINT=$SRURL'/subjects/'$DUMMY_SUBJECT'/versions'
SUBJECTSCHEMA_ENDPOINT_2=$SRURL'/subjects/'$DUMMY_SUBJECT_2'/versions'
CONTENT='Content-Type: application/json'

## Check we are dealing with a brand new SR
RESULT_SUBJECTS=$(curl -u $AUTH --silent -k $SRURL"/schemas?deleted=true")
if [ "$RESULT_SUBJECTS" != "[]" ]
then
echo "ERROR: Schema Registry is not empty. Aborting."
exit 1
fi

## Set IMPORT on a single subject
RESULT_SETIMPORT=$(curl -u $AUTH --silent -k -X PUT -d "$IMPORT_PAYLOAD" -H "$CONTENT" $SUBJECTMODE_URL)

## Register dummy schema
RESULT_REGISTER=$(curl -u $AUTH --silent -k -X POST -d "$DUMMYSCHEMA_IMPORT" -H "$CONTENT" $SUBJECTSCHEMA_ENDPOINT)

## Set READWRITE on a single subject
RESULT_SETRW=$(curl -u $AUTH --silent -k -X PUT -d "$READWRITE_PAYLOAD" -H "$CONTENT" $SUBJECTMODE_URL)

## Soft delete dummy schema to maintain the beginning ID
RESULT_DELETE=$(curl -u $AUTH --silent -k -X DELETE $SUBJECTSCHEMA_ENDPOINT"/1")
HOLD=$(curl -u $AUTH --silent -k -X DELETE $SUBJECTSCHEMA_ENDPOINT"/1?permanent=true")

## Register another dummy and confirm ID=SRBEGINNING+1 <- Sanity Check
OUTPUT=$(curl -u $AUTH --silent -k -X POST -d "$DUMMY_SCHEMA" -H "$CONTENT" $SUBJECTSCHEMA_ENDPOINT_2)
SR_REGISTERED=$(echo $OUTPUT | tr -d -c 0-9)
if (( $SR_REGISTERED >= $SRBEGINNING )); then
  echo "Validation Test Passed."
  echo "Schema Registry ID space now starts at "$SRBEGINNING+
else
  echo "Validation Test Failed."
fi

## Cleanup
HOLD=$(curl -u $AUTH --silent -k -X DELETE $SUBJECTSCHEMA_ENDPOINT_2"/1")
HOLD=$(curl -u $AUTH --silent -k -X DELETE $SUBJECTSCHEMA_ENDPOINT_2"/1?permanent=true")
