# Schema Exporter for Confluent Cloud
Exporter scripts for Confluent Schema Registry.

A tool to export all schemas from a Confluent Cloud Schema Registry to another.
Currently it only supports a batch export. Consistent sync between registries might come in the future.

If you are looking to migrate schemas between On-Prem and Confluent Cloud, check out 
[Confuent Replicator](https://docs.confluent.io/current/connect/kafka-connect-replicator/index.html).

The exporter expects the following variables to be set in the environment to make the necessary calls:

- `SRC_SR_URL` : The URL for the source Schema Registry
- `SRC_API_KEY` : The API KEY to be used to make calls to the source Schema Registry
- `SRC_API_SECRET` : The API SECRET to be used to make calls to the source Schema Registry
- `DST_SR_URL` : The URL for the destination Schema Registry
- `DST_API_KEY` : The API KEY to be used to make calls to the destination Schema Registry
- `DST_API_SECRET` : The API SECRET to be used to make calls to the destination Schema Registry

It is also possible to define the credentials through command flags. If both are defined, the flags take precedence.

## Build
````
git clone https://github.com/abraham-leal/ccloud-schema-exporter
cd ccloud-schema-exporter/cmd/ccloud-schema-exporter
go build
````

## Run
`./cmd/ccloud-schema-exporter/ccloud-schema-exporter` : Running the command with the set env variables will 
automatically start the process.

### Options

````
Usage of ./ccloud-schema-exporter:
  -deleteAllFromDestination
    	Setting this will run a delete on all schemas written to the destination registry
  -dest-sr-key string
    	API KEY for the Destination Schema Registry Cluster
  -dest-sr-secret string
    	API SECRET for the Destination Schema Registry Cluster
  -dest-sr-url string
    	Url to the Source Schema Registry Cluster
  -src-sr-key string
    	API KEY for the Source Schema Registry Cluster
  -src-sr-secret string
    	API SECRET for the Source Schema Registry Cluster
  -src-sr-url string
    	Url to the Source Schema Registry Cluster
  -timeout int
    	Timeout, in second, to use for all REST call with the Schema Registries (default 60)
  -usage
    	Print the usage of this tool
  -version
    	Print the current version and exit
````

#### Example Usage 
````
export SRC_SR_URL=XXXX
export SRC_API_KEY=XXXX
export SRC_API_SECRET=XXXX
export DST_SR_URL=XXXX
export DST_API_KEY=XXXX
export DST_API_SECRET=XXXX
./ccloud-schema-exporter
````


