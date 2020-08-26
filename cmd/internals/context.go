package client

//
// context.go
// Author: Abraham Leal
//

import (
	"flag"
	"fmt"
	"os"
	"log"
)

func GetFlags() {

	flag.StringVar(&SrcSRUrl, "src-sr-url", "", "Url to the Source Schema Registry Cluster")
	flag.StringVar(&SrcSRKey, "src-sr-key", "", "API KEY for the Source Schema Registry Cluster")
	flag.StringVar(&SrcSRSecret, "src-sr-secret", "", "API SECRET for the Source Schema Registry Cluster")
	flag.StringVar(&DestSRUrl, "dest-sr-url", "", "Url to the Destination Schema Registry Cluster")
	flag.StringVar(&DestSRKey, "dest-sr-key", "", "API KEY for the Destination Schema Registry Cluster")
	flag.StringVar(&DestSRSecret, "dest-sr-secret", "", "API SECRET for the Destination Schema Registry Cluster")
	flag.IntVar(&httpCallTimeout, "timeout", 60, "Timeout, in seconds, to use for all REST calls with the Schema Registries")
	flag.IntVar(&ScrapeInterval, "scrapeInterval", 60, "Amount of time ccloud-schema-exporter will delay between schema sync checks in seconds")
	versionFlag := flag.Bool("version", false, "Print the current version and exit")
	usageFlag := flag.Bool("usage", false, "Print the usage of this tool")
	batchExportFlag := flag.Bool("batchExport", false, "Perform a one-time export of all schemas")
	syncFlag := flag.Bool("sync", false, "Sync schemas continuously")
	deleteFlag := flag.Bool("deleteAllFromDestination", false, "Setting this will run a delete on all schemas written to the destination registry")
	syncDeletesFlag := flag.Bool("syncDeletes", false, "Setting this will sync soft deletes from the source cluster to the destination")

	flag.Parse()

	if *syncDeletesFlag {
		syncDeletes = true
	}

	if *versionFlag {
		printVersion()
		os.Exit(0)
	}

	if *usageFlag {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *deleteFlag{
		log.Println("Deleting all schemas from DESTINATION registry")
		deleteAll(DestSRUrl,DestSRKey,DestSRSecret)
		os.Exit(0)
	}

	if !*syncFlag && !*batchExportFlag {
		log.Println("You must specify whether to run in batch or sync mode.")
		log.Println("Usage:")
		log.Println("")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *batchExportFlag {
		RunMode = "BATCH"
	}

	if *syncFlag {
		RunMode = "SYNC"
	}

}

func printVersion() {
	fmt.Printf("ccloud-schema-exporter: %s\n", Version)
}

func deleteAll(sr string, key string, secret string){
	destClient := NewSchemaRegistryClient(sr,key,secret, "dst")
	aChan := make (chan map[string][]int, 1)
	destClient.GetSubjectsWithVersions(aChan)
	_ = <- aChan
	destClient.DeleteAllSubjectsPermanently()
}



