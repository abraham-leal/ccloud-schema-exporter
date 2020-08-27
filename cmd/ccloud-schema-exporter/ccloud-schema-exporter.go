package main

//
// ccloud-schema-exporter.go
// Author: Abraham Leal
//

import (
	"fmt"
	"github.com/abraham-leal/ccloud-schema-exporter/cmd/internals"
	"log"
	"os"
)

func main() {

	client.GetFlags()

	srcClient := client.NewSchemaRegistryClient(client.SrcSRUrl,client.SrcSRKey, client.SrcSRSecret , "src")
	if (!srcClient.IsReachable()){
		log.Println("Could not reach source registry. Possible bad credentials?")
		os.Exit(0)
	}

	if client.RunMode == "LOCAL" {
		client.WriteToFS(srcClient, client.PathToWrite)

		log.Println("-----------------------------------------------")
		log.Println("All Done! Thanks for using ccloud-schema-exporter!")

		os.Exit(0)
	}

	destClient := client.NewSchemaRegistryClient(client.DestSRUrl, client.DestSRKey, client.DestSRSecret, "dst")
	if (!destClient.IsReachable()){
		log.Println("Could not reach destination registry. Possible bad credentials?")
		os.Exit(0)
	}

	destChan := make(chan map[string][]int)
	go destClient.GetSubjectsWithVersions(destChan)
	destSubjects := <- destChan
	close(destChan)

	if (len(destSubjects) != 0 && client.RunMode != "SYNC") {
		log.Println("You have existing subjects registered in this registry, exporter cannot write schemas when " +
			"previous schemas exist in batch mode.")
		os.Exit(0)
	}

	if (!destClient.IsImportModeReady()){

		fmt.Println("Destination Schema Registry is not set to IMPORT mode!")
		fmt.Println("------------------------------------------------------")
		fmt.Println("Set to import mode? (Y/n)")

		var text string

		_, err := fmt.Scanln(&text)
		if err != nil {
			log.Fatal(err)
		}

		if (text == "Y") {
			err := destClient.SetMode("IMPORT")
			if err == false {
				log.Println("Could not set destination registry to IMPORT ModeRecord.")
				os.Exit(0)
			}
		} else {
			log.Println("Cannot export schemas if destination is not set to IMPORT ModeRecord.")
			os.Exit(0)
		}
	}

	if (client.RunMode == "SYNC") {
		client.Sync(srcClient,destClient)
	}
	if (client.RunMode == "BATCH") {
		client.BatchExport(srcClient,destClient)
	}

	log.Println("-----------------------------------------------")

	log.Println("Resetting target to READWRITE")
	destClient.SetMode("READWRITE")

	log.Println("All Done! Thanks for using ccloud-schema-exporter!")

}
