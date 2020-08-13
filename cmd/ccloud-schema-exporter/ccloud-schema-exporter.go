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
		fmt.Println("Could not reach source registry. Possible bad credentials?")
		os.Exit(0);
	}
	destClient := client.NewSchemaRegistryClient(client.DestSRUrl, client.DestSRKey, client.DestSRSecret, "dst")
	if (!destClient.IsReachable()){
		fmt.Println("Could not reach destination registry. Possible bad credentials?")
		os.Exit(0);
	}

	destSubjects := destClient.GetSubjectsWithVersions()

	if (len(destSubjects) != 0 && client.RunMode != "SYNC") {
		fmt.Println("You have existing subjects registered in this registry, exporter cannot write schemas when " +
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
				fmt.Println("Could not set destination registry to IMPORT ModeRecord.")
				os.Exit(0)
			}
		} else {
			fmt.Println("Cannot export schemas if destination is not set to IMPORT ModeRecord.")
			os.Exit(0)
		}
	}

	if (client.RunMode == "SYNC") {
		client.Sync(srcClient,destClient)
	}
	if (client.RunMode == "BATCH") {
		client.BatchExport(srcClient,destClient)
	}

	fmt.Println("")
	fmt.Println("-----------------------------------------------")

	fmt.Println("Resetting target to READWRITE")
	destClient.SetMode("READWRITE")

	fmt.Println("All Done! Thanks for using ccloud-schema-exporter!")

}
