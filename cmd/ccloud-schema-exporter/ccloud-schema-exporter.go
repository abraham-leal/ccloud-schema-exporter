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
	"strings"
)

var factory = map[string]client.CustomDestination{
	"sampleCustomDestination": client.NewSampleCustomDestination(),
	// Add here a mapping of name -> factory/empty struct for reference at runtime
	// See sample above for the built-in sample custom destination that is within the client package
}

func main() {

	client.GetFlags()

	srcClient := client.NewSchemaRegistryClient(client.SrcSRUrl, client.SrcSRKey, client.SrcSRSecret, "src")
	if !srcClient.IsReachable() {
		log.Fatalln("Could not reach source registry. Possible bad credentials?")
	}

	if client.CustomDestinationName != "" {

		if client.ThisRun == client.BATCH {
			client.RunCustomDestinationBatch(srcClient, factory[client.CustomDestinationName])
		}
		if client.ThisRun == client.SYNC {
			client.RunCustomDestinationSync(srcClient, factory[client.CustomDestinationName])
		}
		log.Println("-----------------------------------------------")
		log.Println("All Done! Thanks for using ccloud-schema-exporter!")

		os.Exit(0)
	}

	if client.ThisRun == client.LOCAL {
		workingDir, err := os.Getwd()
		if err != nil {
			log.Fatalln("Could not get execution path. Possibly a permissions issue.")
		}
		client.WriteToFS(srcClient, client.PathToWrite, workingDir)

		log.Println("-----------------------------------------------")
		log.Println("All Done! Thanks for using ccloud-schema-exporter!")

		os.Exit(0)
	}

	destClient := client.NewSchemaRegistryClient(client.DestSRUrl, client.DestSRKey, client.DestSRSecret, "dst")
	if !destClient.IsReachable() {
		log.Println("Could not reach destination registry. Possible bad credentials?")
		os.Exit(0)
	}

	destChan := make(chan map[string][]int64)
	go destClient.GetSubjectsWithVersions(destChan)
	destSubjects := <-destChan
	close(destChan)

	if len(destSubjects) != 0 && client.ThisRun != client.SYNC && !client.NoPrompt {
		log.Println("You have existing subjects registered in the destination registry, exporter cannot write schemas when " +
			"previous schemas exist in batch mode.")
		os.Exit(0)
	}

	if !destClient.IsImportModeReady() && !client.NoPrompt {

		fmt.Println("Destination Schema Registry is not set to IMPORT mode!")
		fmt.Println("------------------------------------------------------")
		fmt.Println("Set to import mode? (Y/n)")

		var text string

		_, err := fmt.Scanln(&text)
		if err != nil {
			log.Fatal(err)
		}

		if strings.EqualFold(text, "Y") {
			err := destClient.SetMode(client.IMPORT)
			if err == false {
				log.Println("Could not set destination registry to IMPORT Mode.")
				os.Exit(0)
			}
		} else {
			log.Println("Cannot export schemas if destination is not set to IMPORT Mode")
			os.Exit(0)
		}
	}

	if !destClient.IsCompatReady() && !client.NoPrompt {

		fmt.Println("Destination Schema Registry is not set to NONE global compatibility level!")
		fmt.Println("We assume the source to be maintaining correct compatibility between registrations, per subject compatibility changes are not supported.")
		fmt.Println("------------------------------------------------------")
		fmt.Println("Set to NONE? (Y/n)")

		var text string

		_, err := fmt.Scanln(&text)
		if err != nil {
			log.Fatal(err)
		}

		if strings.EqualFold(text, "Y") {
			err := destClient.SetGlobalCompatibility(client.NONE)
			if err == false {
				log.Fatalln("Could not set destination registry to Global NONE Compatibility Level.")
			}
		} else {
			log.Println("Continuing without NONE Global Compatibility Level. Note this might arise some failures in registration of some schemas.")
		}
	}

	if (!strings.HasSuffix(srcClient.SRUrl, "confluent.cloud") ||
		!strings.HasSuffix(destClient.SRUrl, "confluent.cloud")) &&
		client.ThisRun == client.SYNC && client.SyncHardDeletes && !client.NoPrompt {

		fmt.Println("It looks like you are trying to sync hard deletions between non-Confluent Cloud Schema Registries")
		fmt.Println("Starting v1.1, ccloud-schema-exporter only supports hard deletion sync between Confluent Cloud Schema Registries")
		fmt.Println("------------------------------------------------------")
		fmt.Println("Do you wish to continue? (Y/n)")

		var text string

		_, err := fmt.Scanln(&text)
		if err != nil {
			log.Fatal(err)
		}

		if !strings.EqualFold(text, "Y") {
			os.Exit(0)
		}
	}

	if client.ThisRun == client.SYNC {
		client.Sync(srcClient, destClient)
	}
	if client.ThisRun == client.BATCH {
		client.BatchExport(srcClient, destClient)
	}

	log.Println("-----------------------------------------------")

	if client.ThisRun == client.BATCH {
		log.Println("Resetting target to READWRITE")
		destClient.SetMode(client.READWRITE)
	}

	log.Println("All Done! Thanks for using ccloud-schema-exporter!")

}
