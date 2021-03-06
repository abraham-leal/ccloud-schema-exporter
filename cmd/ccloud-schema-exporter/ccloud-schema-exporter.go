package main

//
// ccloud-schema-exporter.go
// Copyright 2020 Abraham Leal
//

import (
	"fmt"
	"github.com/abraham-leal/ccloud-schema-exporter/cmd/internals"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"os"
	"strings"
)

var sampleDestObject = client.NewSampleCustomDestination()
var customDestFactory = map[string]client.CustomDestination{
	"sampleCustomDestination": &sampleDestObject,
	// Add here a mapping of name -> customDestFactory/empty struct for reference at runtime
	// See sample above for the built-in sample custom destination that is within the client package
}
var apicurioObject = client.NewApicurioSource()
var customSrcFactory = map[string]client.CustomSource{
	"sampleCustomSourceApicurio": &apicurioObject,
	// Add here a mapping of name -> customSrcFactory/empty struct for reference at runtime
	// See sample above for the built-in sample custom source that is within the client package
}

func main() {

	client.GetFlags()

	if client.WithMetrics {
		log.Println("Starting exposure of metrics on :9020/metrics")
		http.Handle("/metrics", promhttp.Handler())

		go func() {
			err := http.ListenAndServe(":9020", nil)
			if err != nil {
				log.Println("Could not start metrics endpoint")
				log.Println(err)
				log.Println("Continuing without exposing metrics")
			}
		}()

	}

	if client.CustomSourceName != "" {

		destClient := client.NewSchemaRegistryClient(client.DestSRUrl, client.DestSRKey, client.DestSRSecret, "dst")
		if !client.NoPrompt {
			preflightWriteChecks(destClient)
		}

		if client.ThisRun == client.BATCH {
			client.RunCustomSourceBatch(destClient, customSrcFactory[client.CustomSourceName])
		}
		if client.ThisRun == client.SYNC {
			client.RunCustomSourceSync(destClient, customSrcFactory[client.CustomSourceName])
		}
		log.Println("-----------------------------------------------")
		log.Println("All Done! Thanks for using ccloud-schema-exporter!")

		os.Exit(0)
	}

	if client.ThisRun == client.FROMLOCAL {
		workingDir, err := os.Getwd()
		if err != nil {
			log.Fatalln("Could not get execution path. Possibly a permissions issue.")
		}

		destClient := client.NewSchemaRegistryClient(client.DestSRUrl, client.DestSRKey, client.DestSRSecret, "dst")
		if !client.NoPrompt {
			preflightWriteChecks(destClient)
		}

		client.WriteFromFS(destClient, client.PathToWrite, workingDir)

		log.Println("-----------------------------------------------")
		log.Println("All Done! Thanks for using ccloud-schema-exporter!")

		os.Exit(0)
	}

	srcClient := client.NewSchemaRegistryClient(client.SrcSRUrl, client.SrcSRKey, client.SrcSRSecret, "src")
	if !srcClient.IsReachable() {
		log.Fatalln("Could not reach source registry. Possible bad credentials?")
	}

	if client.CustomDestinationName != "" {

		if client.ThisRun == client.BATCH {
			client.RunCustomDestinationBatch(srcClient, customDestFactory[client.CustomDestinationName])
		}
		if client.ThisRun == client.SYNC {
			client.RunCustomDestinationSync(srcClient, customDestFactory[client.CustomDestinationName])
		}
		log.Println("-----------------------------------------------")
		log.Println("All Done! Thanks for using ccloud-schema-exporter!")

		os.Exit(0)
	}

	if client.ThisRun == client.TOLOCAL {
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
	if !client.NoPrompt {
		preflightWriteChecks(destClient)
	}

	if (!strings.HasSuffix(srcClient.SRUrl, "confluent.cloud") ||
		!strings.HasSuffix(destClient.SRUrl, "confluent.cloud")) &&
		client.ThisRun == client.SYNC && client.SyncHardDeletes && !client.NoPrompt {

		fmt.Println("It looks like you are trying to sync hard deletions between non-Confluent Cloud Schema Registries")
		fmt.Println("Starting v1.1, ccloud-schema-exporter only supports hard deletion sync between Confluent Cloud Schema Registries, or Confluent Platform 6.1+")
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

func preflightWriteChecks(destClient *client.SchemaRegistryClient) {

	if !destClient.IsReachable() {
		log.Println("Could not reach destination registry. Possible bad credentials?")
		os.Exit(0)
	}

	destSubjects := client.GetCurrentSubjectState(destClient)
	if len(destSubjects) != 0 && client.ThisRun != client.SYNC {
		log.Println("You have existing subjects registered in the destination registry, exporter cannot write schemas when " +
			"previous schemas exist in batch mode.")
		os.Exit(0)
	}

	if !destClient.IsImportModeReady() {

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

	if !destClient.IsCompatReady() {

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
}
