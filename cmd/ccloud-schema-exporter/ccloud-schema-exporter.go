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
	"strconv"
)

func main() {

	client.GetFlags()

	srcClient := client.NewSchemaRegistryClient(client.SrcSRUrl,client.SrcSRKey, client.SrcSRSecret , "src")
	if (!srcClient.IsReachable()){
		fmt.Println("Could not reach source registry. Possible bad credentials?")
		os.Exit(1);
	}
	destClient := client.NewSchemaRegistryClient(client.DestSRUrl, client.DestSRKey, client.DestSRSecret, "dest")
	if (!destClient.IsReachable()){
		fmt.Println("Could not reach destination registry. Possible bad credentials?")
		os.Exit(1);
	}

	destSubjects := destClient.GetSubjectsWithVersions()

	if (len(destSubjects) != 0) {
		fmt.Println("You have existing subjects registered in this registry, exporter cannot write schemas when " +
			"previous schemas exist")
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
				fmt.Println("Could not set destination registry to IMPORT Mode.")
				os.Exit(0)
			}
		} else {
			fmt.Println("Cannot export schemas if destination is not set to IMPORT Mode.")
			os.Exit(0)
		}
	}

	srcSubjects := srcClient.GetSubjectsWithVersions()

	fmt.Println("Registering all schemas from " + srcClient.SRUrl)
	for srcSubject , srcVersions := range srcSubjects {
		for _ , v := range srcVersions {
			schema := srcClient.GetSchema(srcSubject,int64(v))
			fmt.Println("Registering schema: " + schema.Subject +
				" with version: " + strconv.FormatInt(schema.Version,10) +
				" and ID: " + strconv.FormatInt(schema.Id,10) +
				" and Type: " + schema.SType )
			destClient.RegisterSchemaBySubjectAndIDAndVersion(schema.Schema,
				schema.Subject,
				int(schema.Id),
				int(schema.Version),
				schema.SType)
		}
	}


	fmt.Println("Resetting target to READWRITE")
	destClient.SetMode("READWRITE")

	fmt.Println("All Done! Thanks for using ccloud-schema-exporter!")

}
