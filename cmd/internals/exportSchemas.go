package client

//
// exportSchemas.go
// Copyright 2020 Abraham Leal
//

import (
	"log"
)

func BatchExport(srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {
	listenForInterruption()

	srcSubjects := GetCurrentSubjectState(srcClient)

	// Set up soft Deleted IDs in destination for interpretation by the destination registry
	if SyncDeletes {
		syncExistingSoftDeletedSubjects(srcClient,destClient)
	}

	log.Println("Registering all schemas from " + srcClient.SRUrl)
	for srcSubject, srcVersions := range srcSubjects {
		if CancelRun == true {
			return
		}
		for _, v := range srcVersions {
			schema := srcClient.GetSchema(srcSubject, v, false)
			log.Printf("Registering schema: %s with version: %d and ID: %d and Type: %s",
				schema.Subject, schema.Version, schema.Id, schema.SType)
			destClient.RegisterSchemaBySubjectAndIDAndVersion(schema.Schema,
				schema.Subject,
				schema.Id,
				schema.Version,
				schema.SType)
		}
	}
}
