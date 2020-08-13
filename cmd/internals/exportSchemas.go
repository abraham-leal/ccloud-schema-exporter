package client

import (
	"fmt"
	"strconv"
)

func BatchExport (srcClient SchemaRegistryClient, destClient SchemaRegistryClient) {
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
	
}
