package client

//
// syncSchemas.go
// Copyright 2020 Abraham Leal
//

import (
	"log"
	"reflect"
	"strconv"
	"time"
)

func Sync(srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {

	listenForInterruption()

	// Set up soft Deleted IDs in destination for interpretation by the destination registry
	if SyncDeletes {
		syncExistingSoftDeletedSubjects(srcClient,destClient)
	}

	//Begin sync
	for {
		if CancelRun == true {
			return
		}
		beginSync := time.Now()

		srcSubjects, destSubjects := GetCurrentSubjectsStates(srcClient, destClient)

		if !reflect.DeepEqual(srcSubjects, destSubjects) {
			diff := GetSubjectDiff(srcSubjects, destSubjects)
			// Perform sync
			initialSync(diff, srcClient, destClient)
			//Perform soft delete check
			if SyncDeletes {
				syncSoftDeletes(destSubjects, srcSubjects, destClient)
			}
		}

		// Perform hard delete check
		if SyncHardDeletes {
			syncHardDeletes(srcClient, destClient)
		}

		syncDuration := time.Since(beginSync)
		log.Printf("Finished sync in %d ms", syncDuration.Milliseconds())

		time.Sleep(time.Duration(ScrapeInterval) * time.Second)
	}

}

func initialSync(diff map[string][]int64, srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {
	if len(diff) != 0 {
		log.Println("Source registry has values that Destination does not, syncing...")
		for subject, versions := range diff {
			for _, v := range versions {
				schema := srcClient.GetSchema(subject, v, false)
				log.Println("Registering new schema: " + schema.Subject +
					" with version: " + strconv.FormatInt(schema.Version, 10) +
					" and ID: " + strconv.FormatInt(schema.Id, 10) +
					" and Type: " + schema.SType)
				destClient.RegisterSchemaBySubjectAndIDAndVersion(schema.Schema,
					schema.Subject,
					schema.Id,
					schema.Version,
					schema.SType)
				if WithMetrics {
					schemasRegistered.Inc()
				}
			}
		}
	}
}

func syncSoftDeletes(destSubjects map[string][]int64, srcSubjects map[string][]int64, destClient *SchemaRegistryClient) {
	diff := GetSubjectDiff(destSubjects, srcSubjects)
	if len(diff) != 0 {
		log.Println("Source registry has deletes that Destination does not, syncing...")
		for subject, versions := range diff {
			for _, v := range versions {
				destClient.PerformSoftDelete(subject, v)
				if WithMetrics {
					schemasSoftDeleted.Inc()
				}
			}
		}
	}
}

func syncHardDeletes(srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {
	permDel := GetIDDiff(destClient.GetSoftDeletedIDs(),srcClient.GetSoftDeletedIDs())
	if len(permDel) != 0 {
		for id, subjectVersionsMap := range permDel {
			for subject, versions := range subjectVersionsMap {
				for _, version := range versions {
					log.Printf("Discovered Hard Deleted Schema with ID %d, Subject %s, and Version: %d",
						id, subject, version)
					destClient.PerformHardDelete(subject, version)
					if WithMetrics {
						schemasHardDeleted.Inc()
					}
				}
			}
		}
	}
}

func syncExistingSoftDeletedSubjects (srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {
	softDel := GetIDDiff(srcClient.GetSoftDeletedIDs(),destClient.GetSoftDeletedIDs())
	if len(softDel) != 0 {
		log.Println("There are soft Deleted IDs in the source. Sinking to the destination at startup...")
		for _ , meta := range softDel {
			for sbj, versions := range meta {
				for _, version := range versions {
					softDeletedSchema := srcClient.GetSchema(sbj,version,true)
					destClient.RegisterSchemaBySubjectAndIDAndVersion(softDeletedSchema.Schema,
						softDeletedSchema.Subject,softDeletedSchema.Id,softDeletedSchema.Version,softDeletedSchema.SType)
					destClient.PerformSoftDelete(softDeletedSchema.Subject,softDeletedSchema.Version)
				}
			}
		}
	}
}
