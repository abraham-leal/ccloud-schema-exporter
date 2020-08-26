package client

import (
	"reflect"
	"strconv"
	"time"
	"log"
)

func Sync (srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {

	//Begin sync
	for {
		beginSync := time.Now()

		srcSubjects := make (map[string][]int)
		destSubjects := make (map[string][]int)

		srcChan := make(chan map[string][]int)
		destChan := make(chan map[string][]int)

		go srcClient.GetSubjectsWithVersions(srcChan)
		go destClient.GetSubjectsWithVersions(destChan)

		srcSubjects = <- srcChan
		destSubjects = <- destChan

		// Perform SR Sync without worrying about deletes
		if !reflect.DeepEqual(srcSubjects,destSubjects) {
			diff := GetSubjectDiff(srcSubjects,destSubjects)
			if len(diff) != 0 {
				log.Println("Source registry has values that Destination does not, syncing...")
				for subject, versions := range diff {
					for _, v := range versions {
						log.Printf("Subject: %s, Version: %d", subject, v)
						schema := srcClient.GetSchema(subject, int64(v))
						log.Println("Registering new schema: " + schema.Subject +
							" with version: " + strconv.FormatInt(schema.Version, 10) +
							" and ID: " + strconv.FormatInt(schema.Id, 10) +
							" and Type: " + schema.SType)
						destClient.RegisterSchemaBySubjectAndIDAndVersion(schema.Schema,
							schema.Subject,
							int(schema.Id),
							int(schema.Version),
							schema.SType)
					}
				}
			}

			//Perform SR sync taking into account soft and hard deletes
			if syncDeletes {
				diff := GetSubjectDiff(destSubjects,srcSubjects)
				if len(diff) != 0 {
					log.Println("Source registry has deletes that Destination does not, syncing...")
					for subject, versions := range diff {
						for _, v := range versions {
							log.Printf("Subject: %s, Version: %d", subject, v)

							// Retrieve the known schema to be deleted
							schema := destClient.GetSchema(subject, int64(v))
							log.Printf("Soft deleting schema: %s with version: %d and ID: %d and Type: %s",
								schema.Subject, schema.Version, schema.Id, schema.SType)
							destClient.performSoftDelete(subject, v)
						}
					}
				}
			}
		}

		syncDuration := time.Since(beginSync)
		log.Printf("Finished sync in %d ms", syncDuration.Milliseconds())

		time.Sleep(time.Duration(ScrapeInterval) * time.Second)
	}

}

func GetSubjectDiff ( m1 map[string][]int, m2 map[string][]int) map[string][]int {
	diffMap := map[string][]int{}
	for subject, versions := range m1 {
		if m2[subject] != nil {
			versionDiff := GetVersionsDiff(m1[subject],m2[subject])
			if len(versionDiff) != 0 {
				aDiff := versionDiff
				diffMap[subject] = aDiff
			}
		} else {
			diffMap[subject] = versions
		}
	}
	return diffMap
}

func GetVersionsDiff (a1 []int, a2 []int) []int {
	m := map[int]bool{}
	diff := []int{}

	for _, item := range a2 {
		m[item] = true
	}

	for _, item := range a1 {
		if _, ok := m[item]; !ok {
			diff = append(diff, item)
		}
	}
	return diff
}