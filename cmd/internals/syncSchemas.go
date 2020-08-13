package client

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
)

func Sync (srcClient SchemaRegistryClient, destClient SchemaRegistryClient) {

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


		if (!reflect.DeepEqual(srcSubjects,destSubjects)) {
			fmt.Println("Source registry has values that Destination does not, syncing...")
			diff := GetSubjectDiff(srcSubjects,destSubjects)
			for subject, versions := range diff {
				for _, v := range versions {
					schema := srcClient.GetSchema(subject, int64(v))
					fmt.Println("Registering new schema: " + schema.Subject +
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

		syncDuration := time.Since(beginSync)
		fmt.Println("Finished sync in " + strconv.FormatInt(syncDuration.Milliseconds(),10) + " ms")

		time.Sleep(time.Duration(ScrapeInterval) * time.Second)
	}

}

func GetSubjectDiff ( m1 map[string][]int, m2 map[string][]int) map[string][]int {
	diffMap := map[string][]int{}
	for subject, versions := range m1 {
		if (m2[subject] != nil) {
			aDiff := GetVersionsDiff(m1[subject],m2[subject])
			diffMap[subject] = aDiff
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