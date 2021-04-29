package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type SchemaLoader struct {
	dstClient     *SchemaRegistryClient
	schemasType   SchemaType                                            // Define the Loader Type
	schemaRecords map[SchemaDescriptor]map[int64]map[string]interface{} // Internal map of SchemaDescriptor -> version -> unstructured schema
	path          string
}

type SchemaDescriptor struct {
	namespace string
	name      string
}

// Define SchemaType enum
type SchemaType int

const (
	AVRO SchemaType = iota
	PROTOBUF
	JSON
)

func (st SchemaType) String() string {
	return [...]string{"AVRO", "PROTOBUF", "JSON"}[st]
}

// Types that do not need further resolution
var nativeTypes = map[string]struct{}{
	"null": {}, "boolean": {}, "int": {}, "long": {}, "float": {}, "double": {}, "bytes": {},
	"string": {}, "record": {}, "enum": {}, "array": {}, "map": {}, "decimal": {}, "fixed": {}, "uuid": {},
	"date": {}, "time-millis": {}, "time-micros": {}, "timestamp-millis": {}, "timestamp-micros": {},
	"local-timestamp-millis": {}, "local-timestamp-micros": {}, "duration": {},
}

/*
	A SchemaLoader takes in a SchemaType and a path and will load the schemas in the path onto Schema Registry.
	Schema Loaders take in the schemas in their natural form, and registers them, this means that if the underlying
	schema
*/
func NewSchemaLoader(schemaType string, dstClient *SchemaRegistryClient, givenPath string, workingDirectory string) *SchemaLoader {
	if strings.EqualFold(schemaType, AVRO.String()) {
		return &SchemaLoader{
			dstClient:     dstClient,
			schemasType:   AVRO,
			schemaRecords: map[SchemaDescriptor]map[int64]map[string]interface{}{},
			path:          CheckPath(givenPath, workingDirectory),
		}
	} else if strings.EqualFold(schemaType, PROTOBUF.String()) {
		log.Fatalln("The Protobuf schema load is not supported yet.")
	} else if strings.EqualFold(schemaType, JSON.String()) {
		log.Fatalln("The Json schema load is not supported yet.")
	}

	log.Fatalln("This type of schema load is not supported, and there are no plans for support.")
	return nil
}

func (sl *SchemaLoader) Run() {
	listenForInterruption()
	sl.loadFromPath()

	if sl.schemasType == AVRO {
		for schemaDesc, schemaVersions := range sl.schemaRecords {
			if CancelRun == true {
				return
			}
			versions := make([]int64, 0)
			for versionNumber, _ := range schemaVersions {
				versions = append(versions, versionNumber)
			}
			sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
			for _, sortedVersion := range versions {
				sl.maybeRegisterAvroSchema(schemaDesc, sortedVersion, schemaVersions[sortedVersion])
			}
		}
	}
}

func (sl *SchemaLoader) loadFromPath() {

	if sl.schemasType == AVRO {
		err := filepath.Walk(sl.path, sl.loadAvroFiles)
		check(err)
	}

	if CancelRun != true {
		log.Println("Successfully read schemas")
	} else {
		log.Println("Interrupted while reading schemas")
	}

}

func (sl *SchemaLoader) maybeRegisterAvroSchema(desc SchemaDescriptor, version int64, fullSchema map[string]interface{}) bool {

	thisSchemaName := fmt.Sprintf("%s.%s", desc.namespace, desc.name)
	thisSchemaReferences := []SchemaReference{}

	// Check there are fields to the schema
	if fullSchema["fields"] != nil {
		thisSchemaReferences = sl.getReferencesForAvroSchema(thisSchemaName, fullSchema["fields"])
	}

	mapAsJsonBytes, err := json.Marshal(fullSchema)
	check(err)
	mapAsJsonString := string(mapAsJsonBytes)

	thisSchemaSubject := thisSchemaName+"-value"

	if checkSubjectIsAllowed(thisSchemaName) && !sl.dstClient.schemaIsRegisteredUnderSubject(thisSchemaSubject,
		"AVRO", mapAsJsonString, thisSchemaReferences) {
		log.Println(fmt.Sprintf("Registering schema not previously registered: %s with version: %d", thisSchemaSubject, version))
		sl.dstClient.RegisterSchema(
			mapAsJsonString,
			thisSchemaSubject,
			"AVRO",
			thisSchemaReferences)
		return true
	}
	return false
}

func (sl *SchemaLoader) loadAvroFiles(path string, info os.FileInfo, err error) error {
	check(err)

	if !info.IsDir() {
		currentSchema, err := os.Open(path)
		checkDontFail(err)
		defer currentSchema.Close()

		jsonBytes, _ := ioutil.ReadAll(currentSchema)

		var schemaStruct map[string]interface{}
		json.Unmarshal(jsonBytes, &schemaStruct)

		thisSchemaDescription := SchemaDescriptor{
			namespace: fmt.Sprintf("%v", schemaStruct["namespace"]),
			name:      fmt.Sprintf("%v", schemaStruct["name"]),
		}

		thisSchemaVersion := int64(len(sl.schemaRecords[thisSchemaDescription]))

		if thisSchemaVersion == 0 {
			sl.schemaRecords[thisSchemaDescription] = map[int64]map[string]interface{}{
				thisSchemaVersion: schemaStruct,
			}
		} else {
			newVersion := sl.schemaRecords[thisSchemaDescription]
			newVersion[thisSchemaVersion] = schemaStruct
			sl.schemaRecords[thisSchemaDescription] = newVersion
		}

	}
	return nil
}

func (sl *SchemaLoader) getReferencesForAvroSchema(thisSchemaName string, fields interface{}) []SchemaReference {
	references := []SchemaReference{}

	switch theseFields := fields.(type) {
	case []interface{}:
		for _, oneField := range theseFields {
			switch typecastedField := oneField.(type) {
			case map[string]interface{}:
				switch typeOfTypeField := typecastedField["type"].(type) {
				case string:
					sl.resolveReferenceAndRegister(typeOfTypeField, &references)
				case []interface{}:
					for _, singleTypeInArray := range typeOfTypeField {
						switch thisFieldType := singleTypeInArray.(type) {
						case string:
							sl.resolveReferenceAndRegister(thisFieldType, &references)
						default:
							log.Println("Could not cast a type to string: " + fmt.Sprintf("%v", thisFieldType))
						}
					}
				case map[string]interface{}:
					values, valuesExist := typeOfTypeField["values"]
					items, itemsExist := typeOfTypeField["items"]
					if valuesExist && !itemsExist {
						switch valuesFields := values.(type) {
						case string:
							sl.resolveReferenceAndRegister(valuesFields, &references)
						case []interface{}:
							for _, singleTypeInArray := range valuesFields {
								switch thisFieldType := singleTypeInArray.(type) {
								case string:
									sl.resolveReferenceAndRegister(thisFieldType, &references)
								default:
									log.Println("Could not cast a type to string: " + fmt.Sprintf("%v", thisFieldType))
								}
							}
						default:
							log.Println("Could not parse avro array: " + fmt.Sprintf("%v", valuesFields))
						}
					}
					if itemsExist && !valuesExist {
						switch itemsTypes := items.(type) {
						case string:
							sl.resolveReferenceAndRegister(itemsTypes, &references)
						case []interface{}:
							for _, singleTypeInArray := range itemsTypes {
								switch thisFieldType := singleTypeInArray.(type) {
								case string:
									sl.resolveReferenceAndRegister(thisFieldType, &references)
								default:
									log.Println("Could not cast a type to string: " + fmt.Sprintf("%v", thisFieldType))
								}
							}
						default:
							log.Println("Could not parse avro map: " + fmt.Sprintf("%v", itemsTypes))
						}
					}
				default:
					log.Println("Could not get types from schema")
					log.Println(fmt.Sprintf("%v", typeOfTypeField))
				}
			default:
				log.Println("Could not typecast input")
			}
		}
	default:
		log.Println("Could not define the field array in this schema")
		log.Println(fmt.Sprintf("%v", theseFields))
	}
	return references
}

func (sl *SchemaLoader) resolveReferenceAndRegister(referenceName string, references *[]SchemaReference) {
	// If this is a reference, then fill array and register that first
	//Build SchemaDescriptor for the reference
	_, exists := nativeTypes[referenceName]
	if !exists {
		thisReferenceDescriptor := GetAvroSchemaDescriptor(referenceName)
		schemaFullName := fmt.Sprintf("%s.%s", thisReferenceDescriptor.namespace, thisReferenceDescriptor.name)

		versions, refExists := sl.schemaRecords[thisReferenceDescriptor]
		if !refExists {
			log.Fatalln("Reference doesn't exist: " + fmt.Sprintf("%v", thisReferenceDescriptor))
		}
		latestVersionForReference := int64(len(versions))

		sl.registerReferenceSet(versions, thisReferenceDescriptor)

		thisReference := SchemaReference{
			Name:    schemaFullName,              // The type referenced
			Subject: schemaFullName + "-value", // The SchemaDescriptor
			Version: latestVersionForReference,   // Latest version of schema descriptor
		}

		if !referenceIsInSlice(thisReference, *references) {
			*references = append(*references, thisReference)
		}
	}
}

func (sl *SchemaLoader) registerReferenceSet(versionsMap map[int64]map[string]interface{}, descriptor SchemaDescriptor) {
	versionsSlice := make([]int64, 0)
	for versionNumber, _ := range versionsMap {
		versionsSlice = append(versionsSlice, versionNumber)
	}
	sort.Slice(versionsSlice, func(i, j int) bool { return versionsSlice[i] < versionsSlice[j] })

	for _, sortedVersion := range versionsSlice {
		if sl.maybeRegisterAvroSchema(descriptor, sortedVersion, sl.schemaRecords[descriptor][sortedVersion]) {
			log.Println(fmt.Sprintf("Registered schema reference not previously registered: %s.%s with version: %d",
				descriptor.namespace, descriptor.name, sortedVersion))
		}
	}
}
