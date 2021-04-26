package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type SchemaLoader struct {
	dstClient		*SchemaRegistryClient
	schemasType		SchemaType			// Define the Loader Type
	schemaRecords	map[SchemaDescriptor]map[int64]map[string]interface{}	// Internal map of SchemaDescriptor -> version -> unstructured schema
	path			string
}

type SchemaDescriptor struct {
	namespace	string
	name		string
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
"null" :{}, "boolean" : {}, "int" : {}, "long" : {}, "float" : {}, "double" : {} , "bytes" : {} ,
"string" : {}, "record" : {}, "enum" : {}, "array" : {}, "map" : {}, "decimal" : {}, "fixed" : {}, "uuid" : {},
"date" : {}, "time-millis" : {}, "time-micros" : {}, "timestamp-millis" : {}, "timestamp-micros" : {},
"local-timestamp-millis" : {}, "local-timestamp-micros" : {}, "duration" : {},
}

/*
	A SchemaLoader takes in a SchemaType and a path and will load the schemas in the path onto Schema Registry.
	Schema Loaders take in the schemas in their natural form, and registers them, this means that if the underlying
	schema
 */
func NewSchemaLoader (schemaType string, dstClient *SchemaRegistryClient, givenPath string, workingDirectory string) *SchemaLoader {
	if strings.EqualFold(schemaType,AVRO.String()) {
		return &SchemaLoader{
			dstClient:		dstClient,
			schemasType:  	AVRO,
			schemaRecords: 	map[SchemaDescriptor]map[int64]map[string]interface{}{},
			path:			CheckPath(givenPath,workingDirectory),
		}
	} else if strings.EqualFold(schemaType,PROTOBUF.String()) {
		log.Fatalln("The Protobuf schema load is not supported yet.")
	} else if strings.EqualFold(schemaType, JSON.String()) {
		log.Fatalln("The Json schema load is not supported yet.")
	}

	log.Fatalln("This type of schema load is not supported, and there are no plans for support.")
	return nil
}

func (sl *SchemaLoader) Run () {
	listenForInterruption()
	sl.loadFromPath()

	if sl.schemasType == AVRO {
		for schemaDesc, schemaVersions := range sl.schemaRecords {
			if CancelRun == true {
				return
			}
			for _, fullSchema := range schemaVersions {
				sl.maybeRegisterAvroSchema(schemaDesc, fullSchema)
			}
		}
	}
}

func (sl *SchemaLoader) loadFromPath () {

	if sl.schemasType == AVRO {
		err := filepath.WalkDir(sl.path, sl.loadAvroFiles)
		check(err)
	}

	if CancelRun != true {
		log.Println("Successfully read schemas")
	} else {
		log.Println("Interrupted while reading schemas")
	}

}

func (sl *SchemaLoader) maybeRegisterAvroSchema(desc SchemaDescriptor, fullSchema map[string]interface{}) bool {

	thisSchemaName := fmt.Sprintf("%s.%s", desc.namespace, desc.name)
	thisSchemaReferences := []SchemaReference{}

	// Check there are fields to the schema
	if fullSchema["fields"] != nil {
		thisSchemaReferences = sl.getReferencesForAvroSchema(thisSchemaName, fullSchema["fields"])
	}

	mapAsJsonBytes, err := json.Marshal(fullSchema)
	check(err)
	mapAsJsonString := string(mapAsJsonBytes)

	if !sl.dstClient.schemaIsRegisteredUnderSubject(thisSchemaName,
		"AVRO", mapAsJsonString, thisSchemaReferences) && checkSubjectIsAllowed(thisSchemaName){
		log.Print("Registering schema with name: " + thisSchemaName)
		sl.dstClient.RegisterSchema(
			mapAsJsonString,
			thisSchemaName+"-value",
			"AVRO",
			thisSchemaReferences)
	}
	return true
}

func (sl *SchemaLoader) loadAvroFiles(path string, info os.DirEntry, err error) error {
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

func (sl *SchemaLoader) getReferencesForAvroSchema (thisSchemaName string, fields interface{}) []SchemaReference {
	references := []SchemaReference{}

	switch theseFields := fields.(type) {
	case []interface{}:
		for _, oneField := range theseFields {
			switch typecastedField := oneField.(type) {
			case map[string]interface{}:
				_, existsType := typecastedField["type"]
				_, existsValues := typecastedField["values"]
				_, existsItems := typecastedField["items"]

				fieldToSwitch := ""

				if existsItems {
					fieldToSwitch = "items"
				} else if existsValues {
					fieldToSwitch = "values"
				} else if existsType {
					fieldToSwitch = "type"
				}

				switch typeOfTypeField := typecastedField[fieldToSwitch].(type) {
				case string:
					_, exists := nativeTypes[typeOfTypeField]
					if !exists {
						// If this is a reference, then fill array and register that first
						//Build SchemaDescriptor for the reference
						thisReferenceDescriptor := GetAvroSchemaDescriptor(typeOfTypeField)
						schemaFullName := fmt.Sprintf("%s.%s", thisReferenceDescriptor.namespace, thisReferenceDescriptor.name)


						versions, refExists := sl.schemaRecords[thisReferenceDescriptor]
						if !refExists {
							log.Fatalln("Reference doesn't exist: " + fmt.Sprintf("%v",thisReferenceDescriptor))
						}
						latestVersionForReference := int64(len(versions))

						for versionNum, _ := range versions {
							log.Println("Register schema reference: " +
								fmt.Sprintf("%s.%s",thisReferenceDescriptor.namespace, thisReferenceDescriptor.name) +
								" for schema " + thisSchemaName)
							sl.maybeRegisterAvroSchema(thisReferenceDescriptor, sl.schemaRecords[thisReferenceDescriptor][versionNum])
						}

						thisReference := SchemaReference{
							Name:    schemaFullName,             // The type referenced
							Subject: schemaFullName + "-value",  // The SchemaDescriptor
							Version: latestVersionForReference, // Latest version of schema descriptor
						}

						if !referenceIsInSlice(thisReference, references) {
							references = append(references, thisReference)
						}

					}
				case []interface{}:
					for _, singleTypeInArray := range typeOfTypeField {
						switch thisFieldType := singleTypeInArray.(type) {
						case string:
							_, exists := nativeTypes[thisFieldType]
							if !exists {
								// If this is a reference, then fill array and register that first
								//Build SchemaDescriptor for the reference
								thisReferenceDescriptor := GetAvroSchemaDescriptor(thisFieldType)
								schemaFullName := fmt.Sprintf("%s.%s", thisReferenceDescriptor.namespace, thisReferenceDescriptor.name)

								versions, refExists := sl.schemaRecords[thisReferenceDescriptor]
								if !refExists {
									log.Fatalln("Reference doesn't exist: " + fmt.Sprintf("%v",thisReferenceDescriptor))
								}
								latestVersionForReference := int64(len(versions)-1)

								for versionNum, _ := range versions {
									log.Println("Register schema reference: " +
										fmt.Sprintf("%s.%s",thisReferenceDescriptor.namespace, thisReferenceDescriptor.name) +
										" for schema " + thisSchemaName)
									sl.maybeRegisterAvroSchema(thisReferenceDescriptor, sl.schemaRecords[thisReferenceDescriptor][versionNum])
								}

								thisReference := SchemaReference{
									Name:    schemaFullName,             // The type referenced
									Subject: schemaFullName + "-value",  // The SchemaDescriptor
									Version: latestVersionForReference, // Latest version of schema descriptor
								}

								if !referenceIsInSlice(thisReference, references) {
									references = append(references, thisReference)
								}

							}
						default:
							log.Println("Could not cast a type to string: " + fmt.Sprintf("%v", thisFieldType))
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

