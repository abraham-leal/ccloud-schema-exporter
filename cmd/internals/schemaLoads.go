package client

import (
	"log"
)

type SchemaLoader struct {
	schemasType		SchemaType
	schemaRecords	map[string]string
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

/*
	A SchemaLoader takes in a SchemaType and a path and will load the schemas in the path onto Schema Registry.
	Schema Loaders take in the schemas in their natural form, and registers them, this means that if the underlying
	schema
 */
func NewSchemaLoader (schemaType SchemaType) *SchemaLoader {
	if schemaType == AVRO {
		return &SchemaLoader{
			schemasType:  AVRO,
			schemaRecords: map[string]string{},
		}
	} else if schemaType == PROTOBUF {
		log.Fatalln("The Protobuf schema load is not supported yet.")
	} else if schemaType == JSON {
		log.Fatalln("The Json schema load is not supported yet.")
	}

	log.Fatalln("This type of schema load is not supported, and there are no plans for support.")
	return nil
}

func (sl *SchemaLoader) run () {

}

func (sl *SchemaLoader) loadFromPath (givenPath string) {

}

func (sl *SchemaLoader) registerSchema (specificPath string) {

}

func LoadAVSC(definedPath string, workingDirectory string) {
	nativeTypes := map[string]struct{}{
		"null" :{}, "boolean" : {}, "int" : {}, "long" : {}, "float" : {}, "double" : {} , "bytes" : {} ,
		"string" : {}, "record" : {}, "enum" : {}, "array" : {}, "map" : {}, "decimal" : {}, "fixed" : {}, "uuid" : {},
		"date" : {}, "time-millis" : {}, "time-micros" : {}, "timestamp-millis" : {}, "timestamp-micros" : {},
		"local-timestamp-millis" : {}, "local-timestamp-micros" : {}, "duration" : {},
	}
	log.Println(nativeTypes)

}

