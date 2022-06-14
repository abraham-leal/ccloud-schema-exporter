package testingUtils

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
)

var cpTestVersion = "7.1.1"
var Ctx = context.Background()

func GetBaseInfra(networkName string) (zooContainer testcontainers.Container, kafkaContainer testcontainers.Container, schemaRegistryContainer testcontainers.Container) {
	var network = testcontainers.NetworkRequest{
		Name:   networkName,
		Driver: "bridge",
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := provider.GetNetwork(Ctx, network); err != nil {
		if _, err := provider.CreateNetwork(Ctx, network); err != nil {
			log.Fatal(err)
		}
	}

	zooContainer, err = testcontainers.GenericContainer(Ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "confluentinc/cp-zookeeper:" + cpTestVersion,
				ExposedPorts: []string{"2181/tcp"},
				WaitingFor:   wait.ForListeningPort("2181/tcp"),
				Name:         "zookeeperclients",
				Env: map[string]string{
					"ZOOKEEPER_CLIENT_PORT": "2181",
					"ZOOKEEPER_TICK_TIME":   "2000",
				},
				Networks: []string{networkName},
			},
			Started: true,
		})
	CheckFail(err, "Zookeeper was not able to start")

	kafkaContainer, err = testcontainers.GenericContainer(Ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "confluentinc/cp-server:" + cpTestVersion,
				ExposedPorts: []string{"29092/tcp"},
				WaitingFor:   wait.ForListeningPort("29092/tcp"),
				Name:         "brokerclients",
				Env: map[string]string{
					"KAFKA_BROKER_ID":                                   "1",
					"KAFKA_ZOOKEEPER_CONNECT":                           "zookeeperclients:2181",
					"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":              "PLAINTEXT:PLAINTEXT",
					"KAFKA_ADVERTISED_LISTENERS":                        "PLAINTEXT://brokerclients:29092",
					"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":            "1",
					"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":            "0",
					"KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR":  "1",
					"KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR": "1",
					"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":               "1",
					"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR":    "1",
				},
				Networks: []string{networkName},
			},
			Started: true,
		})
	CheckFail(err, "Kafka was not able to start")

	schemaRegistryContainer, err = testcontainers.GenericContainer(Ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "confluentinc/cp-schema-registry:" + cpTestVersion,
				ExposedPorts: []string{"8081/tcp"},
				WaitingFor:   wait.ForListeningPort("8081/tcp"),
				Name:         "schema-registry-src-clients",
				Env: map[string]string{
					"SCHEMA_REGISTRY_HOST_NAME":                           "schema-registry-src",
					"SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID":            "schema-src",
					"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS":        "brokerclients:29092",
					"SCHEMA_REGISTRY_KAFKASTORE_TOPIC":                    "_schemas",
					"SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR": "1",
					"SCHEMA_REGISTRY_MODE_MUTABILITY":                     "true",
				},
				Networks: []string{networkName},
			},
			Started: true,
		})
	CheckFail(err, "Source SR was not able to start")

	return zooContainer, kafkaContainer, schemaRegistryContainer
}

// Simple check function that will fail all if there is an error present and allows a custom message to be printed
func CheckFail(e error, msg string) {
	if e != nil {
		log.Println(e)
		log.Fatalln(msg)
	}
}
