package client

//
// context.go
// Author: Abraham Leal
//

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func GetFlags() {

	flag.StringVar(&SrcSRUrl, "src-sr-url", "", "Url to the Source Schema Registry Cluster")
	flag.StringVar(&SrcSRKey, "src-sr-key", "", "API KEY for the Source Schema Registry Cluster")
	flag.StringVar(&SrcSRSecret, "src-sr-secret", "", "API SECRET for the Source Schema Registry Cluster")
	flag.StringVar(&DestSRUrl, "dest-sr-url", "", "Url to the Destination Schema Registry Cluster")
	flag.StringVar(&DestSRKey, "dest-sr-key", "", "API KEY for the Destination Schema Registry Cluster")
	flag.StringVar(&DestSRSecret, "dest-sr-secret", "", "API SECRET for the Destination Schema Registry Cluster")
	flag.StringVar(&CustomDestinationName, "customDestination", "", "Name of the implementation to be used as a destination (same as mapping)")
	flag.IntVar(&HttpCallTimeout, "timeout", 60, "Timeout, in seconds, to use for all REST calls with the Schema Registries")
	flag.IntVar(&ScrapeInterval, "scrapeInterval", 60, "Amount of time ccloud-schema-exporter will delay between schema sync checks in seconds")
	flag.StringVar(&PathToWrite, "localPath", "",
		"Optional custom path for local functions. This must be an existing directory structure.")
	flag.Var(&AllowList, "allowList", "A comma delimited list of schema subjects to allow. It also accepts paths to a file containing a list of subjects.")
	flag.Var(&DisallowList, "disallowList", "A comma delimited list of schema subjects to disallow. It also accepts paths to a file containing a list of subjects.")
	versionFlag := flag.Bool("version", false, "Print the current version and exit")
	usageFlag := flag.Bool("usage", false, "Print the usage of this tool")
	batchExportFlag := flag.Bool("batchExport", false, "Perform a one-time export of all schemas")
	syncFlag := flag.Bool("sync", false, "Sync schemas continuously")
	localCopyFlag := flag.Bool("getLocalCopy", false, "Perform a local back-up of all schemas in the source registry. Defaults to a folder (SchemaRegistryBackup) in the current path of the binaries.")
	fromLocalCopyFlag := flag.Bool("fromLocalCopy", false, "Registers all local schemas written by getLocalCopy. Defaults to a folder (SchemaRegistryBackup) in the current path of the binaries.")
	deleteFlag := flag.Bool("deleteAllFromDestination", false, "Setting this will run a delete on all schemas written to the destination registry. No respect for allow/disallow lists.")
	syncDeletesFlag := flag.Bool("syncDeletes", false, "Setting this will sync soft deletes from the source cluster to the destination")
	syncHardDeletesFlag := flag.Bool("syncHardDeletes", false, "Setting this will sync hard deletes from the source cluster to the destination")
	noPromptFlag := flag.Bool("noPrompt", false, "Set this flag to avoid checks while running. Assure you have the destination SR to correct Mode and Compatibility.")

	flag.Parse()

	if *noPromptFlag {
		NoPrompt = true
	}

	if *syncDeletesFlag {
		SyncDeletes = true
	}

	if *syncHardDeletesFlag {
		SyncHardDeletes = true
	}

	if *versionFlag {
		printVersion()
		os.Exit(0)
	}

	if *usageFlag {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *deleteFlag {
		log.Println("Deleting all schemas from DESTINATION registry")
		deleteAllFromDestination(DestSRUrl, DestSRKey, DestSRSecret)
		os.Exit(0)
	}

	if !*syncFlag && !*batchExportFlag && !*localCopyFlag && !*fromLocalCopyFlag {
		fmt.Println("You must specify a mode to run on.")
		fmt.Println("Usage:")
		fmt.Println("")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *fromLocalCopyFlag {
		ThisRun = FROMLOCAL
	}

	if *localCopyFlag {
		ThisRun = TOLOCAL
	}

	if *batchExportFlag {
		ThisRun = BATCH
	}

	if *syncFlag {
		ThisRun = SYNC
	}

}
