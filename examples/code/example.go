package main

import (
	"fmt"
	"log"

	"github.com/nuvo/cain/pkg/cain"
)

func main() {

	namespace := "default"
	selector := "release=cassandra"
	container := "cassandra"
	keyspace := "keyspace"
	parallel := 0 // all at once

	// Backup
	dst := "s3://bucket/cassandra"
	tag, err := cain.Backup(
		cain.BackupOptions{
			Namespace: namespace,
			Selector:  selector,
			Container: container,
			Keyspace:  keyspace,
			Dst:       dst,
			Parallel:  parallel,
		})
	if err != nil {
		log.Fatal(err)
	}

	// Restore
	src := "s3://bucket/cassandra/namespace/cluster-name"
	if err := cain.Restore(cain.RestoreOptions{
		Src:       src,
		Namespace: namespace,
		Selector:  selector,
		Container: container,
		Keyspace:  keyspace,
		Tag:       tag,
		Parallel:  parallel,
	}); err != nil {
		log.Fatal(err)
	}

	// Schema
	schema, sum, err := cain.Schema(
		cain.SchemaOptions{
			Namespace: namespace,
			Selector:  selector,
			Container: container,
			Keyspace:  keyspace,
		})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("sum: %s", sum)
	fmt.Println(schema)
}
