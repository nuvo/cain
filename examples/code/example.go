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
	tag, err := cain.Backup(namespace, selector, container, keyspace, dst, parallel)
	if err != nil {
		log.Fatal(err)
	}

	// Restore
	src := "s3://bucket/cassandra/namespace/cluster-name"
	if err := cain.Restore(src, keyspace, tag, namespace, selector, container, parallel); err != nil {
		log.Fatal(err)
	}

	// Schema
	schema, sum, err := cain.Schema(namespace, selector, container, keyspace)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("sum: %s", sum)
	fmt.Println(schema)
}
