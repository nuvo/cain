package cain

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/maorfr/skbn/pkg/skbn"

	"github.com/maorfr/cain/pkg/utils"
)

// Backup performs backup
func Backup(namespace, selector, container, keyspace, bucket string, parallel int) error {
	log.Println("Backup started!")

	log.Println("Getting clients")
	k8sClient, s3Client, err := skbn.GetClients("k8s", "s3", "", bucket)
	if err != nil {
		return err
	}

	log.Println("Getting pods")
	pods, err := utils.GetPods(k8sClient, namespace, selector)
	if err != nil {
		return err
	}

	log.Println("Backing up schema")
	s3BasePath, err := BackupKeyspaceSchema(k8sClient, s3Client, namespace, pods[0], container, keyspace, bucket)
	if err != nil {
		return err
	}

	log.Println("Taking snapshots")
	tag := TakeSnapshots(k8sClient, pods, namespace, container, keyspace)

	log.Println("Calculating paths. This may take a while...")
	fromToPathsAllPods, err := utils.GetFromAndToPathsFromK8s(k8sClient, pods, namespace, container, keyspace, tag, s3BasePath)
	if err != nil {
		return err
	}

	log.Println("Starting files copy")
	if err := skbn.PerformCopy(k8sClient, s3Client, "k8s", "s3", fromToPathsAllPods, parallel); err != nil {
		return err
	}

	log.Println("Clearing snapshots")
	ClearSnapshots(k8sClient, pods, namespace, container, keyspace, tag)

	log.Println("All done!")
	return nil
}

// Restore performs restore
func Restore(bucket, namespace, cluster, keyspace, tag, toNamespace, selector, container string, parallel int) error {
	log.Println("Restore started!")
	if toNamespace == "" {
		toNamespace = namespace
	}

	log.Println("Getting clients")
	s3Client, k8sClient, err := skbn.GetClients("s3", "k8s", bucket, "")
	if err != nil {
		return err
	}

	log.Println("Getting pods")
	existingPods, err := utils.GetPods(k8sClient, toNamespace, selector)
	if err != nil {
		return err
	}

	log.Println("Getting current schema")
	_, sum, err := DescribeKeyspaceSchema(k8sClient, toNamespace, existingPods[0], container, keyspace)
	if err != nil {
		return err
	}

	log.Println("Calculating paths. This may take a while...")
	s3BasePath := filepath.Join(bucket, "cassandra", namespace, cluster, keyspace, sum, tag)
	fromToPaths, podsToBeRestored, tablesToRefresh, err := utils.GetFromAndToPathsFromS3(s3Client, k8sClient, s3BasePath, toNamespace, container)
	if err != nil {
		return err
	}

	log.Println("Validating pods match restore")
	if err := utils.SliceContainsSlice(podsToBeRestored, existingPods); err != nil {
		return err
	}

	log.Println("Truncating tables")
	TruncateTables(k8sClient, toNamespace, container, keyspace, existingPods, tablesToRefresh)

	log.Println("Starting files copy")
	if err := skbn.PerformCopy(s3Client, k8sClient, "s3", "k8s", fromToPaths, parallel); err != nil {
		return err
	}

	log.Println("Refreshing tables")
	RefreshTables(k8sClient, toNamespace, container, keyspace, podsToBeRestored, tablesToRefresh)

	log.Println("All done!")
	return nil
}

// Schema gets the schema of the cassandra cluster
func Schema(namespace, selector, container, keyspace string, onlySum bool) error {
	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return err
	}
	pods, err := utils.GetPods(k8sClient, namespace, selector)
	if err != nil {
		return err
	}
	schema, sum, err := DescribeKeyspaceSchema(k8sClient, namespace, pods[0], container, keyspace)

	if onlySum {
		fmt.Println(sum)
	} else {
		fmt.Println((string)(schema))
	}

	return nil
}
