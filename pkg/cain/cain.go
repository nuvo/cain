package cain

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/maorfr/skbn/pkg/skbn"
	skbn_utils "github.com/maorfr/skbn/pkg/utils"

	"github.com/maorfr/cain/pkg/utils"
)

// Backup performs backup
func Backup(namespace, selector, container, keyspace, dst string, parallel int) error {
	log.Println("Backup started!")
	dstPrefix, dstPath := skbn_utils.SplitInTwo(dst, "://")

	log.Println("Getting clients")
	k8sClient, dstClient, err := skbn.GetClients("k8s", dstPrefix, "", dstPath)
	if err != nil {
		return err
	}

	log.Println("Getting pods")
	pods, err := utils.GetPods(k8sClient, namespace, selector)
	if err != nil {
		return err
	}

	log.Println("Backing up schema")
	dstBasePath, err := BackupKeyspaceSchema(k8sClient, dstClient, namespace, pods[0], container, keyspace, dstPrefix, dstPath)
	if err != nil {
		return err
	}

	log.Println("Taking snapshots")
	tag := TakeSnapshots(k8sClient, pods, namespace, container, keyspace)

	log.Println("Calculating paths. This may take a while...")
	fromToPathsAllPods, err := utils.GetFromAndToPathsFromK8s(k8sClient, pods, namespace, container, keyspace, tag, dstBasePath)
	if err != nil {
		return err
	}

	log.Println("Starting files copy")
	if err := skbn.PerformCopy(k8sClient, dstClient, "k8s", dstPrefix, fromToPathsAllPods, parallel); err != nil {
		return err
	}

	log.Println("Clearing snapshots")
	ClearSnapshots(k8sClient, pods, namespace, container, keyspace, tag)

	log.Println("All done!")
	return nil
}

// Restore performs restore
func Restore(src, keyspace, tag, namespace, selector, container string, parallel int) error {
	log.Println("Restore started!")
	srcPrefix, srcBasePath := skbn_utils.SplitInTwo(src, "://")

	log.Println("Getting clients")
	srcClient, k8sClient, err := skbn.GetClients(srcPrefix, "k8s", srcBasePath, "")
	if err != nil {
		return err
	}

	log.Println("Getting pods")
	existingPods, err := utils.GetPods(k8sClient, namespace, selector)
	if err != nil {
		return err
	}

	log.Println("Getting current schema")
	_, sum, err := DescribeKeyspaceSchema(k8sClient, namespace, existingPods[0], container, keyspace)
	if err != nil {
		return err
	}
	log.Println("Found schema:", sum)

	log.Println("Calculating paths. This may take a while...")
	srcPath := filepath.Join(srcBasePath, keyspace, sum, tag)
	fromToPaths, podsToBeRestored, tablesToRefresh, err := utils.GetFromAndToPathsSrcToK8s(srcClient, k8sClient, srcPrefix, srcPath, srcBasePath, namespace, container)
	if err != nil {
		return err
	}

	log.Println("Validating pods match restore")
	if err := utils.SliceContainsSlice(podsToBeRestored, existingPods); err != nil {
		return err
	}

	log.Println("Truncating tables")
	TruncateTables(k8sClient, namespace, container, keyspace, existingPods, tablesToRefresh)

	log.Println("Starting files copy")
	if err := skbn.PerformCopy(srcClient, k8sClient, srcPrefix, "k8s", fromToPaths, parallel); err != nil {
		return err
	}

	log.Println("Refreshing tables")
	RefreshTables(k8sClient, namespace, container, keyspace, podsToBeRestored, tablesToRefresh)

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
