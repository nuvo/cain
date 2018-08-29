package cain

import (
	"fmt"
	"path/filepath"

	"github.com/maorfr/skbn/pkg/skbn"

	"github.com/maorfr/cain/pkg/utils"
)

// Backup performs backup
func Backup(namespace, selector, container, keyspace, bucket string, parallel int) error {

	k8sClient, s3Client, err := skbn.GetClients("k8s", "s3", "", bucket)
	if err != nil {
		return err
	}
	pods, err := utils.GetPods(k8sClient, namespace, selector)
	if err != nil {
		return err
	}
	s3BasePath, err := BackupSchema(k8sClient, s3Client, namespace, pods[0], container, bucket)
	if err != nil {
		return err
	}

	tag := TakeSnapshotsInParallel(k8sClient, pods, namespace, container, keyspace)
	fromToPathsAllPods, err := utils.GetFromAndToPathsFromK8s(k8sClient, pods, namespace, container, keyspace, tag, s3BasePath)
	if err != nil {
		return err
	}
	skbn.PerformCopy(k8sClient, s3Client, "k8s", "s3", fromToPathsAllPods, parallel)
	ClearSnapshotsInParallel(k8sClient, pods, namespace, container, keyspace, tag)

	return nil
}

// Restore performs restore
func Restore(bucket, namespace, cluster, keyspace, tag, toNamespace, selector, container string, parallel int) error {
	if toNamespace == "" {
		toNamespace = namespace
	}
	s3Client, k8sClient, err := skbn.GetClients("s3", "k8s", bucket, "")
	if err != nil {
		return err
	}
	pods, err := utils.GetPods(k8sClient, toNamespace, selector)
	if err != nil {
		return err
	}
	_, sum, err := DescribeSchema(k8sClient, toNamespace, pods[0], container)
	if err != nil {
		return err
	}

	s3BasePath := filepath.Join(bucket, "cassandra", namespace, cluster, sum, keyspace, tag)
	fromToPaths, err := utils.GetFromAndToPathsFromS3(s3Client, k8sClient, s3BasePath, toNamespace, container)
	if err != nil {
		return err
	}
	skbn.PerformCopy(s3Client, k8sClient, "s3", "k8s", fromToPaths, parallel)

	return nil
}

// Schema gets the schema of the cassandra cluster
func Schema(namespace, selector, container string, onlySum bool) error {
	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return err
	}
	pods, err := utils.GetPods(k8sClient, namespace, selector)
	if err != nil {
		return err
	}
	schema, sum, err := DescribeSchema(k8sClient, namespace, pods[0], container)

	if onlySum {
		fmt.Println(sum)
	} else {
		fmt.Println((string)(schema))
	}

	return nil
}
