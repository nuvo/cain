package utils

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/maorfr/skbn/pkg/skbn"
)

const cassandraDataDir = "/var/lib/cassandra/data"

// GetFromAndToPathsFromK8s aggregates paths from all pods
func GetFromAndToPathsFromK8s(iClient interface{}, pods []string, namespace, container, keyspace, tag, s3BasePath string) ([]skbn.FromToPair, error) {
	k8sClient := iClient.(*skbn.K8sClient)
	var fromToPathsAllPods []skbn.FromToPair
	for _, pod := range pods {

		fromToPaths, err := GetFromAndToPathsK8sToS3(k8sClient, namespace, pod, container, keyspace, tag, s3BasePath)
		if err != nil {
			return nil, err
		}
		fromToPathsAllPods = append(fromToPathsAllPods, fromToPaths...)
	}

	return fromToPathsAllPods, nil
}

func GetFromAndToPathsFromS3(s3Client, k8sClient interface{}, s3BasePath, toNamespace, container string) ([]skbn.FromToPair, error) {
	var fromToPaths []skbn.FromToPair

	filesToCopyRelativePaths, err := skbn.GetListOfFilesFromS3(s3Client, s3BasePath)
	if err != nil {
		return nil, err
	}

	for _, fileToCopyRelativePath := range filesToCopyRelativePaths {

		fromPath := filepath.Join(s3BasePath, fileToCopyRelativePath)
		toPath, err := PathFromS3ToK8s(k8sClient, fromPath, cassandraDataDir, s3BasePath, toNamespace, container)
		if err != nil {
			return nil, err
		}

		fromToPaths = append(fromToPaths, skbn.FromToPair{FromPath: fromPath, ToPath: toPath})
	}

	return fromToPaths, nil
}

func GetFromAndToPathsK8sToS3(k8sClient interface{}, namespace, pod, container, keyspace, tag, s3BasePath string) ([]skbn.FromToPair, error) {
	var fromToPaths []skbn.FromToPair

	pathPrfx := filepath.Join(namespace, pod, container, cassandraDataDir)

	keyspacePath := filepath.Join(pathPrfx, keyspace)
	tablesRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, keyspacePath, "d", tag)
	if err != nil {
		return nil, err
	}

	for _, tableRelativePath := range tablesRelativePaths {

		tablePath := filepath.Join(keyspacePath, tableRelativePath)
		filesToCopyRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, tablePath, "f", "*")
		if err != nil {
			return nil, err
		}

		for _, fileToCopyRelativePath := range filesToCopyRelativePaths {

			fromPath := filepath.Join(tablePath, fileToCopyRelativePath)
			toPath := PathFromK8sToS3(fromPath, cassandraDataDir, s3BasePath)

			fromToPaths = append(fromToPaths, skbn.FromToPair{FromPath: fromPath, ToPath: toPath})
		}
	}

	return fromToPaths, nil
}

func PathFromK8sToS3(k8sPath, cassandraDataDir, s3BasePath string) string {
	k8sPath = strings.Replace(k8sPath, cassandraDataDir, "", 1)
	pSplit := strings.Split(k8sPath, "/")

	// 0 = namespace
	pod := pSplit[1]
	// 2 = container
	keyspace := pSplit[3]
	tableWithHash := pSplit[4]
	// 5 = snapshots
	tag := pSplit[6]
	file := pSplit[7]

	table := strings.Split(tableWithHash, "-")[0]

	return filepath.Join(s3BasePath, keyspace, tag, pod, table, file)
}

func PathFromS3ToK8s(k8sClient interface{}, s3Path, cassandraDataDir, s3BasePath, toNamespace, container string) (string, error) {
	pSplit := strings.Split(s3Path, "/")

	// 0 = bucket
	// 1 = cassandra
	// 2 = namespace
	// 3 = cluster
	// 4 = sum
	keyspace := pSplit[5]
	// 6 = tag
	pod := pSplit[7]
	table := pSplit[8]
	file := pSplit[9]

	k8sKeyspacePath := filepath.Join(toNamespace, pod, container, cassandraDataDir, keyspace)
	tableRelativePath, err := skbn.GetListOfFilesFromK8s(k8sClient, k8sKeyspacePath, "d", table+"*")
	if err != nil {
		return "", err
	}
	if len(tableRelativePath) != 1 {
		return "", fmt.Errorf("Error with table %s, found %d directories", table, len(tableRelativePath))
	}

	toPath := filepath.Join(k8sKeyspacePath, tableRelativePath[0], file)

	return toPath, nil
}
