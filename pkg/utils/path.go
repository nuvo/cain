package utils

import (
	"path/filepath"
	"strings"

	"github.com/maorfr/skbn/pkg/skbn"
)

const cassandraDataDir = "/var/lib/cassandra/data"

type FromToPair struct {
	FromPath string
	ToPath   string
}

// GetFromAndToPathsFromAllPods aggregates paths from all pods
func GetFromAndToPathsFromAllPods(k8sClient *skbn.K8sClient, pods []string, namespace, container, keyspace, tag, s3BasePath string) ([]FromToPair, error) {
	var fromToPathsAllPods []FromToPair
	for _, pod := range pods {

		fromToPaths, err := GetFromAndToPathsK8sToS3(k8sClient, namespace, pod, container, keyspace, tag, s3BasePath)
		if err != nil {
			return nil, err
		}
		fromToPathsAllPods = append(fromToPathsAllPods, fromToPaths...)
	}

	return fromToPathsAllPods, nil
}

func GetFromAndToPathsK8sToS3(k8sClient interface{}, namespace, pod, container, keyspace, tag, s3BasePath string) ([]FromToPair, error) {
	var fromToPaths []FromToPair

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

			fromToPaths = append(fromToPaths, FromToPair{FromPath: fromPath, ToPath: toPath})
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

func PathFromS3ToK8s(path string) string {
	return ""
}
