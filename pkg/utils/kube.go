package utils

import (
	"path/filepath"
	"strings"

	"github.com/maorfr/skbn/pkg/skbn"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type FromToPair struct {
	FromPath string
	ToPath   string
}

func GetFromAndToPathsByTag(k8sClient interface{}, namespace, pod, container, keyspace, tag, bucket string) ([]FromToPair, error) {
	const cassandraDataDir = "/var/lib/cassandra/data"
	var fromToPaths []FromToPair

	pathPrfx := filepath.Join(namespace, pod, container)
	keyspacePath := filepath.Join(pathPrfx, cassandraDataDir, keyspace)
	tablesRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, keyspacePath, "d", tag)
	if err != nil {
		return nil, err
	}
	for _, tableRelativePath := range tablesRelativePaths {
		tablePath := filepath.Join(keyspacePath, tableRelativePath)
		filesToCopyRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, tablePath, "f", "")
		if err != nil {
			return nil, err
		}

		for _, fileToCopyRelativePath := range filesToCopyRelativePaths {
			fromPath := filepath.Join(tablePath, fileToCopyRelativePath)
			tableRelativeDirNoSnapshotOrTag := strings.Replace(tableRelativePath, "snapshots/"+tag, "", 1)
			toPath := filepath.Join(bucket, namespace, "cassandra", pod, keyspace, tag, tableRelativeDirNoSnapshotOrTag, fileToCopyRelativePath)
			fromToPair := FromToPair{FromPath: fromPath, ToPath: toPath}
			fromToPaths = append(fromToPaths, fromToPair)
		}
	}

	return fromToPaths, nil
}

func GetPods(k8sClient *skbn.K8sClient, namespace, selector string) ([]string, error) {

	pods, err := k8sClient.ClientSet.CoreV1().Pods(namespace).List(metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		panic(err.Error())
	}

	var podList []string
	for _, pod := range pods.Items {
		podList = append(podList, pod.Name)
	}

	return podList, nil
}
