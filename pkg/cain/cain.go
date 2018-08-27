package cain

import (
	"log"
	"path/filepath"
	"strings"

	"github.com/maorfr/skbn/pkg/skbn"

	"github.com/maorfr/cain/pkg/utils"
)

func Backup(namespace, selector, container, keyspace, bucket string) error {

	const cassandraDataDir = "/var/lib/cassandra/data"
	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return err
	}
	s3Client, err := skbn.GetClientToS3(bucket)
	if err != nil {
		return err
	}

	tag := utils.GetTag()
	pods, _ := utils.GetPods(k8sClient, namespace, selector)
	for _, pod := range pods {
		log.Println("Backing up " + pod)
		if err := TakeSnapshot(*k8sClient, namespace, pod, container, keyspace, tag); err != nil {
			return err
		}
		pathPrfx := filepath.Join(namespace, pod, container)
		keyspacePath := filepath.Join(pathPrfx, cassandraDataDir, keyspace)
		tablesRelativePaths, err := skbn.GetListOfFilesFromK8s(*k8sClient, keyspacePath, "d", tag)
		if err != nil {
			return err
		}
		for _, tableRelativePath := range tablesRelativePaths {
			tablePath := filepath.Join(keyspacePath, tableRelativePath)
			filesToCopyRelativePaths, err := skbn.GetListOfFilesFromK8s(*k8sClient, tablePath, "f", "")
			if err != nil {
				return err
			}

			for _, fileToCopyRelativePath := range filesToCopyRelativePaths {
				fromPath := filepath.Join(tablePath, fileToCopyRelativePath)
				log.Println("src: ", fromPath)
				buffer, err := skbn.DownloadFromK8s(*k8sClient, fromPath)
				if err != nil {
					return err
				}
				tableRelativeDirNoSnapshotOrTag := strings.Replace(tableRelativePath, "snapshots/"+tag, "", 1)
				toPath := filepath.Join(bucket, namespace, "cassandra", pod, keyspace, tag, tableRelativeDirNoSnapshotOrTag, fileToCopyRelativePath)
				log.Println("dst: ", toPath)
				err = skbn.UploadToS3(s3Client, toPath, fromPath, buffer)
				if err != nil {
					return err
				}
			}
		}
		if err := ClearSnapshot(*k8sClient, namespace, pod, container, keyspace, tag); err != nil {
			return err
		}
	}

	return nil
}

func Restore() error {
	return nil
}
