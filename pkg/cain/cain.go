package cain

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/nuvo/skbn/pkg/skbn"
	"github.com/spyoff/cain/pkg/utils"
)

// BackupOptions are the options to pass to Backup
type BackupOptions struct {
	Namespace         string
	Selector          string
	Container         string
	Keyspace          string
	Dst               string
	Parallel          int
	BufferSize        float64
	CassandraDataDir  string
	CassandraUsername string
	CassandraPassword string
}

// Backup performs backup
func Backup(o BackupOptions) (string, error) {
	log.Println("Backup started!")
	dstPrefix, dstPath := utils.SplitInTwo(o.Dst, "://")

	if err := skbn.TestImplementationsExist("k8s", dstPrefix); err != nil {
		return "", err
	}

	log.Println("Getting clients")
	k8sClient, dstClient, err := skbn.GetClients("k8s", dstPrefix, "", dstPath)
	if err != nil {
		return "", err
	}

	log.Println("Getting pods")
	pods, err := utils.GetPods(k8sClient, o.Namespace, o.Selector)
	if err != nil {
		return "", err
	}

	log.Println("Testing existence of data dir")
	if err := utils.TestK8sDirectory(k8sClient, pods, o.Namespace, o.Container, o.CassandraDataDir); err != nil {
		return "", err
	}

	log.Println("Backing up schema")
	dstBasePath, err := BackupKeyspaceSchema(k8sClient, dstClient, o.Namespace, pods[0], o.Container, o.Keyspace, dstPrefix, dstPath, o.CassandraUsername, o.CassandraPassword)
	if err != nil {
		return "", err
	}

	log.Println("Taking snapshots")
	tag := TakeSnapshots(k8sClient, pods, o.Namespace, o.Container, o.Keyspace)

	log.Println("Calculating paths. This may take a while...")
	fromToPathsAllPods, err := utils.GetFromAndToPathsFromK8s(k8sClient, pods, o.Namespace, o.Container, o.Keyspace, tag, dstBasePath, o.CassandraDataDir)
	if err != nil {
		return "", err
	}

	log.Println("Starting files copy")
	if err := skbn.PerformCopy(k8sClient, dstClient, "k8s", dstPrefix, fromToPathsAllPods, o.Parallel, o.BufferSize); err != nil {
		return "", err
	}

	log.Println("Clearing snapshots")
	ClearSnapshots(k8sClient, pods, o.Namespace, o.Container, o.Keyspace, tag)

	log.Println("All done!")
	return tag, nil
}

// RestoreOptions are the options to pass to Restore
type RestoreOptions struct {
	Src               string
	Keyspace          string
	Tag               string
	Schema            string
	Namespace         string
	Selector          string
	Container         string
	Parallel          int
	BufferSize        float64
	UserGroup         string
	CassandraDataDir  string
	CassandraUsername string
	CassandraPassword string
}

// Restore performs restore
func Restore(o RestoreOptions) error {
	log.Println("Restore started!")
	srcPrefix, srcBasePath := utils.SplitInTwo(o.Src, "://")

	log.Println("Getting clients")
	srcClient, k8sClient, err := skbn.GetClients(srcPrefix, "k8s", srcBasePath, "")
	if err != nil {
		return err
	}

	log.Println("Getting pods")
	existingPods, err := utils.GetPods(k8sClient, o.Namespace, o.Selector)
	if err != nil {
		return err
	}

	log.Println("Testing existence of data dir")
	if err := utils.TestK8sDirectory(k8sClient, existingPods, o.Namespace, o.Container, o.CassandraDataDir); err != nil {
		return err
	}

	log.Println("Getting current schema")
	_, sum, err := DescribeKeyspaceSchema(k8sClient, o.Namespace, existingPods[0], o.Container, o.Keyspace, o.CassandraUsername, o.CassandraPassword)
	if err != nil {
		if o.Schema == "" {
			return err
		}
		log.Println("Schema not found, restoring schema", o.Schema)
		sum, err = RestoreKeyspaceSchema(srcClient, k8sClient, srcPrefix, srcBasePath, o.Namespace, existingPods[0], o.Container, o.Keyspace, o.Schema, o.Parallel, o.BufferSize, o.CassandraUsername, o.CassandraPassword)
		if err != nil {
			return err
		}
		log.Println("Restored schema:", sum)
	}

	if o.Schema != "" && sum != o.Schema {
		return fmt.Errorf("specified schema %s is not the same as found schema %s", o.Schema, sum)
	}

	log.Println("Found schema:", sum)

	log.Println("Calculating paths. This may take a while...")
	srcPath := filepath.Join(srcBasePath, o.Keyspace, sum, o.Tag)
	fromToPaths, podsToBeRestored, tablesToRefresh, err := utils.GetFromAndToPathsSrcToK8s(srcClient, k8sClient, srcPrefix, srcPath, srcBasePath, o.Namespace, o.Container, o.CassandraDataDir)
	if err != nil {
		return err
	}

	log.Println("Validating pods match restore")
	if err := utils.SliceContainsSlice(podsToBeRestored, existingPods); err != nil {
		return err
	}

	log.Println("Getting materialized views to exclude")
	materializedViews, err := GetMaterializedViews(k8sClient, o.Namespace, o.Container, existingPods[0], o.Keyspace, o.CassandraUsername, o.CassandraPassword)
	if err != nil {
		return err
	}

	log.Println("Truncating tables")
	TruncateTables(k8sClient, o.Namespace, o.Container, o.Keyspace, existingPods, tablesToRefresh, materializedViews, o.CassandraUsername, o.CassandraPassword)

	log.Println("Starting files copy")
	if err := skbn.PerformCopy(srcClient, k8sClient, srcPrefix, "k8s", fromToPaths, o.Parallel, o.BufferSize); err != nil {
		return err
	}

	log.Println("Changing files ownership")
	if err := utils.ChangeFilesOwnership(k8sClient, existingPods, o.Namespace, o.Container, o.UserGroup, o.CassandraDataDir); err != nil {
		return err
	}

	log.Println("Refreshing tables")
	RefreshTables(k8sClient, o.Namespace, o.Container, o.Keyspace, podsToBeRestored, tablesToRefresh)

	log.Println("All done!")
	return nil
}

// SchemaOptions are the options to pass to Schema
type SchemaOptions struct {
	Namespace         string
	Selector          string
	Container         string
	Keyspace          string
	CassandraUsername string
	CassandraPassword string
}

// Schema gets the schema of the cassandra cluster
func Schema(o SchemaOptions) ([]byte, string, error) {
	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return nil, "", err
	}
	pods, err := utils.GetPods(k8sClient, o.Namespace, o.Selector)
	if err != nil {
		return nil, "", err
	}
	schema, sum, err := DescribeKeyspaceSchema(k8sClient, o.Namespace, pods[0], o.Container, o.Keyspace, o.CassandraUsername, o.CassandraPassword)
	if err != nil {
		return nil, "", err
	}

	return schema, sum, nil
}
