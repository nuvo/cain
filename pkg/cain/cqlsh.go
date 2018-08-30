package cain

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"

	"github.com/maorfr/cain/pkg/utils"
	"github.com/maorfr/skbn/pkg/skbn"
	skbn_utils "github.com/maorfr/skbn/pkg/utils"
)

func BackupKeyspaceSchema(iSrcClient, s3Client interface{}, namespace, pod, container, keyspace, bucket string) (string, error) {
	k8sClient := iSrcClient.(*skbn.K8sClient)
	clusterName, err := GetClusterName(k8sClient, namespace, pod, container)
	if err != nil {
		return "", err
	}

	schema, sum, err := DescribeKeyspaceSchema(k8sClient, namespace, pod, container, keyspace)
	if err != nil {
		return "", err
	}

	s3BasePath := fmt.Sprintf("%s/%s/%s/%s/%s/%s", bucket, "cassandra", namespace, clusterName, keyspace, sum)
	schemaToPath := fmt.Sprintf("%s/%s", s3BasePath, "schema.cql")

	if err := skbn.UploadToS3(s3Client, schemaToPath, "", schema); err != nil {
		return "", nil
	}

	return s3BasePath, nil
}

func DescribeKeyspaceSchema(iClient interface{}, namespace, pod, container, keyspace string) ([]byte, string, error) {
	option := fmt.Sprintf("DESC %s;", keyspace)
	schema, err := cqlsh(iClient, namespace, pod, container, option)
	if err != nil {
		return nil, "", err
	}
	h := sha256.New()
	h.Write(schema)
	sum := fmt.Sprintf("%x", h.Sum(nil))[0:6]

	return schema, sum, nil
}

func TruncateTables(iClient interface{}, namespace, container, keyspace string, pods, tables []string) {
	bwgSize := len(pods)
	bwg := skbn_utils.NewBoundedWaitGroup(bwgSize)
	for _, pod := range pods {
		bwg.Add(1)

		go func(iClient interface{}, namespace, container, keyspace, pod string) {
			for _, table := range tables {
				log.Println(pod, "Truncating table", table, "in keyspace", keyspace)
				option := fmt.Sprintf("TRUNCATE %s.%s;", keyspace, table)
				_, err := cqlsh(iClient, namespace, pod, container, option)
				if err != nil {
					log.Fatal(err)
				}
			}
			bwg.Done()
		}(iClient, namespace, container, keyspace, pod)

	}
	bwg.Wait()
}

func cqlsh(iClient interface{}, namespace, pod, container, option string) ([]byte, error) {
	k8sClient := iClient.(*skbn.K8sClient)

	stdin := strings.NewReader(option)
	executionFile := filepath.Join("/tmp", utils.GetRandString()+".cql")

	// Copy execution file to /tmp
	if err := copyToTmp(k8sClient, namespace, pod, container, executionFile, stdin); err != nil {
		return nil, err
	}

	command := fmt.Sprintf("cqlsh -f %s", executionFile)
	stdout, stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil)

	rmFromTmp(k8sClient, namespace, pod, container, executionFile)

	if len(stderr) != 0 {
		return nil, fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return nil, err
	}

	return removeWarning(stdout), nil
}

func copyToTmp(k8sClient *skbn.K8sClient, namespace, pod, container, tmpFileName string, stdin io.Reader) error {
	command := fmt.Sprintf("cp /dev/stdin %s", tmpFileName)
	_, stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, stdin)
	if len(stderr) != 0 {
		return fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return err
	}

	return nil
}

func rmFromTmp(k8sClient *skbn.K8sClient, namespace, pod, container, tmpFileName string) error {
	command := fmt.Sprintf("rm %s", tmpFileName)
	_, stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil)
	if len(stderr) != 0 {
		return fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return err
	}

	return nil
}

func removeWarning(b []byte) []byte {
	const warning = "Warning: Cannot create directory at `/home/cassandra/.cassandra`. Command history will not be saved."
	return []byte(strings.Replace((string)(b), warning, "", 1))
}
