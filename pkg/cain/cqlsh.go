package cain

import (
	"crypto/sha256"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/maorfr/cain/pkg/utils"
	"github.com/maorfr/skbn/pkg/skbn"
)

func BackupSchema(k8sClient *skbn.K8sClient, s3Client *session.Session, namespace, pod, container, bucket string) (string, error) {
	clusterName, err := GetClusterName(k8sClient, namespace, pod, container)
	if err != nil {
		return "", err
	}

	schema, sum, err := DescribeSchema(k8sClient, namespace, pod, container)
	if err != nil {
		return "", err
	}

	s3BasePath := fmt.Sprintf("%s/%s/%s/%s/%s", bucket, "cassandra", namespace, clusterName, sum)
	schemaToPath := fmt.Sprintf("%s/%s", s3BasePath, "schema.cql")

	if err := skbn.UploadToS3(s3Client, schemaToPath, "", schema); err != nil {
		return "", nil
	}

	return s3BasePath, nil
}

func DescribeSchema(k8sClient *skbn.K8sClient, namespace, pod, container string) ([]byte, string, error) {

	stdin := strings.NewReader("DESC schema;")
	executionFile := filepath.Join("/tmp", utils.RandString()+".cql")

	// Copy execution file to /tmp
	if err := copyToTmp(k8sClient, namespace, pod, container, executionFile, stdin); err != nil {
		return nil, "", err
	}

	// Execute cqlsh command with file
	option := fmt.Sprintf("-f %s", executionFile)
	schema, err := cqlsh(k8sClient, namespace, pod, container, option)
	if err != nil {
		return nil, "", err
	}

	schema = removeWarning(schema)
	h := sha256.New()
	h.Write(schema)
	sum := fmt.Sprintf("%x", h.Sum(nil))[0:6]

	rmFromTmp(k8sClient, namespace, pod, container, executionFile)

	return schema, sum, nil
}

func cqlsh(k8sClient *skbn.K8sClient, namespace, pod, container, option string) ([]byte, error) {
	command := fmt.Sprintf("cqlsh %s", option)
	stdout, stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil)

	if len(stderr) != 0 {
		return nil, fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return nil, err
	}

	return stdout, nil
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
