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
)

// BackupKeyspaceSchema gets the schema of the keyspace and backs it up
func BackupKeyspaceSchema(iK8sClient, iDstClient interface{}, namespace, pod, container, keyspace, dstPrefix, dstPath string) (string, error) {
	clusterName, err := GetClusterName(iK8sClient, namespace, pod, container)
	if err != nil {
		return "", err
	}

	schema, sum, err := DescribeKeyspaceSchema(iK8sClient, namespace, pod, container, keyspace)
	if err != nil {
		return "", err
	}

	dstBasePath := filepath.Join(dstPath, namespace, clusterName, keyspace, sum)
	schemaToPath := filepath.Join(dstBasePath, "schema.cql")

	if err := skbn.Upload(iDstClient, dstPrefix, schemaToPath, "", schema); err != nil {
		return "", nil
	}

	return dstBasePath, nil
}

// DescribeKeyspaceSchema describes the schema of the keyspace
func DescribeKeyspaceSchema(iK8sClient interface{}, namespace, pod, container, keyspace string) ([]byte, string, error) {
	option := fmt.Sprintf("DESC %s;", keyspace)
	schema, err := Cqlsh(iK8sClient, namespace, pod, container, option)
	if err != nil {
		return nil, "", err
	}
	h := sha256.New()
	h.Write(schema)
	sum := fmt.Sprintf("%x", h.Sum(nil))[0:6]

	return schema, sum, nil
}

// TruncateTables truncates the provided tables in all pods
func TruncateTables(iK8sClient interface{}, namespace, container, keyspace string, pods, tables, materializedViews []string) {
	bwgSize := len(pods)
	bwg := utils.NewBoundedWaitGroup(bwgSize)
	for _, pod := range pods {
		bwg.Add(1)

		go func(iK8sClient interface{}, namespace, container, keyspace, pod string) {
			for _, table := range tables {
				if utils.Contains(materializedViews, table) {
					log.Println(pod, "Skipping materialized view", table, "in keyspace", keyspace)
					continue
				}
				log.Println(pod, "Truncating table", table, "in keyspace", keyspace)
				option := fmt.Sprintf("TRUNCATE %s.%s;", keyspace, table)
				_, err := Cqlsh(iK8sClient, namespace, pod, container, option)
				if err != nil {
					log.Fatal(err)
				}
			}
			bwg.Done()
		}(iK8sClient, namespace, container, keyspace, pod)

	}
	bwg.Wait()
}

// GetMaterializedViews gets all materialized views to avoid truncate and refresh
func GetMaterializedViews(iK8sClient interface{}, namespace, container, pod, keyspace string) ([]string, error) {

	option := fmt.Sprintf("select view_name from system_schema.views where keyspace_name='%s';", keyspace)
	output, err := Cqlsh(iK8sClient, namespace, pod, container, option)
	if err != nil {
		log.Fatal(err)
	}

	var views []string
	headerPassed := false
	for _, line := range strings.Split((string)(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.HasPrefix(line, "(") {
			break
		}
		if headerPassed {
			views = append(views, strings.TrimSpace(line))
		}
		if strings.HasPrefix(line, "-") {
			headerPassed = true
		}
	}

	return views, nil
}

// Cqlsh executes cqlsh -e 'option' in a given pod
func Cqlsh(iK8sClient interface{}, namespace, pod, container, option string) ([]byte, error) {
	k8sClient := iK8sClient.(*skbn.K8sClient)

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
