package cain

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/nuvo/cain/pkg/utils"
	"github.com/nuvo/skbn/pkg/skbn"
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

	reader := bytes.NewReader(schema)
	if err := skbn.Upload(iDstClient, dstPrefix, schemaToPath, "", reader); err != nil {
		return "", nil
	}

	return dstBasePath, nil
}

// ProcessKeyspaceSchema gets the schema of the keyspace and returns the sum and destination base path
func ProcessKeyspaceSchema(iK8sClient, iDstClient interface{}, namespace, pod, container, keyspace, dstPrefix, dstPath string) (string, error) {
	clusterName, err := GetClusterName(iK8sClient, namespace, pod, container)
	if err != nil {
		return "", err
	}

	_, sum, err := DescribeKeyspaceSchema(iK8sClient, namespace, pod, container, keyspace)
	if err != nil {
		return "", err
	}

	dstBasePath := filepath.Join(dstPath, namespace, clusterName, keyspace, sum)

	return dstBasePath, nil
}

// DescribeKeyspaceSchema describes the schema of the keyspace
func DescribeKeyspaceSchema(iK8sClient interface{}, namespace, pod, container, keyspace string) ([]byte, string, error) {
	command := []string{fmt.Sprintf("DESC %s;", keyspace)}
	schema, err := Cqlsh(iK8sClient, namespace, pod, container, command)
	if err != nil {
		return nil, "", fmt.Errorf("Could not describe schema. make sure a schema exists for keyspace \"%s\" or restore it using \"--schema\". %s", keyspace, err)
	}
	h := sha256.New()
	h.Write(schema)
	sum := fmt.Sprintf("%x", h.Sum(nil))[0:6]

	return schema, sum, nil
}

// RestoreKeyspaceSchema restores a keyspace schema
func RestoreKeyspaceSchema(srcClient, iK8sClient interface{}, srcPrefix, srcPath, namespace, pod, container, keyspace, schema string, parallel int, bufferSize float64) (string, error) {
	schemaTmpFile := fmt.Sprintf("/tmp/%s/schema.cql", keyspace)
	fromTo := skbn.FromToPair{
		FromPath: filepath.Join(srcPath, keyspace, schema, "schema.cql"),
		ToPath:   filepath.Join(namespace, pod, container, schemaTmpFile),
	}
	if err := skbn.PerformCopy(srcClient, iK8sClient, srcPrefix, "k8s", []skbn.FromToPair{fromTo}, parallel, bufferSize); err != nil {
		return "", err
	}
	if _, err := CqlshF(iK8sClient, namespace, pod, container, schemaTmpFile); err != nil {
		return "", err
	}
	_, sum, err := DescribeKeyspaceSchema(iK8sClient, namespace, pod, container, keyspace)

	return sum, err
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
				command := []string{fmt.Sprintf("TRUNCATE %s.%s;", keyspace, table)}
				_, err := Cqlsh(iK8sClient, namespace, pod, container, command)
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

	command := []string{fmt.Sprintf("SELECT view_name FROM system_schema.views WHERE keyspace_name='%s';", keyspace)}
	output, err := Cqlsh(iK8sClient, namespace, pod, container, command)
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

// Cqlsh executes cqlsh -e 'command' in a given pod
func Cqlsh(iK8sClient interface{}, namespace, pod, container string, command []string) ([]byte, error) {
	k8sClient := iK8sClient.(*skbn.K8sClient)

	command = append([]string{"cqlsh", "-e"}, command...)
	stdout := new(bytes.Buffer)
	stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil, stdout)

	if len(stderr) != 0 {
		return nil, fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return nil, err
	}

	return removeWarning(stdout.Bytes()), nil
}

// CqlshF executes cqlsh -f file in a given pod
func CqlshF(iK8sClient interface{}, namespace, pod, container string, file string) ([]byte, error) {
	k8sClient := iK8sClient.(*skbn.K8sClient)

	command := []string{"cqlsh", "-f", file}
	stdout := new(bytes.Buffer)
	stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil, stdout)

	if len(stderr) != 0 {
		return nil, fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return nil, err
	}

	return removeWarning(stdout.Bytes()), nil
}

func removeWarning(b []byte) []byte {
	const warning = "Warning: Cannot create directory at `/home/cassandra/.cassandra`. Command history will not be saved."
	return []byte(strings.Replace((string)(b), warning, "", 1))
}
