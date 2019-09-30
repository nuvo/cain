package cain

import (
	"bytes"
	"fmt"
	"log"
	"strings"

	"github.com/nuvo/skbn/pkg/skbn"
	"github.com/spyoff/cain/pkg/utils"
)

// TakeSnapshots takes a snapshot using nodetool in all pods in parallel
func TakeSnapshots(iClient interface{}, pods []string, namespace, container, keyspace string) string {
	k8sClient := iClient.(*skbn.K8sClient)
	tag := utils.GetTimeStamp()
	bwgSize := len(pods)
	bwg := utils.NewBoundedWaitGroup(bwgSize)
	for _, pod := range pods {
		bwg.Add(1)

		go func(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, tag string) {
			if err := takeSnapshot(k8sClient, namespace, pod, container, keyspace, tag); err != nil {
				log.Fatal(err)
			}
			bwg.Done()
		}(k8sClient, namespace, pod, container, keyspace, tag)
	}
	bwg.Wait()

	return tag
}

// ClearSnapshots clears a snapshot using nodetool in all pods in parallel
func ClearSnapshots(iClient interface{}, pods []string, namespace, container, keyspace, tag string) {
	k8sClient := iClient.(*skbn.K8sClient)
	bwgSize := len(pods)
	bwg := utils.NewBoundedWaitGroup(bwgSize)
	for _, pod := range pods {
		bwg.Add(1)

		go func(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, tag string) {
			if err := clearSnapshot(k8sClient, namespace, pod, container, keyspace, tag); err != nil {
				log.Fatal(err)
			}
			bwg.Done()
		}(k8sClient, namespace, pod, container, keyspace, tag)
	}
	bwg.Wait()
}

// RefreshTables refreshes tables in all pods in parallel
func RefreshTables(iClient interface{}, namespace, container, keyspace string, pods, tables []string) {
	k8sClient := iClient.(*skbn.K8sClient)
	bwgSize := len(pods)
	bwg := utils.NewBoundedWaitGroup(bwgSize)
	for _, pod := range pods {
		bwg.Add(1)

		go func(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace string, table []string) {
			for _, table := range tables {
				if err := refreshTable(k8sClient, namespace, pod, container, keyspace, table); err != nil {
					log.Fatal(err)
				}
			}
			bwg.Done()
		}(k8sClient, namespace, pod, container, keyspace, tables)
	}
	bwg.Wait()
}

// GetClusterName gets the name of the cassandra cluster
func GetClusterName(iClient interface{}, namespace, pod, container string) (string, error) {
	k8sClient := iClient.(*skbn.K8sClient)
	command := []string{"describecluster"}
	output, err := nodetool(k8sClient, namespace, pod, container, command)
	if err != nil {
		return "", err
	}

	subStr := "Name:"
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, subStr) {
			output = strings.TrimSpace(strings.Replace(line, subStr, "", 1))
			break
		}
	}

	return output, nil
}

func takeSnapshot(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, tag string) error {
	log.Println(pod, "Taking snapshot of keyspace", keyspace)
	command := []string{"snapshot", "-t", tag, keyspace}
	output, err := nodetool(k8sClient, namespace, pod, container, command)
	if err != nil {
		return err
	}
	printOutput(output, pod)
	return nil
}

func clearSnapshot(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, tag string) error {
	log.Println(pod, "Clearing snapshot of keyspace", keyspace)
	command := []string{"clearsnapshot", "-t", tag, keyspace}
	output, err := nodetool(k8sClient, namespace, pod, container, command)
	if err != nil {
		return err
	}
	printOutput(output, pod)
	return nil
}

func refreshTable(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, table string) error {
	log.Println(pod, "Refreshing table", table, "in keyspace", keyspace)
	command := []string{"refresh", keyspace, table}
	output, err := nodetool(k8sClient, namespace, pod, container, command)
	if err != nil {
		return err
	}
	printOutput(output, pod)
	return nil
}

func nodetool(k8sClient *skbn.K8sClient, namespace, pod, container string, command []string) (string, error) {
	command = append([]string{"nodetool"}, command...)
	stdout := new(bytes.Buffer)
	stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil, stdout)
	if len(stderr) != 0 {
		return "", fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return "", err
	}

	return stdout.String(), nil
}

func printOutput(output, pod string) {
	for _, line := range strings.Split(output, "\n") {
		if line != "" {
			log.Println(pod, line)
		}
	}
}
