package cain

import (
	"fmt"
	"log"

	"github.com/maorfr/skbn/pkg/skbn"
)

func TakeSnapshot(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, tag string) error {
	if err := nodetool(k8sClient, namespace, pod, container, keyspace, tag, "snapshot"); err != nil {
		return err
	}
	return nil
}

func ClearSnapshot(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, tag string) error {
	if err := nodetool(k8sClient, namespace, pod, container, keyspace, tag, "clearsnapshot"); err != nil {
		return err
	}
	return nil
}

func nodetool(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, tag, option string) error {
	command := fmt.Sprintf("nodetool -h localhost -p 7199 %s -t %s %s", option, tag, keyspace)
	stdout, stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil)
	if len(stderr) != 0 {
		return fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return err
	}
	log.Println((string)(stdout))

	return nil
}
