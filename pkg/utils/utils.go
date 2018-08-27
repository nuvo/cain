package utils

import (
	"time"

	"github.com/maorfr/skbn/pkg/skbn"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

func GetTag() string {
	return time.Now().Format("20060102150405")
}
