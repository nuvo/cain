package utils

import (
	"fmt"

	"github.com/maorfr/skbn/pkg/skbn"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetPods returns a slice of strings of pod names by namespace and selector
func GetPods(iClient interface{}, namespace, selector string) ([]string, error) {

	k8sClient := *iClient.(*skbn.K8sClient)
	pods, err := k8sClient.ClientSet.CoreV1().Pods(namespace).List(metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, err
	}

	var podList []string
	for _, pod := range pods.Items {
		podList = append(podList, pod.Name)
	}
	if len(podList) == 0 {
		return nil, fmt.Errorf("No pods were found in namespace %s by selector %s", namespace, selector)
	}

	return podList, nil
}
