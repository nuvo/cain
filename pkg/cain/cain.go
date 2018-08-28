package cain

import (
	"fmt"
	"log"
	"math"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/maorfr/skbn/pkg/skbn"
	skbn_utils "github.com/maorfr/skbn/pkg/utils"

	"github.com/maorfr/cain/pkg/utils"
)

func Backup(namespace, selector, container, keyspace, bucket string, parallel int) error {

	k8sClient, s3Client, err := getClients(bucket)
	if err != nil {
		return err
	}
	pods, _ := utils.GetPods(k8sClient, namespace, selector)

	tag := TakeSnapshotsInParallel(k8sClient, pods, namespace, container, keyspace)
	fromToPathsAllPods, err := GetFromAndToPathsFromAllPods(k8sClient, pods, namespace, container, keyspace, tag, bucket)
	if err != nil {
		return err
	}
	CopyFilesInParallel(k8sClient, s3Client, fromToPathsAllPods, parallel)
	ClearSnapshotsInParallel(k8sClient, pods, namespace, container, keyspace, tag)

	return nil
}

func Restore() error {
	return nil
}

func TakeSnapshotsInParallel(k8sClient *skbn.K8sClient, pods []string, namespace, container, keyspace string) string {
	tag := utils.GetTag()
	bwgSize := len(pods)
	bwg := skbn_utils.NewBoundedWaitGroup(bwgSize)
	for _, pod := range pods {
		bwg.Add(1)

		go func(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, tag string) {
			log.Println("Taking snapshot in pod", pod)

			if err := TakeSnapshot(k8sClient, namespace, pod, container, keyspace, tag); err != nil {
				log.Fatal(err)
			}
			bwg.Done()
		}(k8sClient, namespace, pod, container, keyspace, tag)
	}
	bwg.Wait()

	return tag
}

func ClearSnapshotsInParallel(k8sClient *skbn.K8sClient, pods []string, namespace, container, keyspace, tag string) {
	bwgSize := len(pods)
	bwg := skbn_utils.NewBoundedWaitGroup(bwgSize)
	for _, pod := range pods {
		bwg.Add(1)

		go func(k8sClient *skbn.K8sClient, namespace, pod, container, keyspace, tag string) {
			log.Println("Clearing snapshot in pod", pod)

			if err := ClearSnapshot(k8sClient, namespace, pod, container, keyspace, tag); err != nil {
				log.Fatal(err)
			}
			bwg.Done()
		}(k8sClient, namespace, pod, container, keyspace, tag)
	}
	bwg.Wait()
}

func CopyFilesInParallel(k8sClient *skbn.K8sClient, s3Client *session.Session, fromToPathsAllPods []utils.FromToPair, parallel int) {
	totalFiles := len(fromToPathsAllPods)
	if parallel == 0 {
		parallel = totalFiles
	}
	bwgSize := int(math.Min(float64(parallel), float64(totalFiles))) // Very stingy :)
	bwg := skbn_utils.NewBoundedWaitGroup(bwgSize)
	currentLine := 0
	for _, ftp := range fromToPathsAllPods {

		bwg.Add(1)
		currentLine++

		totalDigits := skbn_utils.CountDigits(totalFiles)
		currentLinePadded := skbn_utils.LeftPad2Len(currentLine, 0, totalDigits)

		go func(srcClient, dstClient interface{}, fromPath, toPath, currentLinePadded string, totalFiles int) {
			buffer, err := skbn.DownloadFromK8s(k8sClient, fromPath)
			if err != nil {
				log.Fatal(err)
				bwg.Done()
				return
			}
			log.Println(fmt.Sprintf("file [%s/%d] src: %s", currentLinePadded, totalFiles, fromPath))

			err = skbn.UploadToS3(s3Client, toPath, fromPath, buffer)
			if err != nil {
				log.Fatal(err)
				bwg.Done()
				return
			}
			log.Println(fmt.Sprintf("file [%s/%d] dst: %s", currentLinePadded, totalFiles, toPath))

			bwg.Done()
		}(k8sClient, s3Client, ftp.FromPath, ftp.ToPath, currentLinePadded, totalFiles)
	}
	bwg.Wait()
}

func GetFromAndToPathsFromAllPods(k8sClient *skbn.K8sClient, pods []string, namespace, container, keyspace, tag, bucket string) ([]utils.FromToPair, error) {
	var fromToPathsAllPods []utils.FromToPair
	for _, pod := range pods {

		fromToPaths, err := utils.GetFromAndToPathsByTag(k8sClient, namespace, pod, container, keyspace, tag, bucket)
		if err != nil {
			return nil, err
		}
		fromToPathsAllPods = append(fromToPathsAllPods, fromToPaths...)
	}

	return fromToPathsAllPods, nil
}

func getClients(bucket string) (*skbn.K8sClient, *session.Session, error) {
	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return nil, nil, err
	}
	s3Client, err := skbn.GetClientToS3(bucket)
	if err != nil {
		return nil, nil, err
	}

	return k8sClient, s3Client, nil
}
