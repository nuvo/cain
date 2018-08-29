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

// Backup performs backup
func Backup(namespace, selector, container, keyspace, bucket string, parallel int) error {

	k8sClient, s3Client, err := getClients(bucket)
	if err != nil {
		return err
	}
	pods, err := utils.GetPods(k8sClient, namespace, selector)
	if err != nil {
		return err
	}
	s3BasePath, err := BackupSchema(k8sClient, s3Client, namespace, pods[0], container, bucket)
	if err != nil {
		return err
	}

	tag := TakeSnapshotsInParallel(k8sClient, pods, namespace, container, keyspace)
	fromToPathsAllPods, err := utils.GetFromAndToPathsFromAllPods(k8sClient, pods, namespace, container, keyspace, tag, s3BasePath)
	if err != nil {
		return err
	}
	CopyFilesInParallelK8sToS3(k8sClient, s3Client, fromToPathsAllPods, parallel)
	ClearSnapshotsInParallel(k8sClient, pods, namespace, container, keyspace, tag)

	return nil
}

// Restore performs restore
func Restore(namespace, selector, container, keyspace, bucket, tag string, parallel int) error {
	_, s3Client, err := getClients(bucket)
	if err != nil {
		return err
	}

	// s3BasePath := filepath.Join(bucket, namespace, "cassandra")
	fromPaths, err := skbn.GetListOfFilesFromS3(s3Client, "nuvo-skbn-test/dev1/cassandra/cassandra-0/kaa/20180828160856")
	if err != nil {
		return err
	}

	for _, f := range fromPaths {
		fmt.Println(f)
	}

	return nil
}

// Schema gets the schema of the cassandra cluster
func Schema(namespace, selector, container string, onlySum bool) error {
	k8sClient, _, err := getClients("")
	if err != nil {
		return err
	}
	pods, err := utils.GetPods(k8sClient, namespace, selector)
	if err != nil {
		return err
	}
	schema, sum, err := DescribeSchema(k8sClient, namespace, pods[0], container)

	if onlySum {
		fmt.Println(sum)
	} else {
		fmt.Println((string)(schema))
	}

	return nil
}

// CopyFilesInParallelK8sToS3 copies snapshots from all pods in parallel
func CopyFilesInParallelK8sToS3(k8sClient *skbn.K8sClient, s3Client *session.Session, fromToPathsAllPods []utils.FromToPair, parallel int) {
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

func getClients(bucket string) (*skbn.K8sClient, *session.Session, error) {
	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return nil, nil, err
	}
	if bucket == "" {
		return k8sClient, nil, nil
	}
	s3Client, err := skbn.GetClientToS3(bucket)
	if err != nil {
		return nil, nil, err
	}

	return k8sClient, s3Client, nil
}
