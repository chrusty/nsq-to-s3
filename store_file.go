package main

import (
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"os"
	"time"
)

const (
	fileChunkSize = 5242880 // 5MB in bytes
)

// Store multi-part file (avoids blowing the memory by loading a huge file):
func StoreMultiPartFile(localFileName string) error {

	// Build the filename we'll use for S3:
	remoteFileName := fmt.Sprintf("%v/%v/%v/%v/%v/%v.%v.gz", *s3Path, time.Now().Year(), time.Now().Month(), time.Now().Day(), time.Now().Hour(), time.Now().Minute(), *s3FileExtention)

	log.Infof("Storing local file (%v) on S3 (%v)...", localFileName, remoteFileName)

	// Authenticate with AWS:
	awsAuth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		log.Criticalf("Unable to authenticate to AWS! (%s) ...\n", err)
		return err
	} else {
		log.Debugf("Authenticated to AWS")
	}

	// Make a new S3 connection:
	log.Debugf("Connecting to AWS...")
	s3Connection := s3.New(awsAuth, aws.Regions[*awsRegion])

	// Make a bucket object:
	s3Bucket := s3Connection.Bucket(*s3Bucket)

	// Prepare arguments for the call to store messages on S3:
	contType := "text/plain"
	perm := s3.BucketOwnerFull

	// Get a multi-part handler:
	multiPartHandler, err := s3Bucket.InitMulti(remoteFileName, contType, perm)
	if err != nil {
		log.Criticalf("Failed to get a multi-part handler (%v) on S3 (%v)", remoteFileName, err)
		return err
	}

	// Open the file:
	localFile, err := os.Open(localFileName)
	defer localFile.Close()
	if err != nil {
		log.Errorf("Coudln't open file (%v): %v", localFileName, err)
		return err
	}

	// Calculate the number of parts by dividing the file-size:
	parts, err := multiPartHandler.PutAll(localFile, fileChunkSize)
	if err != nil {
		log.Errorf("Coudln't perform multi-part S3 upload (%v): %v", remoteFileName, err)
		return err
	}

	// Now finish the upload:
	err = multiPartHandler.Complete(parts)
	if err != nil {
		log.Errorf("Coudln't complete multi-part S3 upload (%v): %v", remoteFileName, err)
		return err
	}

	log.Infof("Stored file (%v) on s3", remoteFileName)

	return nil
}
