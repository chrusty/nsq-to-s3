package main

import (
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"os"
	"time"
)

// Print messages to the screen:
func PrintMessages(fileData []byte) error {

	fileName := fmt.Sprintf("%v/%v/%v/%v/%v/%v.%v", *s3Path, time.Now().Year(), time.Now().Month(), time.Now().Day(), time.Now().Hour(), time.Now().Minute(), *s3FileExtention)

	log.Infof("Would store in '%v'", fileName)

	log.Debugf("Messages: %v", string(fileData))

	return nil
}

// Store messages to S3:
func StoreMessages(fileData []byte) error {

	log.Infof("Storing %d bytes...", len(fileData))

	// Authenticate with AWS:
	awsAuth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		log.Criticalf("Unable to authenticate to AWS! (%s) ...\n", err)
		os.Exit(2)
	} else {
		log.Debugf("Authenticated to AWS")
	}

	log.Debugf("Connecting to AWS...")
	// Make a new S3 connection:
	s3Connection := s3.New(awsAuth, aws.Regions[*awsRegion])

	// Make a bucket object:
	s3Bucket := s3Connection.Bucket(*s3Bucket)

	// Prepare arguments for the call to store messages on S3:
	contType := "text/plain"
	perm := s3.BucketOwnerFull
	options := &s3.Options{
		SSE:  false,
		Meta: nil,
	}

	// Build the filename we'll use for S3:
	fileName := fmt.Sprintf("%v/%v/%v/%v/%v/%v.%v", *s3Path, time.Now().Year(), time.Now().Month(), time.Now().Day(), time.Now().Hour(), time.Now().Minute(), *s3FileExtention)

	// Upload the data:
	err = s3Bucket.Put(fileName, fileData, contType, perm, *options)
	if err != nil {
		log.Criticalf("Failed to put file (%v) on S3 (%v)", fileName, err)
		os.Exit(2)
	} else {
		log.Infof("Stored file (%v) on s3", fileName)
	}

	return nil
}
