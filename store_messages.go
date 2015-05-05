package main

import (
	"fmt"
	"github.com/bitly/go-nsq"
	log "github.com/cihub/seelog"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"time"
)

// Print messages to the screen:
func PrintMessages(messageBuffer []*nsq.Message) error {

	fileName := fmt.Sprintf("%v/%v/%v/%v/%v/%v", *s3Path, time.Now().Year(), time.Now().Month(), time.Now().Day(), time.Now().Hour(), time.Now().Minute())

	log.Infof("Would store in '%v'", fileName)

	for _, message := range messageBuffer {
		log.Debugf("Message: %v", string(message.Body))
	}

	return nil
}

// Store messages to S3:
func StoreMessages(messageBuffer []*nsq.Message) error {

	var fileData = make([]byte, 0)

	log.Infof("Storing %d messages...", len(messageBuffer))

	// Authenticate with AWS:
	awsAuth, err := aws.EnvAuth()
	if err != nil {
		log.Errorf("Unable to authenticate to AWS! (%s) ...\n", err)
		return err
	}

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
	fileName := fmt.Sprintf("%v/%v/%v/%v/%v/%v", *s3Path, time.Now().Year(), time.Now().Month(), time.Now().Day(), time.Now().Hour(), time.Now().Minute())

	// Turn the message bodies into a []byte:
	for _, message := range messageBuffer {
		fileData = append(fileData, message.Body...)
	}

	// Upload the data:
	err = s3Bucket.Put(fileName, fileData, contType, perm, *options)
	if err != nil {
		log.Errorf("Failed to put file on S3 (%v)", err)
	}

	return nil
}
