package main

import (
	"bytes"
	"bufio"
	"compress/gzip"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"os"
	"time"
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
		os.Exit(2)
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
	options := &s3.Options{
		SSE:  false,
		Meta: nil,
	}

	// Get a multi-part handler:
	multiPartHandler, err := s3Bucket.InitMulti(remoteFileName, contType, perm)
	if err != nil {
		log.Criticalf("Failed to get a multi-part handler (%v) on S3 (%v)", remoteFileName, err)
		os.Exit(2)
	}

    file, err := os.Open(fileToBeUploaded)

         if err != nil {
                 fmt.Println(err)
                 os.Exit(1)
         }

         defer file.Close()

         fileInfo, _ := file.Stat()
         var fileSize int64 = fileInfo.Size()
         bytes := make([]byte, fileSize)

         // read into buffer
         buffer := bufio.NewReader(file)
         _, err = buffer.Read(bytes)

         // then we need to determine the file type
         // see https://www.socketloop.com/tutorials/golang-how-to-verify-uploaded-file-is-image-or-allowed-file-types

         filetype := http.DetectContentType(bytes)

         // set up for multipart upload
         multi, err := bucket.InitMulti(s3path, filetype, s3.ACL("public-read"))

         if err != nil {
                 fmt.Println(err)
                 os.Exit(1)
         }

         // this is for PutPart ( see https://godoc.org/launchpad.net/goamz/s3#Multi.PutPart)

         // calculate the number of parts by dividing up the file size by 5MB
         const fileChunk = 5242880 // 5MB in bytes

         parts, err := multi.PutAll(file, fileChunk)

         if err != nil {
                 fmt.Println(err)
                 os.Exit(1)
         }

         err = multi.Complete(parts)



	 else {
		log.Infof("Stored file (%v) on s3", remoteFileName)
	}

	return nil
}
