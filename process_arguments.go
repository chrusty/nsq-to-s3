package main

import (
	"flag"
	"fmt"
	"github.com/bitly/nsq/internal/version"
	log "github.com/cihub/seelog"
	"github.com/goamz/goamz/aws"
	"math/rand"
	"time"
)

// Process command-line arguments:
func processArguments() bool {

	// Make sure these log messages get out before this function ends:
	defer log.Flush()

	// Parse the command-line arguments:
	flag.Parse()

	// See if we've been asked to just print the version and exit:
	if *showVersion {
		log.Infof("nsq-to-s3 v%s\n", version.Binary)
		return true
	}

	// Ensure that the user has provided an S3 bucket:
	if *s3Bucket == "" {
		log.Warnf("--s3bucket is required")
		return true
	} else {
		log.Infof("S3-Bucket: %v", *s3Bucket)
	}

	// See if the user has provided a channel name, or invent a random one:
	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("nsq_to_s3-%06d#ephemeral", rand.Int()%999999)
	}
	log.Infof("Channel: %v", *channel)

	// Ensure that the user has provided a topic-name:
	if *topic == "" {
		log.Warnf("--topic is required")
		return true
	} else {
		log.Infof("Topic: %v", *topic)
	}

	// Ensure that the user has at least provided an NSQd or Lookupd address:
	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Warnf("--nsqd-tcp-address or --lookupd-http-address required")
		return true
	}

	// Ensure that the user hasn't tried to provide both NSQd and Lookupd addresses:
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Warnf("use --nsqd-tcp-address or --lookupd-http-address not both")
		return true
	}

	// Ensure that the user has provided a valid AWS region:
	_, regionExists := aws.Regions[*awsRegion]
	if !regionExists {
		log.Errorf("AWS Region (%s) doesn't exist!", *awsRegion)
		return true
	} else {
		log.Infof("aws-region: %v", *awsRegion)
	}

	// See which mode we've been asked to run in:
	switch *batchMode {
	case "disk":
		{
			log.Infof("Batch-mode: disk (messages will be stored on-disk between flushes)")
			log.Infof("Message-buffer-file: %v", *messageBufferFileName)
		}
	case "memory":
		{
			log.Infof("Batch-mode: memory (messages will be stored in-memory between flushes)")
		}
	case "channel":
		{
			log.Infof("Batch-mode: channel (messages will be left to accumulate in NSQ between flushes)")
		}
	default:
		{
			log.Warnf("Please specify a batch-mode from this list [disk, memory, channel]")
			return true
		}
	}

	// Print some info:
	log.Infof("Bucket-size (messages): %v", *bucketMessages)
	log.Infof("Bucket-size (seconds): %v", *bucketSeconds)
	log.Infof("Max-in-flight (messages): %v", *maxInFlight)
	log.Infof("Max-in-flight (seconds): %v", *maxInFlightTime)

	return false
}
