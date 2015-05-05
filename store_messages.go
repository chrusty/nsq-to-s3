package main

import (
	"github.com/bitly/go-nsq"
	log "github.com/cihub/seelog"
)

// Print messages to the screen:
func PrintMessages(messageBuffer []*nsq.Message) error {

	for _, message := range messageBuffer {
		log.Debugf("Message: %v", string(message.Body))
	}

	return nil
}

// Store messages to S3:
func StoreMessages(messageBuffer []*nsq.Message) error {

	log.Infof("Storing %d messages...", len(messageBuffer))

	return nil
}
