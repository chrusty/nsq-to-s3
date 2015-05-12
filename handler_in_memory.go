package main

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/bitly/go-nsq"
	log "github.com/cihub/seelog"
	"os"
	"time"
)

type InMemoryHandler struct {
	allTimeMessages     int64
	deDuper             map[string]int
	messageBuffer       []*nsq.Message
	timeLastFlushedToS3 int
}

// Message handler:
func (handler *InMemoryHandler) HandleMessage(m *nsq.Message) error {

	// Count all the messages:
	handler.allTimeMessages++

	// Disable auto-response for the message (we'll take care of Finish()ing it later):
	m.DisableAutoResponse()

	// Make a hash of message payload:
	hash := sha256.Sum256(m.Body)
	hashKey := hex.EncodeToString(hash[:])

	// See if we already have this message in the de-duper:
	if _, ok := handler.deDuper[hashKey]; ok {
		m.Finish()
	} else {
		// Add it to the de-duper:
		handler.deDuper[hashKey] = int(time.Now().Unix())

		// Add the message to the message-buffer:
		handler.messageBuffer = append(handler.messageBuffer, m)

		// Finish() the message immediately (we're storing this in-memory):
		m.Finish()
	}

	// See if we need to flush to S3:
	if (len(handler.messageBuffer) == *bucketMessages) || (int(time.Now().Unix())-handler.timeLastFlushedToS3 >= *bucketSeconds) {
		log.Infof("Flushing buffer to S3 ...")
		handler.FlushBufferToS3()
	}

	return nil
}

// Flush the message-buffer:
func (handler *InMemoryHandler) FlushBufferToS3() error {

	log.Debugf("Messages processed (since the beginning): %d", handler.allTimeMessages)

	// A byte array to submit to S3:
	var fileData []byte

	// Turn the message bodies into a []byte:
	for _, message := range handler.messageBuffer {
		fileData = append(fileData, message.Body...)
		fileData = append(fileData, []byte("\n")...)
	}

	// Store them on S3:
	err := StoreMessages(fileData)
	if err != nil {
		log.Criticalf("Unable to store messages! %v", err)
		os.Exit(2)
	}

	// Reset the handler:
	handler.deDuper = make(map[string]int)
	handler.messageBuffer = make([]*nsq.Message, 0)
	handler.timeLastFlushedToS3 = int(time.Now().Unix())

	return nil

}
