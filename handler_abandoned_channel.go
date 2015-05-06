package main

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/bitly/go-nsq"
	log "github.com/cihub/seelog"
	"time"
)

type AbandonedChannelHandler struct {
	allTimeMessages int64
	deDuper         map[string]int
	messageBuffer   []*nsq.Message
	timeLastFlushed int
}

// Message handler:
func (handler *AbandonedChannelHandler) HandleMessage(m *nsq.Message) error {

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

	// Now see if we need to flush:
	if len(handler.messageBuffer) == *bucketMessages {
		log.Infof("Flushing buffer (buffer-size = bucketMessages / %d)...", *bucketMessages)
		handler.FlushBuffer()
	}

	if int(time.Now().Unix())-handler.timeLastFlushed >= *bucketSeconds {
		log.Infof("Flushing buffer (buffer-age >= bucketSeconds / %d)...", *bucketSeconds)
		handler.FlushBuffer()
	}

	return nil
}

// Flush the message-buffer:
func (handler *AbandonedChannelHandler) FlushBuffer() error {

	var fileData []byte

	log.Debugf("Messages processed (since the beginning): %d", handler.allTimeMessages)

	err := StoreMessages(fileData)
	if err != nil {
		log.Errorf("Unable to store messages! %v", err)
		return err
	} else {
		// Reset the de-duper:
		handler.deDuper = make(map[string]int)

		// Reset the message-buffer:
		handler.messageBuffer = make([]*nsq.Message, 0)

		// Reset the timer:
		handler.timeLastFlushed = int(time.Now().Unix())

		return nil
	}
}
