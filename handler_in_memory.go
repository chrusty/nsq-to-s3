package main

import (
	log "github.com/cihub/seelog"

	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/bitly/go-nsq"
)

type InMemoryHandler struct {
	allTimeMessages  int64
	messagesInFlight int
	deDuper          map[string]int
	messageBuffer    []*nsq.Message
	timeLastFlushed  int
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
		// log.Debugf("We already have this message - discarding dupe!")
		m.Finish()
	} else {
		// log.Debugf("Adding message (%d) to the buffer...", len(handler.messageBuffer))

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

// Message handler:
func (handler *InMemoryHandler) FlushBuffer() error {

	log.Debugf("Messages processed (since the beginning): %d", handler.allTimeMessages)

	err := PrintMessages(handler.messageBuffer)
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
