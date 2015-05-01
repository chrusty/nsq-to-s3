package main

import (
	log "github.com/cihub/seelog"

	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/bitly/go-nsq"
)

type InMemoryHandler struct {
	messagesInFlight int
	deDuper          map[string]int
	messageBuffer    [][]byte
}

// Message handler:
func (handler *InMemoryHandler) HandleMessage(m *nsq.Message) error {

	// Disable auto-response for the message (we'll take care of Finish()ing it later):
	m.DisableAutoResponse()

	// Make a hash of message payload:
	hash := sha256.Sum256(m.Body)
	hashKey := hex.EncodeToString(hash[:])

	// See if we already have this message in the de-duper:
	if _, ok := handler.deDuper[hashKey]; ok {
		log.Debugf("We already have this message - discarding dupe!")
		m.Finish()
	} else {
		// Add it to the de-duper:
		handler.deDuper[hashKey] = int32(time.Now().Unix())

		// Add the message to the message-buffer:
		handler.messageBuffer = append(handler.messageBuffer, m)
	}

	return nil
}
