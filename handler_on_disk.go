package main

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/bitly/go-nsq"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"os"
	"time"
)

type OnDiskHandler struct {
	allTimeMessages       int64
	inFlightMessages      []*nsq.Message
	deDuper               map[string]int
	messagesBuffered      int64
	timeLastFlushedToS3   int
	timeLastFlushedToDisk int
}

// Message handler:
func (handler *OnDiskHandler) HandleMessage(m *nsq.Message) error {

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
		// Count the number of in-flight messages:
		handler.allTimeMessages++

		// Count the number of messages buffered:
		handler.messagesBuffered++

		// Add it to the de-duper:
		handler.deDuper[hashKey] = int(time.Now().Unix())

		// Add the message to the message-buffer:
		handler.inFlightMessages = append(handler.inFlightMessages, m)

		// Finish() the message immediately (we're storing this in-memory):
		m.Finish()
	}

	// See if we need to flush to disk:
	if (len(handler.inFlightMessages) == *maxInFlight) || (int(time.Now().Unix())-handler.timeLastFlushedToDisk >= *maxInFlightTime) {
		log.Debugf("Flushing %d in-flight messages to disk (%v)...", len(handler.inFlightMessages), *messageBufferFileName)
		handler.FlushInFlightMessages()
	}

	// See if we need to flush to S3:
	if (handler.messagesBuffered == int64(*bucketMessages)) || (int(time.Now().Unix())-handler.timeLastFlushedToS3 >= *bucketSeconds) {
		log.Infof("Flushing buffer to S3 ...")
		handler.FlushBufferToS3()
	}

	return nil
}

// Flush the in-flight messages:
func (handler *OnDiskHandler) FlushInFlightMessages() error {

	var messageBufferFile *os.File
	var fileData []byte

	// See if the file exists:
	_, err := os.Stat(*messageBufferFileName)
	if err != nil {
		// Create the buffer-file:
		messageBufferFile, err = os.Create(*messageBufferFileName)
	} else {
		// Open the buffer-file:
		messageBufferFile, err = os.OpenFile(*messageBufferFileName, os.O_RDWR, 0600)
	}
	if err != nil {
		log.Criticalf("Unable to open buffer-file! (%v) %v", *messageBufferFileName, err)
		os.Exit(2)
	} else {
		// Make sure we Close() the file, no matter what:
		defer messageBufferFile.Close()
	}

	// Seek to the end of the file:
	_, _ = messageBufferFile.Seek(0, os.SEEK_END)

	// Turn the message bodies into a []byte:
	for _, message := range handler.inFlightMessages {
		fileData = append(fileData, message.Body...)
		fileData = append(fileData, []byte("\n")...)
	}

	// Append messages to the bufferfile:
	messageBufferSize, err := messageBufferFile.Write(fileData)
	if err != nil {
		log.Criticalf("Unable to write to the buffer-file! (%v) %v", *messageBufferFileName, err)
		os.Exit(2)
	} else {
		log.Debugf("Wrote %d bytes to disk", messageBufferSize)
	}

	// Reset the handler:
	handler.inFlightMessages = make([]*nsq.Message, 0)
	handler.timeLastFlushedToDisk = int(time.Now().Unix())

	return nil
}

// Flush the message-buffer:
func (handler *OnDiskHandler) FlushBufferToS3() error {

	log.Debugf("Messages processed (since the beginning): %d", handler.allTimeMessages)

	// Read the messages from disk:
	fileData, err := ioutil.ReadFile(*messageBufferFileName)
	if err != nil {
		log.Criticalf("Unable to read buffer-file! (%v) %v", *messageBufferFileName, err)
		os.Exit(2)
	}

	// Store them on S3:
	err = StoreMessages(fileData)
	if err != nil {
		log.Criticalf("Unable to store messages! %v", err)
		os.Exit(2)
	}

	// Reset the handler:
	handler.deDuper = make(map[string]int)
	handler.timeLastFlushedToS3 = int(time.Now().Unix())
	handler.messagesBuffered = 0
	os.Remove(*messageBufferFileName)

	return nil

}
