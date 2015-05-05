package main

import (
	"flag"
	"fmt"
	log "github.com/cihub/seelog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/app"
	"github.com/bitly/nsq/internal/version"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic           = flag.String("topic", "", "NSQ topic")
	channel         = flag.String("channel", "", "NSQ channel")
	maxInFlight     = flag.Int("max-in-flight", 1000, "max number of messages to allow in flight (before flushing)")
	maxInFlightTime = flag.Int("max-in-flight-time", 60, "max time to keep messages in flight (before flushing)")
	bucketMessages  = flag.Int("bucket-messages", 0, "total number of messages to bucket")
	bucketSeconds   = flag.Int("bucket-seconds", 600, "total time to bucket messages for (seconds)")
	s3Bucket        = flag.String("s3bucket", "", "S3-bucket (and path) to store the output on (eg 's3://nsq-archive/live'")
	storeStrings    = flag.Bool("storeStrings", true, "Store message bodies as strings (rather than bytes)")

	consumerOpts     = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
)

func init() {
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

// Process command-line arguments:
func processArguments() bool {
	// Make sure these log messages get out before this function ends:
	defer log.Flush()

	flag.Parse()

	if *showVersion {
		log.Infof("nsq-to-s3 v%s\n", version.Binary)
		return true
	}

	if *s3Bucket == "" {
		log.Warnf("--s3bucket is required")
		return true
	} else {
		log.Infof("S3-Bucket: %v", *s3Bucket)
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("nsq_to_s3-%06d#ephemeral", rand.Int()%999999)
		log.Infof("Channel: %v", *channel)
	}

	if *topic == "" {
		log.Warnf("--topic is required")
		return true
	} else {
		log.Infof("Topic: %v", *topic)
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Warnf("--nsqd-tcp-address or --lookupd-http-address required")
		return true
	}

	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Warnf("use --nsqd-tcp-address or --lookupd-http-address not both")
		return true
	}

	log.Infof("Bucket-size (messages): %v", *bucketMessages)
	log.Infof("Bucket-size (seconds): %v", *bucketSeconds)
	log.Infof("Max-in-flight (messages): %v", *maxInFlight)
	log.Infof("Max-in-flight (seconds): %v", *maxInFlightTime)
	log.Infof("Store-strings: %v", *storeStrings)

	return false
}

func main() {
	defer log.Flush()

	argumentIssue := processArguments()
	if argumentIssue {
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Don't ask for more messages than we want
	if *bucketMessages > 0 && *bucketMessages < *maxInFlight {
		*maxInFlight = *bucketMessages
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = fmt.Sprintf("nsq_to_s3/%s go-nsq/%s", version.Binary, nsq.VERSION)
	err := app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		panic(err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		panic(err)
	}

	messageHandler := &InMemoryHandler{
		allTimeMessages:  0,
		messagesInFlight: 0,
		deDuper:          make(map[string]int),
		messageBuffer:    make([]*nsq.Message, 0),
		timeLastFlushed:  int(time.Now().Unix()),
	}

	consumer.AddHandler(messageHandler)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		panic(err)
	}

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
			os.Exit(0)
		}
	}
}
