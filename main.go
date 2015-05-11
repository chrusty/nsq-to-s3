package main

import (
	"flag"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/app"
	"github.com/bitly/nsq/internal/version"
	log "github.com/cihub/seelog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic                 = flag.String("topic", "", "NSQ topic")
	channel               = flag.String("channel", "", "NSQ channel")
	maxInFlight           = flag.Int("max-in-flight", 1000, "max number of messages to allow in flight (before flushing)")
	maxInFlightTime       = flag.Int("max-in-flight-time", 60, "max time to keep messages in flight (before flushing)")
	bucketMessages        = flag.Int("bucket-messages", 0, "total number of messages to bucket")
	bucketSeconds         = flag.Int("bucket-seconds", 600, "total time to bucket messages for (seconds)")
	s3Bucket              = flag.String("s3bucket", "", "S3 bucket-name to store the output on (eg 'nsq-archive'")
	s3Path                = flag.String("s3path", "", "S3 path to store files under (eg '/nsq-archive'")
	awsRegion             = flag.String("awsregion", "us-east-1", "The AWS region-name to connect to")
	batchMode             = flag.String("batchmode", "memory", "How to batch the messages between flushes [disk, memory, channel]")
	messageBufferFileName = flag.String("bufferfile", "", "Local file to buffer messages in between flushes to S3")
	s3FileExtention       = flag.String("extention", "txt.gz", "Extention for files on S3")

	consumerOpts     = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
)

func init() {
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

func main() {
	// Make sure we flush the log before quitting:
	defer log.Flush()

	// Process the arguments:
	argumentIssue := processArguments()
	if argumentIssue {
		os.Exit(1)
	}

	// Intercept quit signals:
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Don't ask for more messages than we want
	if *bucketMessages > 0 && *bucketMessages < *maxInFlight {
		*maxInFlight = *bucketMessages
	}

	// Set up the NSQ client:
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

	// See which mode we've been asked to run in:
	switch *batchMode {
	case "disk":
		{
			// On-disk:
			messageHandler := &OnDiskHandler{
				allTimeMessages:       0,
				deDuper:               make(map[string]int),
				inFlightMessages:      make([]*nsq.Message, 0),
				timeLastFlushedToS3:   int(time.Now().Unix()),
				timeLastFlushedToDisk: int(time.Now().Unix()),
			}

			// Add the handler:
			consumer.AddHandler(messageHandler)
		}
	case "channel":
		{
			panic("'channel' batch-mode isn't implemented yet!")
		}
	default:
		{
			// Default to in-memory:
			messageHandler := &InMemoryHandler{
				allTimeMessages:     0,
				deDuper:             make(map[string]int),
				messageBuffer:       make([]*nsq.Message, 0),
				timeLastFlushedToS3: int(time.Now().Unix()),
			}

			// Add the handler:
			consumer.AddHandler(messageHandler)
		}
	}

	// Configure the NSQ connection with the list of NSQd addresses:
	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		panic(err)
	}

	// Configure the NSQ connection with the list of Lookupd HTTP addresses:
	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		panic(err)
	}

	// Handle stop / quit events:
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
