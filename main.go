package main

import (
	"crypto/sha256"
	"encoding/hex"
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

	topic         = flag.String("topic", "", "NSQ topic")
	channel       = flag.String("channel", "", "NSQ channel")
	maxInFlight   = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	totalMessages = flag.Int("n", 0, "total messages to show (will wait if starved)")

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
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_s3 v%s\n", version.Binary)
		return
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("nsq_to_s3%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Warnf("--topic is required")
		os.exit(1)
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Warnf("--nsqd-tcp-address or --lookupd-http-address required")
		os.exit(1)
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Errorf("use --nsqd-tcp-address or --lookupd-http-address not both")
		os.exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Don't ask for more messages than we want
	if *totalMessages > 0 && *totalMessages < *maxInFlight {
		*maxInFlight = *totalMessages
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

	consumer.AddHandler(&InMemoryHandler{})

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
		}
	}
}
