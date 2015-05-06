# nsq-to-s3
Stream an NSQ channel to S3

## Parameters:
* topic: The NSQ topic to subscribe to
* channel: An NSQ channel name to use (defaults to an automatically-generated ephemeral channel)
* maxInFlight: The maximum number of unFinished messages to allow (effectively a flush-batch size)
* maxInFlightTime: The maximum number of seconds to wait before flushing (in case maxInFlight is not enough)
* lookupdHTTPAddrs: The address of an NSQLookup daemon to connect to
* nsqdTCPAddrs: A specific NSQ daemon to connect to
* s3Bucket: The S3 bucket to store the files on (files will end up as s3Bucket/topic/YYYY/MM/DD/timestamp)
* bucketSeconds: The time-bucket-size of each file you want to end up with on S3, if we don't hit bucketMessages first (eg 3600 will give you one file on S3 per-hour)
* bucketMessages: Total number of messages to bucket (if bucketSeconds doesn't elapse first)
* consumerOpts: 

## Modes:
* "Abandoned-channel":
  * Subs to NSQ (creates a channel)
  * Waits for timeBucket to elapse
  * Pauses the channel
  * Takes all the messages off the queue, de-dupes in memory, sticks them on S3
  * Finish()es the messages
  * Unpauses the channel
  * Repeat
* "Batch-on-disk":
  * Subs to NSQ
  * De-dupes in memory (map[string][bool] where string is a hash of the message payload)
  * Once max-in-flight is reached it flushes messages to disk then Finish()es them
  * After timeBucket has elapsed it stops consuming, sticks the file on S3, clears the de-dupe map and continues
* "Continuous-sync-to-s3":
  * As with batch-on-disk but syncs to S3 every x seconds
  * Either overwrites the same file on S3, or piles up new ones
  * At the end of the time-bucket the interim files are removed from S3

## Bugs:
* Dupes can still occur around flush boundaries
