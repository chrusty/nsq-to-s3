# nsq-to-s3
Stream an NSQ channel to S3

## Parameters:
* topic: The NSQ topic to subscribe to
* channel: An NSQ channel name to use (defaults to an automatically-generated ephemeral channel)
* max-in-flight: The maximum number of unFinished messages to allow (effectively a flush-batch size)
* max-in-flight-time: The maximum number of seconds to wait before flushing (in case maxInFlight is not enough)
* lookupd-http-address: The address of an NSQLookup daemon to connect to
* nsqd-tcp-address: A specific NSQ daemon to connect to
* bucket-seconds: The time-bucket-size of each file you want to end up with on S3, if we don't hit bucketMessages first (eg 3600 will give you one file on S3 per-hour)
* bucket-messages: Total number of messages to bucket (if bucketSeconds doesn't elapse first)
* s3bucket: The S3 bucket to store the files on (files will end up as s3Bucket/topic/YYYY/MM/DD/HH/timestamp)
* s3path
* awsregion: 
* batchmode: 
* bufferfile: 
* consumerOpts: 

## Modes (current):
NSQ-to-S3 can operate in several different modes, depending on your storage and/or durability requirements:

### "Batch-on-disk":
  * Subs to NSQ
  * De-dupes in memory (map[string][bool] where string is a hash of the message payload)
  * Once max-in-flight is reached it flushes messages to disk then Finish()es them
  * After timeBucket has elapsed it stops consuming, sticks the file on S3, clears the de-dupe map and continues
  * **You would be well-advised to use some kind of persistent storage (EBS for example)**

### "In-memory":
As with batch-on-disk but all messages are kept in-memory between flushes to S3. **If you stop the process then you will lose messages!**

## Modes (planned):

### "Abandoned-channel":
  * Subs to NSQ (creates a channel)
  * Waits for timeBucket to elapse
  * Pauses the channel
  * Takes all the messages off the queue, de-dupes in memory, sticks them on S3
  * Finish()es the messages
  * Unpauses the channel
  * Repeat

### "Continuous-sync-to-s3":
  * As with batch-on-disk but syncs to S3 every x seconds
  * Either overwrites the same file on S3, or piles up new ones
  * At the end of the time-bucket the interim files are removed from S3

## Examples:

### Consuming a topic, buffering on disk, flushing in-flight at 1000 messages, flushing to S3 every 5 minutes:
```
nsq-to-s3 -s3bucket=hailo-cruft-live -topic=jstats.allingested -channel='nsq-to-s3#ephemeral' -lookupd-http-address=10.0.2.197:4161 -s3path=/nsq -awsregion=eu-west-1 -bucket-seconds=300 -max-in-flight=1000 -batchmode=disk
```
> nsq-to-s3 -s3bucket=hailo-cruft-live -topic=jstats.allingested -channel='nsq-to-s3#ephemeral' -lookupd-http-address=10.0.2.197:4161 -s3path=/nsq -awsregion=eu-west-1 -bucket-seconds=300 -max-in-flight=1000 -batchmode=disk
> prawn

## Bugs (current):
* Dupes can still occur around flush boundaries

## Bugs (fixed):
* ~~User can specify a non-existent AWS region~~
