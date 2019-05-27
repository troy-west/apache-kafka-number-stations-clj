# Apache Kafka Three Ways: Number Stations

A practical exercise introducing the TestTopologyDriver to drive development of a streaming compute application.

Use in unison with [TW AK3W Workshop](https://github.com/troy-west/apache-kafka-three-ways), a solution to this exercise is [available here](https://github.com/troy-west/apache-kafka-number-stations-sln).

This project is also available [in Java](https://github.com/troy-west/apache-kafka-number-stations).

# Background

[Number Stations](https://en.wikipedia.org/wiki/Numbers_station) are shortwave radio stations that broadcast formatted numbers, which are believed to be addressed to intelligence officers operating in foreign countries.

We created a radio that captured ~3hrs of mysterious global Number Station broadcasts, taking the form of 1.5M messages from 541 Number Stations world-wide. The radio also captured some spurious messages of no interest.

Can we filter, branch, translate, group, window, and aggregate these messages to decode the hidden message?

# Troubleshooting

#### Downloading Oracle Java JDK 11+

1. Visit https://www.oracle.com/technetwork/java/javase/downloads/index.html
2. This project requires Java 9+ (we suggest 11+)

#### Docker

If you are having problems with docker similar to the following error messages, try restarting docker:

```
driver failed programming external connectivity on endpoint
-----
input/output error
```

----

## Initialize and Monitor a Cluster

##### Note: These broker nodes are accessible on localhost:8082, 8083, and 8083.

Using [troy-west/apache-kafka-cli-tools](https://github.com/troy-west/apache-kafka-cli-tools):

Start a 3-node Kafka Cluster and enter a shell with all kafka-tools scripts:
```sh
docker-compose down
docker-compose up -d
docker-compose -f docker-compose.tools.yml run kafka-tools
```

In a new terminal, view the running kafka logs:
```sh
docker-compose logs -f
```

Create a new Topic 'radio-logs' with 12 partitions and RF=3:

```sh
# ./bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic radio-logs --partitions 12 --replication-factor 3
```

Confirm the new topic has been created:

```sh
# ./bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list

radio-logs
```

Describe the new topic:

```sh
# ./bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic radio-logs

Topic:radio-logs	PartitionCount:12	ReplicationFactor:3	Configs:
	Topic: radio-logs	Partition: 0	Leader: 3	Replicas: 3,2,1	Isr: 3,2,1
	Topic: radio-logs	Partition: 1	Leader: 1	Replicas: 1,3,2	Isr: 1,3,2
	Topic: radio-logs	Partition: 2	Leader: 2	Replicas: 2,1,3	Isr: 2,1,3
	Topic: radio-logs	Partition: 3	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: radio-logs	Partition: 4	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: radio-logs	Partition: 5	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: radio-logs	Partition: 6	Leader: 3	Replicas: 3,2,1	Isr: 3,2,1
	Topic: radio-logs	Partition: 7	Leader: 1	Replicas: 1,3,2	Isr: 1,3,2
	Topic: radio-logs	Partition: 8	Leader: 2	Replicas: 2,1,3	Isr: 2,1,3
	Topic: radio-logs	Partition: 9	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: radio-logs	Partition: 10	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: radio-logs	Partition: 11	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
```

Take a look at the data in each partition (initially empty)

```
./bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka-1:19092 --topic radio-logs --time -1

radio-logs:0:0
radio-logs:1:0
radio-logs:2:0
radio-logs:3:0
radio-logs:4:0
radio-logs:5:0
radio-logs:6:0
radio-logs:7:0
radio-logs:8:0
radio-logs:9:0
radio-logs:10:0
radio-logs:11:0
```

# Decoding the secret message, part by part.

At any time, run all the project tests with ```lein test```

# Examine the Data

## Sample the radio

Take a look at a sample of twenty intercepted messages:

```clojure
(require '[numbers.radio :as radio])
=> nil
(radio/sample)
=>
({:time 1557125670764, :type "ENG", :name "315", :long 22, :lat 7, :content ["one"]}
 {:time 1557125670765, :type "UXX", :name "X-RAY"}
 {:time 1557125670776, :type "ENG", :name "150", :long -60, :lat -20, :content ["one"]}
 {:time 1557125670777, :type "UXX", :name "X-RAY"}
 {:time 1557125670781, :type "MOR", :name "371", :long 50, :lat 16, :content [".----"]}
 {:time 1557125670787, :type "GER", :name "133", :long -68, :lat -22, :content ["eins"]}
 {:time 1557125670787, :type "MOR", :name "392", :long 61, :lat 20, :content [".----"]}
 {:time 1557125670789, :type "ENG", :name "315", :long 22, :lat 7, :content ["one"]}
 {:time 1557125670798, :type "ENG", :name "417", :long 73, :lat 24, :content ["one"]}
 {:time 1557125670799, :type "ENG", :name "NZ1", :long 166, :lat -78, :content ["one"]}
 {:time 1557125670799, :type "GER", :name "073", :long -98, :lat -32, :content ["eins"]}
 {:time 1557125670801, :type "ENG", :name "150", :long -60, :lat -20, :content ["one"]}
 {:time 1557125670806, :type "MOR", :name "371", :long 50, :lat 16, :content [".----"]}
 {:time 1557125670812, :type "GER", :name "133", :long -68, :lat -22, :content ["eins"]}
 {:time 1557125670812, :type "MOR", :name "392", :long 61, :lat 20, :content [".----"]}
 {:time 1557125670814, :type "ENG", :name "315", :long 22, :lat 7, :content ["one"]}
 {:time 1557125670823, :type "ENG", :name "417", :long 73, :lat 24, :content ["one"]}
 {:time 1557125670824, :type "ENG", :name "NZ1", :long 166, :lat -78, :content ["one"]}
 {:time 1557125670824, :type "GER", :name "073", :long -98, :lat -32, :content ["eins"]}
 {:time 1557125670824, :type "ENG", :name "429", :long 79, :lat 26, :content ["one"]})
```

We have messages of type English, German, and Morse Code. There are also some spurious of type 'UXX'.

# Produce Secret Radio Data to Kafka

## Implement the JSON Serializer / Deserializer

In order to create a producer that sends Message objects to Kafka, first we create the Serializer and Deserializer classes, it's also worth having a quick look at the MessageSerde class which uses both.

* Get numbers.JsonDeserializerTest passing 
* Get numbers.JsonSerializerTest passing

Do we throw or swallow exceptions in the serialization classes, and why?

## Implement Producer.main

Create a new KafkaProducer and send each message returned by SecretRadio.listen() to the radio-logs topic.

What happens if you don't close the Producer and allow the JVM to exit immediately?

Once implemented, you can produce the full broadcast to radio-logs via:

```bash
mvn compile exec:java -Dexec.mainClass="numbers.Producer"
```

# Build Streaming Compute, Test First

## Implement the MessageTimeExtractor

We want our messages to be interpreted at the time they declare in the :time field rather than producer or log time. At this point we cover the different concepts of time in Kafka, and the big idea of deterministic recomputabiity. Broadly similar to favouring pure functions without side-effects in a functional programming sense.

* Get numbers.MessageTimeExtractor passing 
* Get numbers.ComputeTest.testStreamTimestampExtraction passing

Why should you never return a static number (like 0L) from the extractor? Does it impact compaction, deletion, etc?

## Filter Known Messages

We provide a Translator class that can decode individual messages from German, English, and Morse Code into numeric.

Use ```streams.filter(...)``` and ```Translator.knows(Message message)``` to filter out unknown messages.

* Get numbers.ComputeTest.testFilterKnown passing

## Branch Scott Base / Rest of the World

We are told that Scott Base is a special station that should be considered independently.

Use ```streams.branch(...)``` to split the filtered stream in two. Scott Base is the only station below -75 latitude.

* Get numbers.ComputeTest.testBranchRestOfWorld passing
* Get numbers.ComputeTest.testBranchScottBase passing

## Translate Known, Rest of World Messages

Use ```streams.map(...)``` or ```streams.mapValues(...)``` to translate the message stream with ```Translator.translate(...)```

* Get numbers.ComputeTest.testTranslate passing

Why do we prefer streams.mapValues in this case? What is the consequence of using map?

## Correlate Messages by Station and Time Window

We are told that each station produces three messages in every 10s tumbling time window.

Group the translated stream by Station, window the resultant stream by 10s time windows, and aggregate that grouped, windowed stream such that the three messages in a time window are reduced into a single message with three numbers as the content.

In this case we use ```streams.groupByKey()```, ```streams.windowBy(...)```, and ```streams.aggregate(...)```.

Make sure the KTable that results from the aggregation is materialized as 'PT10S-Store'.
 
* Get numbers.ComputeTest.testCorrelate passing

## When the Tests Pass

You have implemented the Kafka Streams Topology described in the streaming compute presentation slides

It turns out that there are 540 number stations, and they each broadcast 960 tuples of three numbers.

Those three numbers are RBG values, and we can reconstitute a 540px by 960px png image from the stream.

You can run that topology against your local cluster:

```bash
mvn clean compile exec:java -Dexec.mainClass="numbers.App"
```

Navigate to localhost:8080 to inspect the decoded message! It may take a minute or two to process.

While the logs are being computed you can check on progress by looking at the offsets of the consumer group

```
./bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --group compute-radio-logs --describe

TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                                                                          HOST            CLIENT-ID
radio-logs      11         27541           103504          75963           compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      4          27670           129498          101828          compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      10         27447           97830           70383           compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      6          18714           135357          116643          compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      3          18729           144000          125271          compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      8          27688           117879          90191           compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      0          18735           152640          133905          compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      7          21479           143891          122412          compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      1          18390           89205           70815           compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      2          27770           158125          130355          compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      9          18719           155520          136801          compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
radio-logs      5          27747           129372          101625          compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer-c21b4661-18fb-422b-981c-38b0615950c1 /172.27.0.1     compute-radio-logs-5a520771-acfe-49d4-bf44-8976ff8d23cf-StreamThread-1-consumer
```
As you progress through this project you can always reset the consumer offets like so:

```
./bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --group compute-radio-logs --reset-offsets --to-earliest --execute --topic radio-logs
```

# Parallelising our Compute

What happens when you run more than one application (say on ports 8081, 8082, 8083), and why?

## Build the app JAR

mvn clean compile package

## Run multiple versions of the app at once on different ports

* java -jar target/apache-kafka-java-number-stations-1.0-SNAPSHOT-jar-with-dependencies.jar 8080 &
* java -jar target/apache-kafka-java-number-stations-1.0-SNAPSHOT-jar-with-dependencies.jar 8081 &

It's a case for interactive queries!

# An Extension

The decoded image doesn't quite match the source (src/main/resources/source.png).

That's because Scott Base is special. It broadcasts a rotation factor for each time window, e.g.

* 0 - don't rotate any of the three numbers.
* 1 - rotate the numbers left one ([111 222 333] becomes [222 333 111]).
* 2 - rotate each number left two ([111 222 333] becomes [333 111 222]).

If we use Scott Base as a further cipher we could aggregate that cipher to a different ktable, then join the stream
of correlated messages with that, rotating where appropriate. This is left up to the adventurous (and may require dropping into the Processor API)

If you do complete the extension problem, please do raise a PR to the [solution project](https://github.com/troy-west/apache-kafka-number-stations-sln)!

----

Copyright © 2019 Troy-West, Pty Ltd. MIT Licensed.