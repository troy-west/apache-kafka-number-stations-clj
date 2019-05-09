# Apache Kafka Three Ways: Number Stations

Use in unison with the TW AK3W Workshop: https://kafka.troywest.com

We have captured a mysterious ~3hr broadcast of global Number Station data.

In raw form it is 1.5M messages in different languages.

Can we filter it, translate it, correlate it, and decode the hidden message?

# Initialize Kafka + Kakfa Tools

Using [troy-west/apache-kafka-cli-tools](https://github.com/troy-west/apache-kafka-cli-tools)

Start a 3-node Kafka Cluster and enter a shell with all kafka-tools scripts:
```sh
docker-compose down
docker-compose up -d
docker-compose -f docker-compose.tools.yml run kafka-tools
```

## Monitor

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

Now, from within this project:

Take a look at the Number Station data

```
(radio/sample)
```

Then:

* Send the full ```(radio/listen)``` data to your local Kafka Cluster
* Complete the compute tests using the TopologyTestRunner
* Build the application and run it against local Kafka
* See the decoded message!

Once the compute tests are complete, you can run the application like so:

```
java -jar target/number-stations-0.1.0-SNAPSHOT-standalone.jar 8082 &
```

What happens when you run more than one application (say on ports 8081, 8082, 8083), and why?
