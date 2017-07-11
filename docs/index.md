# siddhi-io-kafka

This is an extension for siddhi kafka source and sink implementation.
This extension can be used to receive events from a kafka cluster and to publish events to a kafka cluster.

The Kafka Source receives records from a topic with a partition for a Kafka cluster which are in format such as
`text`, `XML` and `JSON`.
The Kafka Source will create the default partition for a given topic, if the topic is not already been created in the
Kafka cluster.

The Kafka Sink publishes records to a topic with a partition for a Kafka cluster which are in format such as `text`,
`XML` and `JSON`.
The Kafka Sink will create the default partition for a given topic, if the topic is not already been created in the
Kafka cluster.
The publishing topic and partition can be a dynamic value taken from the Siddhi event."

#### Prerequisites for using the feature
 - Download and install Kafka.
 - Then convert and copy the Kafka client jars from the <KAFKA_HOME>/libs directory to the <DAS_HOME>/libs directory
 as follows.
   - Create a directory (SOURCE_DIRECTORY) in a preferred location in your machine and copy the following JARs to it from the
   <KAFKA_HOME>/libs directory.
     - kafka_2.11-0.9.0.1.jar
     - kafka-clients-0.9.0.1.jar
     - metrics-core-2.2.0.jar
     - scala-library-2.11.7.jar
     - scala-parser-combinators_2.11-1.0.4.jar
     - zkclient-0.7.jar
     - zookeeper-3.4.6.jar
   - Create another directory (DESTINATION_DIRECTORY) in a preferred location in your machine.
   - To convert all the Kafka jars you copied into the <SOURCE_DIRECTORY>, issue the following command.
     For Windows: <DAS_HOME>/bin/jartobundle.bat <SOURCE_DIRECTORY_PATH> <DESTINATION_DIRECTORY_PATH>
     For Linux: <DAS_HOME>/bin/jartobundle.sh <SOURCE_DIRECTORY_PATH> <DESTINATION_DIRECTORY_PATH>
   - Copy the converted files from the <DESTINATION_DIRECTORY> to the <DAS_HOME>/libs directory.
   - Copy the jars that are not converted from the <SOURCE_DIRECTORY> to the <DAS_HOME>/samples/sample-clients/lib directory.

## How to Contribute
  * Please report issues at [Siddhi Github Issue Tacker](https://github.com/wso2-extensions/siddhi-io-kafka/issues)
  * Carbon Developers List : dev@wso2.org
  * Carbon Architecture List : architecture@wso2.org

**We welcome your feedback and contribution.**

Siddhi SP Team

## API Docs:

1. <a href="./api/4.0.3-SNAPSHOT.md">4.0.3-SNAPSHOT</a>
