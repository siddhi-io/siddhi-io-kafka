siddhi-io-kafka
======================================

The **siddhi-io-kafka extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a>.
This implements siddhi kafka source and sink that can be used to receive events from a kafka cluster and to publish
events to a kafka cluster.

The Kafka Source receives records from a topic with a partition for a Kafka cluster which are in format such as
`text`, `XML` and `JSON`.
The Kafka Source will create the default partition for a given topic, if the topic is not already been created in the
Kafka cluster.

The Kafka Sink publishes records to a topic with a partition for a Kafka cluster which are in format such as `text`,
`XML` and `JSON`.
The Kafka Sink will create the default partition for a given topic, if the topic is not already been created in the
Kafka cluster.
The publishing topic and partition can be a dynamic value taken from the Siddhi event."

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-kafka">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-kafka/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-kafka/issues">Issue tracker</a>

## Latest API Docs

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/api/4.0.13">4.0.13</a>.

## How to use

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

***Prerequisites for using the feature***
 - Download and install Kafka and Zookeeper.
 - Start the Apache ZooKeeper server with the following command: `bin/zookeeper-server-start.sh config/zookeeper.properties`.
 - Start the Kafka server with the following command:  `bin/kafka-server-start.sh config/server.properties`
 - Convert and copy the Kafka client jars from the <KAFKA_HOME>/libs directory to the <SP_HOME>/libs directory as follows.
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
     For Windows: <SP_HOME>/bin/jartobundle.bat <SOURCE_DIRECTORY_PATH> <DESTINATION_DIRECTORY_PATH>
     For Linux: <SP_HOME>/bin/jartobundle.sh <SOURCE_DIRECTORY_PATH> <DESTINATION_DIRECTORY_PATH>
   - Copy the converted files from the <DESTINATION_DIRECTORY> to the <SP_HOME>/libs directory.
   - Copy the jars that are not converted from the <SOURCE_DIRECTORY> to the <SP_HOME>/samples/sample-clients/lib directory.


* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support.

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this
extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-kafka/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.io.kafka</groupId>
        <artifactId>siddhi-io-kafka</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ |
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-kafka/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-kafka/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/api/4.0.13/#kafka-sink">kafka</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>)*<br><div style="padding-left: 1em;"><p>A Kafka sink publishes events processed by WSO2 SP to a topic with a partition for a Kafka cluster. The events can be published in the <code>TEXT</code> <code>XML</code> or <code>JSON</code> format.<br>If the topic is not already created in the Kafka cluster, the Kafka sink creates the default partition for the given topic. The publishing topic and partition can be a dynamic value taken from the Siddhi event.<br>To configure a sink to use the Kafka transport, the <code>type</code> parameter should have <code>kafka</code> as its value.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/api/4.0.13/#kafkamultidc-sink">kafkaMultiDC</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>)*<br><div style="padding-left: 1em;"><p>A Kafka sink publishes events processed by WSO2 SP to a topic with a partition for a Kafka cluster. The events can be published in the <code>TEXT</code> <code>XML</code> or <code>JSON</code> format.<br>If the topic is not already created in the Kafka cluster, the Kafka sink creates the default partition for the given topic. The publishing topic and partition can be a dynamic value taken from the Siddhi event.<br>To configure a sink to publish events via the Kafka transport, and using two Kafka brokers to publish events to the same topic, the <code>type</code> parameter must have <code>kafkaMultiDC</code> as its value.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/api/4.0.13/#kafka-source">kafka</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>)*<br><div style="padding-left: 1em;"><p>A Kafka source receives events to be processed by WSO2 SP from a topic with a partition for a Kafka cluster. The events received can be in the <code>TEXT</code> <code>XML</code> or <code>JSON</code> format.<br>If the topic is not already created in the Kafka cluster, the Kafka sink creates the default partition for the given topic.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/api/4.0.13/#kafkamultidc-source">kafkaMultiDC</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>)*<br><div style="padding-left: 1em;"><p>The Kafka Multi Data Center(DC) Source receives records from the same topic in brokers deployed in two different kafka cluster. It will filter out all duplicate messages and try to ensurethat the events are received in the correct order by using sequence numbers. events are received in format such as <code>text</code>, <code>XML</code> and <code>JSON</code>.The Kafka Source will create the default partition  '0' for a given topic, if the topic is not already been created in the Kafka cluster.</p></div>

## How to Contribute

  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-kafka/issues">GitHub Issue Tracker</a>.

  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-kafka/tree/master">master branch</a>.

## Contact us

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>.

 * Siddhi developers can be contacted via the mailing lists:

    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)

    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)

## Support

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology.

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.
