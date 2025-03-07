Siddhi IO Kafka
======================================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-kafka/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-kafka/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-io-kafka.svg)](https://github.com/siddhi-io/siddhi-io-kafka/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-io-kafka.svg)](https://github.com/siddhi-io/siddhi-io-kafka/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-io-kafka.svg)](https://github.com/siddhi-io/siddhi-io-kafka/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-io-kafka.svg)](https://github.com/siddhi-io/siddhi-io-kafka/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-io-kafka extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that receives and publishes events from and to Kafka.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 5.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.io.kafka/siddhi-io-kafka/">here</a>.
* Versions 4.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.io.kafka/siddhi-io-kafka">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-kafka/api/5.0.19">5.0.19</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-kafka/api/5.0.19/#kafka-sink">kafka</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">Sink</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">A Kafka sink publishes events processed by WSO2 SP to a topic with a partition for a Kafka cluster. The events can be published in the <code>TEXT</code> <code>XML</code> <code>JSON</code> or <code>Binary</code> format.<br>If the topic is not already created in the Kafka cluster, the Kafka sink creates the default partition for the given topic. The publishing topic and partition can be a dynamic value taken from the Siddhi event.<br>To configure a sink to use the Kafka transport, the <code>type</code> parameter should have <code>kafka</code> as its value.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-kafka/api/5.0.19/#kafka-replay-request-sink">kafka-replay-request</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">Sink</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This sink is used to request replay of specific range of events on a specified partition of a topic.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-kafka/api/5.0.19/#kafkamultidc-sink">kafkaMultiDC</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">Sink</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">A Kafka sink publishes events processed by WSO2 SP to a topic with a partition for a Kafka cluster. The events can be published in the <code>TEXT</code> <code>XML</code> <code>JSON</code> or <code>Binary</code> format.<br>If the topic is not already created in the Kafka cluster, the Kafka sink creates the default partition for the given topic. The publishing topic and partition can be a dynamic value taken from the Siddhi event.<br>To configure a sink to publish events via the Kafka transport, and using two Kafka brokers to publish events to the same topic, the <code>type</code> parameter must have <code>kafkaMultiDC</code> as its value.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-kafka/api/5.0.19/#kafka-source">kafka</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">Source</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">A Kafka source receives events to be processed by WSO2 SP from a topic with a partition for a Kafka cluster. The events received can be in the <code>TEXT</code> <code>XML</code> <code>JSON</code> or <code>Binary</code> format.<br>If the topic is not already created in the Kafka cluster, the Kafka sink creates the default partition for the given topic.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-kafka/api/5.0.19/#kafka-replay-response-source">kafka-replay-response</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">Source</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This source is used to listen to replayed events requested from kafka-replay-request sink</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-kafka/api/5.0.19/#kafkamultidc-source">kafkaMultiDC</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">Source</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">The Kafka Multi-Datacenter(DC) source receives records from the same topic in brokers deployed in two different kafka clusters. It filters out all the duplicate messages and ensuresthat the events are received in the correct order using sequential numbering. It receives events in formats such as <code>TEXT</code>, <code>XML</code> JSON<code> and </code>Binary`.The Kafka Source creates the default partition '0' for a given topic, if the topic has not yet been created in the Kafka cluster.</p></p></div>

## Installation

For installing this extension in the Streaming Integrator Server, and to add the dependent jars, refer Streaming Integrator documentation section on <a target="_blank" href="https://ei.docs.wso2.com/en/7.1.0/streaming-integrator/connectors/downloading-and-Installing-Siddhi-Extensions/">downloading and installing siddhi extensions</a>.\
For installing this extension in the Streaming Integrator Tooling, and to add the dependent jars, refer Streaming Integrator documentation section on <a target="_blank" href="https://ei.docs.wso2.com/en/7.1.0/streaming-integrator/develop/installing-siddhi-extensions/">installing siddhi extensions</a>.

## Dependencies 

Following JARs will be converted to osgi and copied to `WSO2SI_HOME/lib` and `WSO2SI_HOME/samples/sample-clients/lib` which are in `<KAFKA_HOME>/libs` directory.

 - kafka_2.11-*.jar
 - kafka-clients-*.jar
 - metrics-core-*.jar
 - scala-library-2.11.*.jar
 - scala-parser-combinators_2.11.*.jar (if exists)
 - zkclient-*.jar
 - zookeeper-*.jar 

#### Setup Kafka

As a prerequisite, you have to start the Kafka message broker. Please follow better steps.
1. Download the Kafka [distribution](https://kafka.apache.org/downloads)
2. Unzip the above distribution and go to the ‘bin’ directory
3. Start the zookeeper by executing below command,
    ```bash
    zookeeper-server-start.sh config/zookeeper.properties
    ```
4. Start the Kafka broker by executing below command,
    ```bash
    kafka-server-start.sh config/server.properties
    ```

Refer the Kafka documentation for more details, https://kafka.apache.org/quickstart 

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-io-kafka/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.
