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

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/api/4.0.3-SNAPSHOT">4.0.3-SNAPSHOT</a>.

## How to use

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support.

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this
extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-kafka/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.io.kafka</groupId>
        <artifactId>siddhi-io-kafka-parent</artifactId>
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

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/api/4.0.3-SNAPSHOT/#kafka-sink">kafka</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sinks">Sink</a>)*<br><div style="padding-left: 1em;"><p>The Kafka Sink publishes records to a topic with a partition for a Kafka cluster which are in format such as <code>text</code>, <code>XML</code> and <code>JSON</code>.<br>The Kafka Sink will create the default partition for a given topic, if the topic is not already been created in the Kafka cluster. The publishing topic and partition can be a dynamic value taken from the Siddhi event</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/api/4.0.3-SNAPSHOT/#kafka-source">kafka</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sources">Source</a>)*<br><div style="padding-left: 1em;"><p>The Kafka Source receives records from a topic with a partition for a Kafka cluster which are in format such as <code>text</code>, <code>XML</code> and <code>JSON</code>.<br>The Kafka Source will create the default partition for a given topic, if the topic is not already been created in the Kafka cluster.</p></div>

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
