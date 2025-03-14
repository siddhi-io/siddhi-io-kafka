# API Docs - v5.0.19

!!! Info "Tested Siddhi Core version: *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/">5.1.21</a>*"
    It could also support other Siddhi Core minor versions.

## Sink

### kafka *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">(Sink)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A Kafka sink publishes events processed by WSO2 SP to a topic with a partition for a Kafka cluster. The events can be published in the <code>TEXT</code> <code>XML</code> <code>JSON</code> or <code>Binary</code> format.<br>If the topic is not already created in the Kafka cluster, the Kafka sink creates the default partition for the given topic. The publishing topic and partition can be a dynamic value taken from the Siddhi event.<br>To configure a sink to use the Kafka transport, the <code>type</code> parameter should have <code>kafka</code> as its value.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(type="kafka", bootstrap.servers="<STRING>", topic="<STRING>", partition.no="<INT>", sequence.id="<STRING>", key="<STRING>", is.binary.message="<BOOL>", optional.configuration="<STRING>", is.synchronous="<BOOL>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">bootstrap.servers</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;"> This parameter specifies the list of Kafka servers to which the Kafka sink must publish events. This list should be provided as a set of comma separated values. e.g., <code>localhost:9092,localhost:9093</code>.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">topic</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The topic to which the Kafka sink needs to publish events. Only one topic must be specified.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">partition.no</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The partition number for the given topic. Only one partition ID can be defined. If no value is specified for this parameter, the Kafka sink publishes to the default partition of the topic (i.e., 0)</p></td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">sequence.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">A unique identifier to identify the messages published by this sink. This ID allows receivers to identify the sink that published a specific message.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The key contains the values that are used to maintain ordering in a Kafka partition.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">is.binary.message</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">In order to send the binary events via kafka sink, this parameter is set to 'True'.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">optional.configuration</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter contains all the other possible configurations that the producer is created with. <br>e.g., <code>producer.type:async,batch.size:200</code></p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">is.synchronous</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The Kafka sink will publish the events to the server synchronously when thevalue is set to <code>true</code>, and asynchronously if otherwise</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('TestExecutionPlan') 
define stream FooStream (symbol string, price float, volume long); 
@info(name = 'query1') 
@sink(
type='kafka',
topic='topic_with_partitions',
partition.no='0',
bootstrap.servers='localhost:9092',
@map(type='xml'))
Define stream BarStream (symbol string, price float, volume long);
from FooStream select symbol, price, volume insert into BarStream;

```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This Kafka sink configuration publishes to 0th partition of the topic named <code>topic_with_partitions</code>.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@App:name('TestExecutionPlan') 
define stream FooStream (symbol string, price float, volume long); 
@info(name = 'query1') 
@sink(
type='kafka',
topic='{{symbol}}',
partition.no='{{volume}}',
bootstrap.servers='localhost:9092',
@map(type='xml'))
Define stream BarStream (symbol string, price float, volume long); 
from FooStream select symbol, price, volume insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query publishes dynamic topic and partitions that are taken from the Siddhi event. The value for <code>partition.no</code> is taken from the <code>volume</code> attribute, and the topic value is taken from the <code>symbol</code> attribute.</p>
<p></p>
### kafka-replay-request *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">(Sink)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This sink is used to request replay of specific range of events on a specified partition of a topic.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(type="kafka-replay-request", sink.id="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">sink.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">a unique SINK_ID should be set. This sink id will be used to match with the appropriate kafka-replay-response source</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('TestKafkaReplay')

@sink(type='kafka-replay-request', sink.id='1')
define stream BarStream (topicForReplay string, partitionForReplay string, startOffset string, endOffset string);

@info(name = 'query1')
@source(type='kafka-replay-response',  group.id='group', threading.option='single.thread', bootstrap.servers='localhost:9092', sink.id='1',
@map(type='json'))
Define stream FooStream (symbol string, amount double);

@sink(type='log')
Define stream logStream(symbol string, amount double);

from FooStream select * insert into logStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this app we can send replay request events into BarStream and observe the replayed events in the logStream</p>
<p></p>
### kafkaMultiDC *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">(Sink)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A Kafka sink publishes events processed by WSO2 SP to a topic with a partition for a Kafka cluster. The events can be published in the <code>TEXT</code> <code>XML</code> <code>JSON</code> or <code>Binary</code> format.<br>If the topic is not already created in the Kafka cluster, the Kafka sink creates the default partition for the given topic. The publishing topic and partition can be a dynamic value taken from the Siddhi event.<br>To configure a sink to publish events via the Kafka transport, and using two Kafka brokers to publish events to the same topic, the <code>type</code> parameter must have <code>kafkaMultiDC</code> as its value.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(type="kafkaMultiDC", bootstrap.servers="<STRING>", topic="<STRING>", sequence.id="<STRING>", key="<STRING>", partition.no="<INT>", is.binary.message="<BOOL>", optional.configuration="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">bootstrap.servers</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;"> This parameter specifies the list of Kafka servers to which the Kafka sink must publish events. This list should be provided as a set of comma -separated values. There must be at least two servers in this list. e.g., <code>localhost:9092,localhost:9093</code>.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">topic</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The topic to which the Kafka sink needs to publish events. Only one topic must be specified.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">sequence.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">A unique identifier to identify the messages published by this sink. This ID allows receivers to identify the sink that published a specific message.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The key contains the values that are used to maintain ordering in a Kafka partition.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">partition.no</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The partition number for the given topic. Only one partition ID can be defined. If no value is specified for this parameter, the Kafka sink publishes to the default partition of the topic (i.e., 0)</p></td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">is.binary.message</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">In order to send the binary events via kafkaMultiDCSink, it is required to set this parameter to <code>true</code>.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">optional.configuration</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter contains all the other possible configurations that the producer is created with. <br>e.g., <code>producer.type:async,batch.size:200</code></p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('TestExecutionPlan') 
define stream FooStream (symbol string, price float, volume long); 
@info(name = 'query1') 
@sink(type='kafkaMultiDC', topic='myTopic', partition.no='0',bootstrap.servers='host1:9092, host2:9092', @map(type='xml'))Define stream BarStream (symbol string, price float, volume long);
from FooStream select symbol, price, volume insert into BarStream;

```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query publishes to the  default (i.e., 0th) partition of the brokers in two data centers </p>
<p></p>
## Source

### kafka *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">(Source)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A Kafka source receives events to be processed by WSO2 SP from a topic with a partition for a Kafka cluster. The events received can be in the <code>TEXT</code> <code>XML</code> <code>JSON</code> or <code>Binary</code> format.<br>If the topic is not already created in the Kafka cluster, the Kafka sink creates the default partition for the given topic.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(type="kafka", bootstrap.servers="<STRING>", topic.list="<STRING>", group.id="<STRING>", threading.option="<STRING>", partition.no.list="<STRING>", seq.enabled="<BOOL>", is.binary.message="<BOOL>", topic.offsets.map="<STRING>", enable.offsets.commit="<BOOL>", enable.async.commit="<BOOL>", optional.configuration="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">bootstrap.servers</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies the list of Kafka servers to which the Kafka source must listen. This list can be provided as a set of comma-separated values.<br>e.g., <code>localhost:9092,localhost:9093</code></p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">topic.list</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies the list of topics to which the source must listen. This list can be provided as a set of comma-separated values.<br>e.g., <code>topic_one,topic_two</code></p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">group.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is an ID to identify the Kafka source group. The group ID ensures that sources with the same topic and partition that are in the same group do not receive the same event.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">threading.option</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;"> This specifies whether the Kafka source is to be run on a single thread, or in multiple threads based on a condition. Possible values are as follows:<br><code>single.thread</code>: To run the Kafka source on a single thread.<br><code>topic.wise</code>: To use a separate thread per topic.<br><code>partition.wise</code>: To use a separate thread per partition.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">partition.no.list</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The partition number list for the given topic. This is provided as a list of comma-separated values. e.g., <code>0,1,2,</code>.</p></td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">seq.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to <code>true</code>, the sequence of the events received via the source is taken into account. Therefore, each event should contain a sequence number as an attribute value to indicate the sequence.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">is.binary.message</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">In order to receive binary events via the Kafka source,it is required to setthis parameter to 'True'.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">topic.offsets.map</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter specifies reading offsets for each topic and partition. The value for this parameter is specified in the following format: <br>&nbsp;<code>&lt;topic&gt;=&lt;offset&gt;,&lt;topic&gt;=&lt;offset&gt;,</code><br>&nbsp;&nbsp;When an offset is defined for a topic, the Kafka source skips reading the message with the number specified as the offset as well as all the messages sent previous to that message. If the offset is not defined for a specific topic it reads messages from the beginning. <br>e.g., <code>stocks=100,trades=50</code> reads from the 101th message of the <code>stocks</code> topic, and from the 51st message of the <code>trades</code> topic.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">enable.offsets.commit</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter specifies whether to commit offsets. <br>If the manual asynchronous offset committing is needed, <code>enable.offsets.commit</code> should be <code>true</code> and <code>enable.auto.commit</code> should be <code>false</code>. <br>If periodical committing is needed <code>enable.offsets.commit</code> should be <code>true</code> and <code>enable.auto.commit</code> should be <code>true</code>. <br>If committing is not needed, <code>enable.offsets.commit</code> should be <code>false</code>. <br><br>Note: <code>enable.auto.commit</code> is an <code>optional.configuration</code> property. If it is set to <code>true</code>, Source will periodically(default: 1000ms. Configurable with <code>auto.commit.interval.ms</code> property as an <code>optional.configuration</code>) commit its current offset (defined as the offset of the next message to be read) for the partitions it is reading from back to Kafka. <br>To guarantee at-least-once processing, we recommend you to enable Siddhi Periodic State Persistence when <code>enable.auto.commit</code> property is set to <code>true</code>. <br>During manual committing, it might introduce a latency during consumption.</p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">enable.async.commit</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter will changes the type of the committing offsets returned on the last poll for the subscribed list of topics and partitions.<br>When <code>enable.async.commit</code> is set to true, committing will be an asynchronous call.</p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">optional.configuration</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter contains all the other possible configurations that the consumer is created with. <br>e.g., <code>ssl.keystore.type:JKS,batch.size:200</code>.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('TestExecutionPlan') 
define stream BarStream (symbol string, price float, volume long); 
@info(name = 'query1') 
@source(
type='kafka', 
topic.list='kafka_topic,kafka_topic2', 
group.id='test', 
threading.option='partition.wise', 
bootstrap.servers='localhost:9092', 
partition.no.list='0,1', 
@map(type='xml'))
Define stream FooStream (symbol string, price float, volume long);
from FooStream select symbol, price, volume insert into BarStream;

```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This kafka source configuration listens to the <code>kafka_topic</code> and <code>kafka_topic2</code> topics with <code>0</code> and <code>1</code> partitions. A thread is created for each topic and partition combination. The events are received in the XML format, mapped to a Siddhi event, and sent to a stream named <code>FooStream</code>. </p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@App:name('TestExecutionPlan') 
define stream BarStream (symbol string, price float, volume long); 
@info(name = 'query1') 
@source(
type='kafka', 
topic.list='kafka_topic',
group.id='test', 
threading.option='single.thread',
bootstrap.servers='localhost:9092',
@map(type='xml'))
Define stream FooStream (symbol string, price float, volume long);
from FooStream select symbol, price, volume insert into BarStream;

```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This Kafka source configuration listens to the <code>kafka_topic</code> topic for the default partition because no <code>partition.no.list</code> is defined. Only one thread is created for the topic. The events are received in the XML format, mapped to a Siddhi event, and sent to a stream named <code>FooStream</code>.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@App:name('TestExecutionPlan')
@source(type='kafka',
        topic.list='trp_topic',
        partition.no.list='0',
        threading.option='single.thread',
        group.id='group',
        bootstrap.servers='localhost:9092',
        @map(type='xml', enclosing.element='//events',             @attributes(symbol ='symbol', price = 'price', volume = 'volume',                         partition = 'trp:partition',                         topic = 'trp:topic', key = 'trp:key',                         recordTimestamp = 'trp:record.timestamp',                          eventTimestamp = 'trp:event.timestamp',                         checkSum = 'trp:check.sum', topicOffset = 'trp:offset')))
define stream FooStream (symbol string, price float, volume long,                                      partition string,                                      topic string, key string,                                      recordTimestamp string,                                      eventTimestamp string, checkSum string,                                      topicOffset string);
from FooStream select * insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This Kafka source configuration listens to the <code>trp_topic</code> topic for the default partition because no <code>partition.no.list</code> is defined. <br>Since the custom attribute mapping is enabled with TRP values, the siddhi event will be populated with the relevant trp values as well</p>
<p></p>
### kafka-replay-response *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">(Source)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This source is used to listen to replayed events requested from kafka-replay-request sink</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(type="kafka-replay-response", bootstrap.servers="<STRING>", group.id="<STRING>", threading.option="<STRING>", sink.id="<INT>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">bootstrap.servers</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies the list of Kafka servers to which the Kafka source must listen. This list can be provided as a set of comma-separated values.<br>e.g., <code>localhost:9092,localhost:9093</code></p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">group.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is an ID to identify the Kafka source group. The group ID ensures that sources with the same topic and partition that are in the same group do not receive the same event.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">threading.option</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;"> This specifies whether the Kafka source is to be run on a single thread, or in multiple threads based on a condition. Possible values are as follows:<br><code>single.thread</code>: To run the Kafka source on a single thread.<br><code>topic.wise</code>: To use a separate thread per topic.<br><code>partition.wise</code>: To use a separate thread per partition.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">sink.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">a unique SINK_ID .</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('TestKafkaReplay')

@sink(type='kafka-replay-request', sink.id='1')
define stream BarStream (topicForReplay string, partitionForReplay string, startOffset string, endOffset string);

@info(name = 'query1')
@source(type='kafka-replay-response',  group.id='group', threading.option='single.thread', bootstrap.servers='localhost:9092', sink.id='1',
@map(type='json'))
Define stream FooStream (symbol string, amount double);

@sink(type='log')
Define stream logStream(symbol string, amount double);

from FooStream select * insert into logStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this app we can send replay request events into BarStream and observe the replayed events in the logStream</p>
<p></p>
### kafkaMultiDC *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">(Source)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">The Kafka Multi-Datacenter(DC) source receives records from the same topic in brokers deployed in two different kafka clusters. It filters out all the duplicate messages and ensuresthat the events are received in the correct order using sequential numbering. It receives events in formats such as <code>TEXT</code>, <code>XML</code> JSON<code> and </code>Binary`.The Kafka Source creates the default partition '0' for a given topic, if the topic has not yet been created in the Kafka cluster.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(type="kafkaMultiDC", bootstrap.servers="<STRING>", topic="<STRING>", partition.no="<INT>", is.binary.message="<BOOL>", optional.configuration="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">bootstrap.servers</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This contains the kafka server list which the kafka source listens to. This is given using comma-separated values. eg: 'localhost:9092,localhost:9093' </p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">topic</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is the topic that the source listens to. eg: 'topic_one' </p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">partition.no</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is the partition number of the given topic.</p></td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">is.binary.message</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">In order to receive the binary events via the Kafka Multi-DC source, the value of this parameter needs to be set to 'True'.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">optional.configuration</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This contains all the other possible configurations with which the consumer can be created.eg: producer.type:async,batch.size:200</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('TestExecutionPlan') 
define stream BarStream (symbol string, price float, volume long); 
@info(name = 'query1') 
@source(type='kafkaMultiDC', topic='kafka_topic', bootstrap.servers='host1:9092,host1:9093', partition.no='1', @map(type='xml'))
Define stream FooStream (symbol string, price float, volume long);
from FooStream select symbol, price, volume insert into BarStream;

```
<p></p>
<p style="word-wrap: break-word;margin: 0;">The following query listens to 'kafka_topic' topic, deployed in the broker host1:9092 and host1:9093, with partition 1. A thread is created for each broker. The receiving xml events are mapped to a siddhi event and sent to the FooStream.</p>
<p></p>
