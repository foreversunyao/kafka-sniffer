# Kafka-sniffer
kafka-sniffer is a network traffic analyzer tool for apache kafka, help you find the details of producer request
This script tool can capture and analyze producer request packets to Apache Kafka Broker server, and outputs are the details of producer request message, including *SourceIP, SourcePort, DestIP, DestPort, DataLen, ApiKey, ApiVersion, CorrelationId, Client, RequiredAcks, Timeout, TopicCount, TopicName, PartitionCount, Partition, MessageSetSize, Offset, Crc, MessageSize, Magic, Attribute, Timestamp, Key, Value*.


# Requirements
Verified on CentOS 6.8 and Centos 7.0

Script need to have root privileges



# Usage and demo

```
python kafka-sniffer.py -h
Useage: python kafka-sniffer.py -t <topic> -s <source> -p <kafka_port>
        -t topic name
        -s source ip address, if all sources, source=0.0.0.0
        -p kafka port
```

```
python kafka-sniffer.py -t kafka-sniffer-topic -s 0.0.0.0 -p 9092

topic: kafka-sniffer-topic
source: 0.0.0.0
port: 9092
======== From 10.65.128.251:63302, Topic kafka-sniffer-topic, Partition Loop 1, Partition 0 =======
[('MessageSetSize', 222), ('PartitionCount', 1), ('ApiVersion', 2), ('TopicCount', 1), ('DestPort', 9092), ('ApiKey', 0), ('DestIP', '10.65.20.44'), ('Timestamp', 1520754766237), ('RequiredAcks', 1), ('Key', 'a'), ('Offset', 0), ('SourceIP', '10.65.132.85'), ('CorrelationId', 1), ('Magic', 1), ('Partition', 0), ('Value', 'bb'), ('Crc', 1031103259), ('Client', 'kafka-python-producer-1'), ('Timeout', 30000), ('Attribute', 0), ('MessageSize', 25), ('DataLen', 298), ('SourcePort', 40639), ('TopicName', 'kafka-sniffer-topic')]
[('MessageSetSize', 222), ('PartitionCount', 1), ('ApiVersion', 2), ('TopicCount', 1), ('DestPort', 9092), ('ApiKey', 0), ('DestIP', '10.65.20.44'), ('Timestamp', 1520754766238), ('RequiredAcks', 1), ('Key', 'a'), ('Offset', 1), ('SourceIP', '10.65.132.85'), ('CorrelationId', 1), ('Magic', 1), ('Partition', 0), ('Value', 'bb'), ('Crc', 1256960491), ('Client', 'kafka-python-producer-1'), ('Timeout', 30000), ('Attribute', 0), ('MessageSize', 25), ('DataLen', 298), ('SourcePort', 40639), ('TopicName', 'kafka-sniffer-topic')]
```



# Install and dependence

Just a python script, need to run on the kafka broker server.

Python Version: python 2.7.

Python package:
```
import socket, sys
import getopt
import array
import struct
```

# Limit
Right now only support Apache Kafka Version < 0.11.0.1, because since Kafka 0.11.0.1 there is a big change to message format, like support transaction and so on.

# Todo
Will add support to Kafka >= 0.11.0.1

# Related
[Apache Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
