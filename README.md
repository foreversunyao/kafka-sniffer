# Kafka-sniffer
kafka-sniffer is a network traffic analyzer tool for apache kafka, help you find the details of producer request
This script tool can capture and analyze producer request packets to Apache Kafka Broker server, and outputs are the details of producer request message, including *SourceIP, SourcePort, DestIP, DestPort, DataLen, ApiKey, ApiVersion, CorrelationId, Client, RequiredAcks, Timeout, TopicCount, TopicName, PartitionCount, Partition, MessageSetSize, Offset, Crc, MessageSize, Magic, Attribute, Timestamp, Key, Value*.


# System requirements
Verified on CentOS 6.8 and Centos 7.0

Script need to have root privileges


# Usage

```
python kafka-sniffer.py -h
Useage: python kafka-sniffer.py -t <topic> -s <source> -p <kafka_port>
        -t topic name
        -s source ip address, if all sources, source=0.0.0.0
        -p kafka port
```

# Install and dependence


# Limit


# Todo


# Related
[Apache Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
