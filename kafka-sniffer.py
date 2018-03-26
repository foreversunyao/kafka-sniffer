#!/bin/python2.7

'''Funtion.
1, Unpack the packet by sniffer from kafka tcp port
2, Get the producer(client) ip, port and kafka protocol related info
3, Support multi partitions in one request for the topic.
4, Support tcp/ip packet fragmentation
5, limit: support kafka version < 0.11.0.1 right now, due to there is a big change on message format since kafka 0.11.0.1
'''

'''Usage.
1, Run on kafka broker server
2, python kafka_sniffer.py -t topicname -s 0.0.0.0 -p 9092
'''

'''
Author:Samuel
Date:20180218
'''

import socket
import sys
import struct
import sys
import getopt
import array
import logging

#clients={'golang':'sarama','default':'producer','cpp':'librdkafka','python':'pykafka'}

## Kafka version < 0.11.0.1
kafka_data_output={
'SourceIP':None,
'SourcePort':-1,
'DestIP':None,
'DestPort':-1,
'DataLen':-1,
'ApiKey':-1,
'ApiVersion':-1,
'CorrelationId':-1,
'Client':None,
'RequiredAcks':-1,
'Timeout':-1,
'TopicCount':-1,
'TopicName':None,
'PartitionCount':-1,
'Partition':-1,
'MessageSetSize':-1,
'Offset':-1,
'MessageSize':-1,
'Crc':None,
'Magic':-1,
'Attribute':-1,
'Timestamp':-1,
'Key':None,
'Value':None
}

sniffer_info_log = logging.FileHandler(filename='kafka-sniffer.data.log', mode='w', encoding='utf-8')
fmt = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s -%(module)s:  %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
sniffer_info_log.setFormatter(fmt)

sniffer_debug_log = logging.FileHandler(filename='kafka-sniffer.debug.log', mode='w', encoding='utf-8')
fmt = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s -%(module)s:  %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
sniffer_debug_log.setFormatter(fmt)

info_logger = logging.Logger(name='kafka_data', level=logging.INFO)
info_logger.addHandler(sniffer_info_log)

# if want to get debug info, please change level to logging.DEBUG as below
# debug_logger = logging.Logger(name='kafka_debug', level=logging.DEBUG)
debug_logger = logging.Logger(name='kafka_debug', level=logging.INFO)
debug_logger.addHandler(sniffer_debug_log)

def get_messageset_data(data,offset,producer_data_len,partition_loop):
	messageset_head = struct.unpack('>I',data[offset:offset+4])
	kafka_data_output['MessageSetSize'] = messageset_head[0]
	offset = offset + 4
	offset = get_message_data(data,offset,producer_data_len,partition_loop)
	return offset

def get_message_data(data,offset,producer_data_len,partition_loop):
	message_head = struct.unpack('>QI',data[offset:offset+12])
	kafka_data_output['Offset'] = message_head[0]
	kafka_data_output['MessageSize'] = message_head[1]
	offset = offset + 12
	message = struct.unpack('>IBB',data[offset:offset+6])
        kafka_data_output['Crc'] = message[0]
        kafka_data_output['Magic'] = int(message[1])
        kafka_data_output['Attribute'] = int(message[2])
	offset = offset + 6
	## kafka version >= 0.10
        if kafka_data_output['Magic'] == 1:
		kafka_data_output['Timestamp'] = struct.unpack('>Q',data[offset:offset+8])
                offset = offset + 8
        key_len = struct.unpack('>I',data[offset:offset+4])
	offset = offset + 4
        ## key is None,default 255,255,255,255
        if key_len[0] == 4294967295:
        	kafka_data_output['Key'] = None
        else:
                kafka_data_output['Key'] = data[offset:offset+key_len[0]]
                offset = offset + key_len[0]
        value_len = struct.unpack('>I',data[offset:offset+4])
	offset = offset + 4
       	kafka_data_output['Value'] = data[offset:offset+value_len[0]]
	offset = offset + value_len[0]
	info_logger.info(kafka_data_output.items())
	if partition_loop > 1:
		return offset
	if offset < producer_data_len:
		get_message_data(data,offset,producer_data_len,partition_loop)
	else:
		return offset

def get_topic_data(data,offset,topic,producer_data_len):
	topic_attribute = struct.unpack('>IH',data[offset:offset+6])
        kafka_data_output['TopicCount'] = topic_attribute[0]
        kafka_data_output['TopicLen'] = topic_attribute[1]
        offset = offset + 6
        kafka_data_output['TopicName'] = data[offset:offset+kafka_data_output['TopicLen']]
	offset = offset + kafka_data_output['TopicLen']
        if kafka_data_output['TopicName'] == topic:
                partition_count = struct.unpack('>I',data[offset:offset+4])
               	kafka_data_output['PartitionCount'] = partition_count[0]
                partition_loop = partition_count[0]
		offset = offset + 4
		while offset < producer_data_len and partition_loop > 0:
			partition = struct.unpack('>I',data[offset:offset+4])
			info_logger.info('======== From %s:%s, Topic %s, Partition Loop %s, Partition %s =======' % (kafka_data_output['SourceIP'],kafka_data_output['SourcePort'],kafka_data_output['TopicName'],partition_loop,partition[0]))
                	kafka_data_output['Partition'] = partition[0]
                	offset = offset + 4
                	offset = get_messageset_data(data,offset,producer_data_len,partition_loop)
			partition_loop = partition_loop - 1
		return
def get_producer_data(data,topic):
	## apikey + apiversion + correlationid + client
	offset = 14
	if offset > len(data):
		return
	try:
		client = struct.unpack('>IHHIH',data[0:offset])
		## Producer ApiKey
		if client[1] == 0:
			kafka_data_output['DataLen'] = client[0]
			producer_data_len = client[0]
                        kafka_data_output['ApiKey'] = client[1]
                        kafka_data_output['ApiVersion'] = client[2]
                        kafka_data_output['CorrelationId'] = client[3]
			client_len = client[4]
			kafka_data_output['Client'] = data[offset:offset+client_len]
			offset = offset + client_len
			## if new protocol version
			if client[2] == 3:
				offset = offset + 2
			client_attribute = struct.unpack('>HI',data[offset:offset+6])
			kafka_data_output['RequiredAcks'] = client_attribute[0]
                        kafka_data_output['Timeout'] = client_attribute[1]
			offset = offset + 6
			get_topic_data(data,offset,topic,producer_data_len)
			return
	except Exception as e:
		debug_logger.debug('The traffic from %s:%s struct.unpack failed.' % (kafka_data_output['SourceIP'],kafka_data_output['SourcePort']))
		return


def unpack_tcpip_packet(source,port,topic):
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
	except socket.error , msg:
		debug_logger.debug('Socket create failed. Error Code : %s Message %s ' % (str(msg[0]),msg[1]))
        	sys.exit()
	## data buffer queue
    	data_buffer = bytes()
	## tcp/ip conn pair
	tcp_worker = {}
	while True:
	    	packet = s.recvfrom(65565)
	    	packet = packet[0]
	    	## ip header
	    	ip_header = packet[0:20]
	    	iph = struct.unpack('!BBHHHBBH4s4s' , ip_header)
	        ## tcp header
	        version_ihl = iph[0]
	        version = version_ihl >> 4
	        ihl = version_ihl & 0xF
	        iph_length=ihl * 4
	        tcp_header = packet[iph_length:iph_length+20]
	        tcph = struct.unpack('!HHLLBBHHH' , tcp_header)
	    	ttl = iph[5]
	    	protocol = iph[6]
	    	s_addr = socket.inet_ntoa(iph[8]);
	    	d_addr = socket.inet_ntoa(iph[9]);
	        ## tcp header
	    	tcp_header = packet[iph_length:iph_length+20]
	    	tcph = struct.unpack('!HHLLBBHHH' , tcp_header)
	    	s_port = tcph[0]
	    	d_port = tcph[1]
	    	sequence = tcph[2]
	    	acknowledgement = tcph[3]
	    	doff_reserved = tcph[4]
	    	tcph_length = doff_reserved >> 4
	        ## data
	        h_size = iph_length + tcph_length * 4
	        data_size = len(packet) - h_size
	        data = packet[h_size:]

		kafka_data_output['SourceIP'] = s_addr
		kafka_data_output['SourcePort'] = s_port
		kafka_data_output['DestIP'] = d_addr
		kafka_data_output['DestPort'] = d_port
		key = s_addr + ',' + str(s_port) + ',' +d_addr + ',' + str(d_port)

		## solve ip/tcp fragmentation
		first_packet = False
		if key not in tcp_worker:
			first_packet = True
			tcp_worker[key] = data
		else:
			tcp_worker[key] = tcp_worker[key] + data
		## parse head of data
		while True:
			if len(tcp_worker[key]) < 14:
				debug_logger.debug('The traffic from %s:%s is too short(len:%s) to analyze.' % (s_addr,s_port,len(tcp_worker[key])))
				tcp_worker.pop(key, None)
				break
			try:
				client = struct.unpack('>IHHIH',tcp_worker[key][0:14])
			except Exception as e:
				## first packet can not be unpacked
				if first_packet :
					tcp_worker.pop(key, None)
				debug_logger.debug('The packet from %s:%s is fragmentation, or upack failed' % (s_addr,s_port))
				break
			kafka_data_size = client[0]
			if len(tcp_worker[key]) < 4 + kafka_data_size:
				if first_packet :
					tcp_worker.pop(key, None)
				debug_logger.debug('The packet from %s:%s is fragmentation, or upack failed' % (s_addr,s_port))
				break
			if source == '0.0.0.0':
				get_producer_data(tcp_worker[key],topic)
				tcp_worker[key] = tcp_worker[key][4+kafka_data_size:]
			elif source == s_addr:
				get_producer_data(tcp_worker[key],topic)
                        	tcp_worker[key] = tcp_worker[key][4+kafka_data_size:]
			else:
				break
		## tcp/ip conn pair is None
		if key in tcp_worker and len(tcp_worker[key]) == 0:
			debug_logger.debug('Remove tcp/ip conn pair %s from tcp_worker' % (key))
			tcp_worker.pop(key, None)
def main():
	try:
        	opts, args = getopt.getopt(sys.argv[1:], '-ht:s:p:',['h','t','s','p'])
	except getopt.GetoptError:
        	print 'Error: kafka_sniffer.py -t <topic> -s <source> -p <kafka_port>'
        	sys.exit(2)
        if len(opts) == 0:
		print 'Error: python kafka_sniffer.py -h for help'
		sys.exit(2)
	for opt, arg in opts:
        	if opt == '-h' :
            		print ' kafka_sniffer.py -t <topic> -s <source> -p <kafka_port>'
            		print ' if all topics, topic=all'
            		print ' if all sources, source=0.0.0.0'
            		sys.exit()
		if opt in ('-t'):
			topic=arg
		if opt in ('-s'):
			source=arg
		if opt in ('-p'):
			port=arg
	print 'Input: '
	print 'Topic: ' + topic
	print 'Source: ' + source
	print 'Port: ' + port
        unpack_tcpip_packet(source,port,topic)
if __name__ == '__main__':
    	main()
