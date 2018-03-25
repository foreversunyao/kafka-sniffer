#!/bin/python2.7

'''Funtion.
1, Unpack the packet by sniffer from kafka tcp port
2, Get the producer(client) ip, port and kafka protocol related info
3, Support multi partitions in one request for the topic.
4, Support tcp/ip packet fragmentation
5, limit: this version support kafka 0.11.0.1
'''

'''Usage.
1, Run on kafka broker server
2, python kafka_sniffer.py -t topicname -s 0.0.0.0 -p 9092
'''

'''
Author:Samuel
Date:20180318
'''

import socket
import sys
import struct
import getopt
import array
import logging
import io
#clients={'golang':'sarama','default':'producer','cpp':'librdkafka','python':'pykafka'}

## Kafka version >= 0.11.0.1
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
'RecordBatchSize':-1,
'FirstOffset':-1,
'RecordBatchLength':-1,
'PartitionLeaderEpoch':-1,
'Magic':-1,
'Crc':-1,
'Attributes':-1,
'LastOffsetDelta':-1,
'FirstTimestamp':-1,
'MaxTimestamp':-1,
'ProducerId':-1,
'ProducerEpoch':-1,
'FirstSequence':-1,
'RecordCount':-1,
'RecordLength':-1,
'RecordAttributes':-1,
'RecordTimestampDelta':-1,
'RecordOffsetDelta':-1,
'KeyLen':-1,
'Key':None,
'ValueLen':-1,
'Value':None,
'Headers':None
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


def bytes_to_string(bytes_var):
	print "bytes_to_string: "+bytes_var
	return "".join(map(chr, bytes_var))

def decode_varint(stream,offset):
	print "decode_varint"
        shift = 0
        result = {'value': 0,'offset': offset}
        bytes_count = 1
        while True:
		print "result:"+stream
                i = ord(stream.read(1))
                result['value'] |= (i & 0x7f) << shift
                shift += 7
                bytes_count +=1
                if not (i & 0x80):
                        break
        result['offset'] = result['offset'] + bytes_count
	print "result:"+result['value']
        return result

## for kafka version >= 0.11.0.1
def get_recordbatch_data(data,offset,producer_data_len,partition_loop):
        recordbatch_head = struct.unpack('>IQIIBIHIQQQHI',data[offset:offset+61])
        kafka_data_output['RecordBatchSize'] = recordbatch_head[0]
        kafka_data_output['FirstOffset'] = recordbatch_head[1]
        kafka_data_output['RecordBatchLength'] = recordbatch_head[2]
        kafka_data_output['PartitionLeaderEpoch'] = recordbatch_head[3]
        kafka_data_output['Magic'] = int(recordbatch_head[4])
        kafka_data_output['Crc'] = recordbatch_head[5]
        kafka_data_output['Attributes'] = recordbatch_head[6]
        kafka_data_output['LastOffsetDelta'] = recordbatch_head[7]
        kafka_data_output['FirstTimestamp'] = recordbatch_head[8]
        kafka_data_output['MaxTimestamp'] = recordbatch_head[9]
        kafka_data_output['ProducerId'] = recordbatch_head[10]
        kafka_data_output['ProducerEpoch'] = recordbatch_head[11]
        kafka_data_output['FirstSequence'] = recordbatch_head[12]
        offset = offset + 61
        offset = get_record_data(data,offset,producer_data_len,partition_loop)
        return offset

## for kafka version >= 0.11.0.1
def get_record_data(data,offset,producer_data_len,partition_loop):
	print "get_record_data"
	print array.array('B',data)
	record_head = struct.unpack('>I',data[offset:offset+4])
	kafka_data_output['RecordCount'] = int(record_head[0])
	record_count = int(record_head[0])
	offset = offset + 4
        decode_varint_result = {'value': 0,'offset': 0 }
	#print kafka_data_output.items()
	s_data = bytes_to_string(data[offset:])
	result = decode_varint(io.BytesIO(s_data),offset)
	kafka_data_output['RecordLength'] = result['value']
	offset = offset + result['offset']
	record_attribute = struct.unpack('>B',data[offset:offset+1])
	kafka_data_output['RecordAttributes'] = record_attribute[0]
	offset = offset + 1
	result = decode_varint(data[offset:],offset)
	kafka_data_output['RecordTimestampDelta'] = result['value']
        offset = offset + result['offset']
	result = decode_varint(data[offset:],offset)
        kafka_data_output['RecordOffsetDelta'] = result['value']
        offset = offset + result['offset']
	result = decode_varint(data[offset:],offset)
        kafka_data_output['KeyLen'] = result['value']
        offset = offset + result['offset']
	kafka_data_output['Key'] = data[offset:offset+result['value']]
	offset = offset + result['value']
	result = decode_varint(data[offset:],offset)
	kafka_data_output['ValueLen'] = result['value']
        offset = offset + result['offset']
	kafka_data_output['Value'] = data[offset:offset+result['value']]
	offset = offset + result['value']
	kafka_data_output['Headers'] = data[offset:]
	print kafka_data_output.items()
	#length
	while int(record_head[0]) > 0:
		get_record_data(data,offset,producer_data_len)
		record_count = record_count - 1
	decode_varint(data)
	decode_bytes(data[offset:offset+1])
        record_head = struct.unpack('>?',data[offset:offset+1])
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
                	#offset = get_messageset_data(data,offset,producer_data_len,partition_loop)
                	offset = get_recordbatch_data(data,offset,producer_data_len,partition_loop)
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
