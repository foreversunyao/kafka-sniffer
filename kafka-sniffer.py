#!/bin/python2.7

"""Funtion.
1, Unpack the packet by sniffer from kafka tcp port
2, Get the producer(client) ip, port and kafka protocol related info
3, limit: support kafka version < 0.11.0.1 , due to there is a big change on message format since kafka 0.11.0.1
"""

"""Usage.
1, Run on kafka broker server
2, python kafka_sniffer.py -t topicname -s 0.0.0.0 -p 9092
"""

"""
Author:Samuel
Date:20180218
"""

import socket, sys
from struct import *
import getopt
import array


clients={'golang':"sarama",'default':"producer",'cpp':"librdkafka",'python':"pykafka"}

def get_messageset_data(data,offset,output,kafka_data_size):
	messageset_head = unpack('>I',data[offset:offset+4])
	offset = offset + 4
        output['MessageSetSize'] = messageset_head[0]
	get_message_data(data,offset,output,kafka_data_size)
	return

def get_message_data(data,offset,output,kafka_data_size):
	print "============= One Message Start =================="
	message_head = unpack('>QI',data[offset:offset+12])
	offset = offset + 12
	output['Offset'] = message_head[0]
	output['MessageSize'] = message_head[1]
	message = unpack('>I??',data[offset:offset+6])
        output['Crc'] = message[0]
        output['Magic'] = int(message[1])
        output['Attribute'] = int(message[2])
	offset = offset + 6
	## kafka version >= 0.10
        if output['Magic'] == 1:
		output['Timestamp'] = unpack('>Q',data[offset:offset+8])
                offset = offset + 8
        key_len = unpack('>I',data[offset:offset+4])
	offset = offset + 4
        ## key is None
        if key_len[0] == 4294967295:
        	output['Key'] = None
        else:
                output['Key'] = data[offset:offset+key_len[0]]
                offset = offset + key_len[0]
        value_len = unpack('>I',data[offset:offset+4])
	offset = offset + 4
        ## print "value_len: "+str(value_len)
       	output['Value'] = data[offset:offset+value_len[0]]
	offset = offset + value_len[0]
	print output.items()
        print "============= One Message End =================="
	## print "offset: " + str(offset)
	## print "kafka_data_size: " + str(kafka_data_size)
	if offset < kafka_data_size:
		get_message_data(data,offset,output,kafka_data_size)
	else:
		return

def get_topic_data(data,offset,output,topic,kafka_data_size):
	topic_attribute = unpack('>IH',data[offset:offset+6])
        output['TopicCount'] = topic_attribute[0]
        output['TopicLen'] = topic_attribute[1]
        offset = offset + 6
        output['TopicName'] = data[offset:offset+output['TopicLen']]
	offset = offset + output['TopicLen']
        if output['TopicName'] == topic:
        	print "============= Request Start =================="
                partition = unpack('>II',data[offset:offset+8])
               	output['PartitionCount'] = partition[0]
                #partition_loop = partition[0]
                output['Partition'] = partition[1]
                offset = offset + 8
                #print array.array('B',data)
                get_messageset_data(data,offset,output,kafka_data_size)
		return
def get_producer_data(data,topic,output):
	## apikey + apiversion + correlationid + client
	offset = 14
	if offset > len(data):
		return
	try:
		client = unpack('>IHHIH',data[0:offset])
		## Producer ApiKey
		if client[1] == 0:
			#print array.array('B',data)
			output['DataLen'] = client[0]
                        output['ApiKey'] = client[1]
                        output['ApiVersion'] = client[2]
                        output['CorrelationId'] = client[3]
			client_len = client[4]
			output['Client'] = data[offset:offset+client_len]
			offset = offset + client_len
			## if new protocol version
			if client[2] == 3:
				offset = offset + 2
			client_attribute = unpack('>HI',data[offset:offset+6])
			output['RequiredAcks'] = client_attribute[0]
                        output['Timeout'] = client_attribute[1]
			offset = offset + 6
			get_topic_data(data,offset,output,topic,client[0])
			return
	except Exception as e:
		kafka_cluster=['']
		if output['SourceIP'] not in kafka_cluster:
			print e
			print output.items()
		print "============= Request End =================="
		return

def get_replica_fetcher_data(data,topic):
	#todo
	pass

def unpack_packet(port,topic,source,kafka_cluster):
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
	except socket.error , msg:
        	print 'Socket create failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        	sys.exit()
    	dataBuffer = bytes()
	tcpWorker = {}
	while True:
		#print "list keys:"
		tcpWorker.keys()
	    	packet = s.recvfrom(65565)
	    	#packet string from tuple
	    	packet = packet[0]
	    	#ip header
	    	ip_header = packet[0:20]
	    	iph = unpack('!BBHHHBBH4s4s' , ip_header)
	        #tcp header
	        version_ihl = iph[0]
	        version = version_ihl >> 4
	        ihl = version_ihl & 0xF
	        iph_length=ihl * 4
	        tcp_header = packet[iph_length:iph_length+20]
	        tcph = unpack('!HHLLBBHHH' , tcp_header)
	    	ttl = iph[5]
	    	protocol = iph[6]
	    	s_addr = socket.inet_ntoa(iph[8]);
	    	d_addr = socket.inet_ntoa(iph[9]);
	        #tcp header
	    	tcp_header = packet[iph_length:iph_length+20]
	    	tcph = unpack('!HHLLBBHHH' , tcp_header)
	    	s_port = tcph[0]
	    	d_port = tcph[1]
	    	sequence = tcph[2]
		#print "tcp sequece: "+ str(sequence)
	    	acknowledgement = tcph[3]
	    	doff_reserved = tcph[4]
	    	tcph_length = doff_reserved >> 4
	        #data
	        h_size = iph_length + tcph_length * 4
	        data_size = len(packet) - h_size
	        data = packet[h_size:]

		#get data by topic&source
		output={'SourceIP':'','SourcePort':'','DestIP':'','DestPort':'','DataLen':-1,'ApiKey':-1,'ApiVersion':-1,'CorrelationId':-1,'Client':'','RequiredAcks':-1,'Timeout':-1,'TopicName':'','PartitionCount':-1,'TopicCount':-1,'Partition':-1,'MessageSetSize':-1,'Offset':-1,'MessageSize':-1,'Magic':-1,'Attribute':-1,'Timestamp':-1,'Key':'','Value':'','Crc':''}
		output['SourceIP'] = s_addr
		output['SourcePort'] = s_port
		output['DestIP'] = d_addr
		output['DestPort'] = d_port
		if s_addr in kafka_cluster:
			continue
		key = s_addr + "," + str(s_port) + "," +d_addr + "," + str(d_port)
		if key not in tcpWorker:
			tcpWorker[key] = data
		else:
			tcpWorker[key] = tcpWorker[key] + data
		while True:
			if len(tcpWorker[key]) < 14:
				print "data packet is too short to analyze"
				break
			try:
				client = unpack('>IHHIH',tcpWorker[key][0:14])
			except Exception as e:
				print "not format"
				break
			kafka_data_size = client[0]
			if len(tcpWorker[key]) < 4 + kafka_data_size:
				print "split data packet"
				break
			if source == '0.0.0.0':
				get_producer_data(tcpWorker[key],topic,output)
				client = unpack('>IHHIH',tcpWorker[key][0:14])
				kafka_data_size = client[0]
				tcpWorker[key] = tcpWorker[key][4+kafka_data_size:]
			elif source == s_addr:
				get_producer_data(tcpWorker[key],topic,output)
				client = unpack('>IHHIH',tcpWorker[key][0:14])
                        	kafka_data_size = client[0]
                        	tcpWorker[key] = tcpWorker[key][4+kafka_data_size:]
			else:
				break
		if len(tcpWorker[key]) == 0:
			tcpWorker.pop(key, None)
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
        	if opt == "-h" :
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
	print "topic:", topic
	print "source:", source
	print "port:", port
	## kafka broker ip addresses
	kafka_cluster=['','','']
        unpack_packet(port,topic,source,kafka_cluster)
if __name__ == "__main__":
    	main()
