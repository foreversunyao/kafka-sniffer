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


def get_producer_data(data,topic,output):
	#print array.array('B',data)
	offset = 14
	api_client_part = unpack('>IHHIH',data[0:14])
	## Producer ApiKey
	if api_client_part[1] == 0:
		try:
			#print data
#			print array.array('B',data)
			output['DataLen'] = api_client_part[0]
                        output['ApiKey'] = api_client_part[1]
                        output['ApiVersion'] = api_client_part[2]
                        output['CorrelationId'] = api_client_part[3]
			client_len = api_client_part[-1]
			output['Client'] = data[offset:offset+client_len]
			if api_client_part[2] == 3:
				offset = offset + 2
			else:
				offset = offset
			client_len = api_client_part[-1]
			offset = offset + client_len
                        conn_part = unpack('>HIIH',data[offset:offset+12])
			output['RequiredAcks'] = conn_part[0]
                        output['Timeout'] = conn_part[1]
			output['TopicCount'] = conn_part[2]
			offset = offset + 12
			topic_name = data[offset:offset+conn_part[3]]
			output['TopicName'] = topic_name
			offset = offset + conn_part[3]
        		if topic_name == topic:
        			partition_part = unpack('>II',data[offset:offset+8])
                                output['PartitionCount'] = partition_part[0]
				partition_loop = partition_part[0]
                                output['Partition'] = partition_part[1]
				offset = offset + 8
				messageset_part = unpack('>I',data[offset:offset+4])
                                output['MessageSetSize'] = messageset_part[0]
				offset = offset + 4
				while offset < len(data):
					##print array.array('B',data[offset:offset+30])
					message_part = unpack('>QII??Q',data[offset:offset+26])
                                	output['Offset'] = message_part[0]
                                	output['MessageSize'] = message_part[1]
                                	output['Crc'] = message_part[2]
                                	output['Magic'] = int(message_part[3])
                                	output['Attribute'] = int(message_part[4])
					if int(message_part[3]) == 1:
                                		output['Timestamp'] = message_part[5]
						offset = offset + 26
					## kafka version < 0.10
					elif int(message_part[3]) == 0 :
						offset = offset + 18
					key_len = unpack('>I',data[offset:offset+4])
                                                ## key is None
                                        if key_len[0] == 4294967295:
                                                output['Key'] = None
                                                offset = offset + 4
                                        else:
                                                offset = offset + 4
                                                output['Key'] = data[offset:offset+key_len[0]]
                                                offset = offset + key_len[0]
					value_len = unpack('>I',data[offset:offset+4])
					offset = offset + 4
					output['Value'] = data[offset:offset+value_len[0]]
					offset = offset + value_len[0]
					partition_loop = partition_loop - 1
					print "partition_loop: "+str(partition_loop)
					if partition_loop > 0 and offset + 8 < len(data):
						partition_others_part = unpack('>II',data[offset:offset+8])
						output['Partition'] = partition_others_part[0]
						output['MessageSetSize'] = partition_others_part[1]
						offset = offset + 8
					print output.items()
				print "=============One Request=================="
		except Exception as e:
			print e
			##print array.array('B',data)
			pass

def get_replica_fetcher_data(data,topic):
	#todo
	pass

def unpack_packet(port,topic,source,kafka_cluster):
    try:
	s = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    except socket.error , msg:
        print 'Socket create failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()

    while True:
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
    	source_port = tcph[0]
    	dest_port = tcph[1]
    	sequence = tcph[2]
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
	output['SourcePort'] = source_port
	output['DestIP'] = d_addr
	output['DestPort'] = dest_port
	if len(data)>12 and source == output['SourceIP'] and output['SourceIP'] not in kafka_cluster and topic in data:
		get_producer_data(data,topic,output)
	elif len(data)>12 and source == "0.0.0.0" and output['SourceIP'] not in kafka_cluster and topic in data:
		get_producer_data(data,topic,output)
	else:
		pass
		#print 'source address or topic is not existing'

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
