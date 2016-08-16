import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-o","--host",help="host|ip:[port]",default="127.0.0.1:9092")
parser.add_argument("-t","--topic",help="message topic",default="test_topic")
args = parser.parse_args()
from kafka import KafkaConsumer

#host = '127.0.0.1:9092'
#topic = 'test_topic'
host = args.host
topic = args.topic
consumer = KafkaConsumer(bootstrap_servers=host\
                         ,auto_offset_reset='earliest')
consumer.subscribe([topic])
print("subscribe host[%s] topic[%s] :" % (host,topic))
for msg in consumer:
    print(msg)
