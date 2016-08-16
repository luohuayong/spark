import time
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-o","--host",help="host|ip:[port]",default="127.0.0.1:9092")
parser.add_argument("-t","--topic",help="message topic",default="test_topic")
args = parser.parse_args()
from kafka import KafkaProducer

#host = '127.0.0.1:9092'
#topic = 'test_topic'
host = args.host
topic = args.topic
producer = KafkaProducer(bootstrap_servers=host)
print("host[%s] topic[%s]  massage sending ..." % (host,topic))
for i in range(100):
    producer.send(topic,'mssage_%s' % i)
    time.sleep(1)
print("massage sended")


