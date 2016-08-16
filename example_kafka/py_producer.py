import time
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-o","--host",help="host|ip:[port]",default="127.0.0.1:9092")
parser.add_argument("-t","--topic",help="message topic",default="test_topic1")
args = parser.parse_args()
from pykafka import KafkaClient

#host = '127.0.0.1:9092'
#topic = 'test_topic'
_host = args.host
_topic = args.topic
client = KafkaClient(hosts=_host)
topic = client.topics[_topic]

producer = topic.get_producer()
print("host[%s] topic[%s]  massage sending ..." % (_host,_topic))
for i in range(10):
    producer.produce('mssage_%s' % i)
    time.sleep(5)
print("massage sended")


