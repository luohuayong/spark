import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-k","--kafka",help="kafka host|ip:[port]",default="127.0.0.1:9092")
parser.add_argument("-z","--zookeeper",help="zookeeper host|ip:[port]",default="127.0.0.1:2181")
parser.add_argument("-t","--topic",help="message topic",default="test_topic1")
args = parser.parse_args()
from pykafka import KafkaClient
_kf = args.kafka
_zp = args.zookeeper
_topic = args.topic
client = KafkaClient(hosts=_kf)
topic = client.topics[_topic]
consumer = topic.get_simple_consumer()
#consumer = topic.get_balanced_consumer(
#    consumer_group = 'testgroup1',
#    auto_commit_enable = True,
#    zookeeper_connect = _zp
#)

print("subscribe kafka[%s] zookeeper[%s] topic[%s] :" % (_kf,_zp,_topic))
for message in consumer:
    if message is not None:
        print message.offset,message.value

