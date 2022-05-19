from kafka import KafkaConsumer

brokers=["localhost:9092"]
topicName = "s3messages"

consumer = KafkaConsumer(topicName,group_id= "one_group",
bootstrap_servers=brokers,auto_offset_reset='earliest')

#for message in consumer:
    #print('Topic: %s:Partion: %d Message:%s' %(message.topic,message.partition,message.value))
from producer import messageSender 
filename='filedetails.txt'

with open(filename,'w') as f:
    for message in consumer:
        print('Topic: %s:Partion: %d Message:%s' %(message.topic,message.partition,message.value))
        f.write('Topic: %s:Partion: %d Message:%s' %(message.topic,message.partition,message.value))
        
    f.close()

while True:
    pass 