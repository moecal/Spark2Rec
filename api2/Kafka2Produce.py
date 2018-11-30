from kafka import KafkaProducer
import time
import json


def main():
    ##生产模块
    producer = KafkaProducer(bootstrap_servers=['120.27.241.54:9092','120.27.243.224:9092','114.55.226.34:9092'])
    # producer = KafkaProducer(bootstrap_servers=['master:9092','slave1:9092','slave2:9092'])
    # producer = KafkaProducer(bootstrap_servers=['master:9092','slave1:9092','slave2:9092'])
    # with open('/Users/ganlin/Dev_Doc/produce.txt', 'r') as f:
    #     for line in f.readlines():
    #         time.sleep(1)
    #         producer.send("test1", line.encode('utf-8'))
    #         print (line)
    #         producer.flush()

    # monitor1
    # ww={"stylistID":1058539,"boxID":170343,"clickTime":"2018-11-02 13:48:31","type":0}
    # ww={"stylistID":1058477,"boxID":"170298","clickTime":"2018-10-29 15:56:53","prodList":[30633,30638,30623,30628,30595,32074],"page":"1","prodNum":5441,"conditions":{"orderid":"20181029278540","page":"1","cate1":"","cate2":"","cate3":"","brand":"","name":""},"taskStatus":12}
    # ww={"stylistID":12213,"boxID":"170271","clickTime":"2018-10-29 22:40:13","prodID":11016,"page":3,"pageSort":33,"sideStatus":1,"conditions":"\"{\"orderid\":\"20181022278510\",\"page\":3,\"cate1\":null,\"cate2\":null,\"cate3\":null,\"brand\":null,\"colour\":[],\"shape\":[],\"thinkness\":[],\"price\":[],\"name\":null}\"","taskStatus":2,"type":0}
    # monitor2
    # ww={"stylistID":11144,"boxID":"170295","clickTime":"2018-11-02 10:44:31","conditions":"\"{\"purchased\":{\"cate\":[\"\\u9774\\u5b50\"],\"style\":[],\"page\":1},\"returned\":{\"cate\":[],\"style\":[],\"feedback\":null,\"page\":1},\"recordType\":1,\"pageSize\":20}\"","taskStatus":1}
    # monitor3
    # ww={"stylistID":1058539,"boxID":170294,"clickTime":"2018-11-02 13:55:16","page":1,"pageSort":0,"conditions":"\"{\"BoxID\":null,\"UserID\":null,\"Name\":null,\"TaskStatus\":null,\"StylistID\":null}\""}
    # monitor4
    # ww={"stylistID":1058539,"boxID":"170343","clickTime":"2018-11-02 13:52:11","prodList":[30203,28713,28722,29926,20969,3178,23324,14460,29330,36841,13312,3177,36767],"page":"1","prodNum":8173,"conditions":"\"{\"orderid\":\"20181101278600\",\"page\":\"1\",\"cate1\":null,\"cate2\":null,\"cate3\":null,\"brand\":null,\"name\":null}\"","taskStatus":12}
    # monitor5
    # ww={"stylistID":1058539,"boxID":170343,"clickTime":"2018-11-02 14:07:37","prodID":36806,"page":4,"pageSort":21,"sideStatus":0,"conditions":"\"{\"orderid\":\"20181101278600\",\"page\":4,\"cate1\":null,\"cate2\":null,\"cate3\":130,\"brand\":null,\"colour\":[],\"shape\":[],\"thinkness\":[],\"price\":[],\"name\":null}\"","taskStatus":12,"type":0}
    # monitor6
    # ww={"stylistID":1058539,"boxID":170343,"clickTime":"2018-11-06 14:10:35","prodID":32565,"taskStatus":12}

    # ww={"stylistID":4343,"boxID":"170248","clickTime":"2018-10-29 19:52:15","prodID":32182,"taskStatus":2}
    # ww=json.dumps(ww)
    # producer.send(topic='test1',value="68".encode('utf-8'),key="1".encode('utf-8'),partition=2)
    # producer.send(topic='test1',value="6".encode('utf-8'),key="3".encode('utf-8'),partition=0)
    # producer.send('test1',"68".encode('utf-8'))
    # producer.send('test1',"58".encode('utf-8'))
    # producer.send('test1', str("68"))
    # producer.send('test1', str("58"))
    producer.send(topic='test12',value="68045".encode('utf-8'),key="1".encode('utf-8'),partition=1)
    producer.send(topic='test12',value="68019".encode('utf-8'),key="2".encode('utf-8'),partition=2)
    producer.send(topic='test12',value="67959".encode('utf-8'),key="3".encode('utf-8'),partition=0)
    producer.send(topic='test12', value="67972".encode('utf-8'), key="1".encode('utf-8'), partition=3)
    producer.send(topic='test12', value="68067".encode('utf-8'), key="2".encode('utf-8'), partition=4)
    producer.send(topic='test12', value="68195".encode('utf-8'), key="3".encode('utf-8'), partition=5)
    producer.send(topic='test12', value="68190".encode('utf-8'), key="1".encode('utf-8'), partition=6)
    producer.send(topic='test12', value="68114".encode('utf-8'), key="2".encode('utf-8'), partition=7)
    producer.send(topic='test12', value="68341".encode('utf-8'), key="3".encode('utf-8'), partition=8)
    producer.send(topic='test12', value="68349".encode('utf-8'), key="1".encode('utf-8'), partition=9)

    producer.send(topic='test12', value="68506".encode('utf-8'), key="1".encode('utf-8'), partition=1)
    producer.send(topic='test12', value="68221".encode('utf-8'), key="2".encode('utf-8'), partition=2)
    producer.send(topic='test12', value="68287".encode('utf-8'), key="3".encode('utf-8'), partition=0)
    producer.send(topic='test12', value="68558".encode('utf-8'), key="1".encode('utf-8'), partition=3)
    producer.send(topic='test12', value="68601".encode('utf-8'), key="2".encode('utf-8'), partition=4)
    producer.send(topic='test12', value="68690".encode('utf-8'), key="3".encode('utf-8'), partition=5)
    producer.send(topic='test12', value="68741".encode('utf-8'), key="1".encode('utf-8'), partition=6)
    producer.send(topic='test12', value="68797".encode('utf-8'), key="2".encode('utf-8'), partition=7)
    producer.send(topic='test12', value="68844".encode('utf-8'), key="3".encode('utf-8'), partition=8)
    producer.send(topic='test12', value="68933".encode('utf-8'), key="1".encode('utf-8'), partition=9)

    producer.send(topic='test12', value="68045".encode('utf-8'), key="1".encode('utf-8'), partition=1)
    producer.send(topic='test12', value="68019".encode('utf-8'), key="2".encode('utf-8'), partition=2)
    producer.send(topic='test12', value="67959".encode('utf-8'), key="3".encode('utf-8'), partition=0)
    producer.send(topic='test12', value="67972".encode('utf-8'), key="1".encode('utf-8'), partition=3)
    producer.send(topic='test12', value="68067".encode('utf-8'), key="2".encode('utf-8'), partition=4)
    producer.send(topic='test12', value="68195".encode('utf-8'), key="3".encode('utf-8'), partition=5)
    producer.send(topic='test12', value="68190".encode('utf-8'), key="1".encode('utf-8'), partition=6)
    producer.send(topic='test12', value="68114".encode('utf-8'), key="2".encode('utf-8'), partition=7)
    producer.send(topic='test12', value="68341".encode('utf-8'), key="3".encode('utf-8'), partition=8)
    producer.send(topic='test12', value="68349".encode('utf-8'), key="1".encode('utf-8'), partition=9)

    producer.send(topic='test12', value="68506".encode('utf-8'), key="1".encode('utf-8'), partition=1)
    producer.send(topic='test12', value="68221".encode('utf-8'), key="2".encode('utf-8'), partition=2)
    producer.send(topic='test12', value="68287".encode('utf-8'), key="3".encode('utf-8'), partition=0)
    producer.send(topic='test12', value="68558".encode('utf-8'), key="1".encode('utf-8'), partition=3)
    producer.send(topic='test12', value="68601".encode('utf-8'), key="2".encode('utf-8'), partition=4)
    producer.send(topic='test12', value="68690".encode('utf-8'), key="3".encode('utf-8'), partition=5)
    producer.send(topic='test12', value="68741".encode('utf-8'), key="1".encode('utf-8'), partition=6)
    producer.send(topic='test12', value="68797".encode('utf-8'), key="2".encode('utf-8'), partition=7)
    producer.send(topic='test12', value="68844".encode('utf-8'), key="3".encode('utf-8'), partition=8)
    producer.send(topic='test12', value="68933".encode('utf-8'), key="1".encode('utf-8'), partition=9)






    # =================

    producer.send(topic='test12', value="68045".encode('utf-8'), key="1".encode('utf-8'), partition=1)
    producer.send(topic='test12', value="68019".encode('utf-8'), key="2".encode('utf-8'), partition=2)
    producer.send(topic='test12', value="67959".encode('utf-8'), key="3".encode('utf-8'), partition=0)
    producer.send(topic='test12', value="67972".encode('utf-8'), key="1".encode('utf-8'), partition=3)
    producer.send(topic='test12', value="68067".encode('utf-8'), key="2".encode('utf-8'), partition=4)
    producer.send(topic='test12', value="68195".encode('utf-8'), key="3".encode('utf-8'), partition=5)
    producer.send(topic='test12', value="68190".encode('utf-8'), key="1".encode('utf-8'), partition=6)
    producer.send(topic='test12', value="68114".encode('utf-8'), key="2".encode('utf-8'), partition=7)
    producer.send(topic='test12', value="68341".encode('utf-8'), key="3".encode('utf-8'), partition=8)
    producer.send(topic='test12', value="68349".encode('utf-8'), key="1".encode('utf-8'), partition=9)

    producer.send(topic='test12', value="68506".encode('utf-8'), key="1".encode('utf-8'), partition=1)
    producer.send(topic='test12', value="68221".encode('utf-8'), key="2".encode('utf-8'), partition=2)
    producer.send(topic='test12', value="68287".encode('utf-8'), key="3".encode('utf-8'), partition=0)
    producer.send(topic='test12', value="68558".encode('utf-8'), key="1".encode('utf-8'), partition=3)
    producer.send(topic='test12', value="68601".encode('utf-8'), key="2".encode('utf-8'), partition=4)
    producer.send(topic='test12', value="68690".encode('utf-8'), key="3".encode('utf-8'), partition=5)
    producer.send(topic='test12', value="68741".encode('utf-8'), key="1".encode('utf-8'), partition=6)
    producer.send(topic='test12', value="68797".encode('utf-8'), key="2".encode('utf-8'), partition=7)
    producer.send(topic='test12', value="68844".encode('utf-8'), key="3".encode('utf-8'), partition=8)
    producer.send(topic='test12', value="68933".encode('utf-8'), key="1".encode('utf-8'), partition=9)

    producer.send(topic='test12', value="68045".encode('utf-8'), key="1".encode('utf-8'), partition=1)
    producer.send(topic='test12', value="68019".encode('utf-8'), key="2".encode('utf-8'), partition=2)
    producer.send(topic='test12', value="67959".encode('utf-8'), key="3".encode('utf-8'), partition=0)
    producer.send(topic='test12', value="67972".encode('utf-8'), key="1".encode('utf-8'), partition=3)
    producer.send(topic='test12', value="68067".encode('utf-8'), key="2".encode('utf-8'), partition=4)
    producer.send(topic='test12', value="68195".encode('utf-8'), key="3".encode('utf-8'), partition=5)
    producer.send(topic='test12', value="68190".encode('utf-8'), key="1".encode('utf-8'), partition=6)
    producer.send(topic='test12', value="68114".encode('utf-8'), key="2".encode('utf-8'), partition=7)
    producer.send(topic='test12', value="68341".encode('utf-8'), key="3".encode('utf-8'), partition=8)
    producer.send(topic='test12', value="68349".encode('utf-8'), key="1".encode('utf-8'), partition=9)

    producer.send(topic='test12', value="68506".encode('utf-8'), key="1".encode('utf-8'), partition=1)
    producer.send(topic='test12', value="68221".encode('utf-8'), key="2".encode('utf-8'), partition=2)
    producer.send(topic='test12', value="68287".encode('utf-8'), key="3".encode('utf-8'), partition=0)
    producer.send(topic='test12', value="68558".encode('utf-8'), key="1".encode('utf-8'), partition=3)
    producer.send(topic='test12', value="68601".encode('utf-8'), key="2".encode('utf-8'), partition=4)
    producer.send(topic='test12', value="68690".encode('utf-8'), key="3".encode('utf-8'), partition=5)
    producer.send(topic='test12', value="68741".encode('utf-8'), key="1".encode('utf-8'), partition=6)
    producer.send(topic='test12', value="68797".encode('utf-8'), key="2".encode('utf-8'), partition=7)
    producer.send(topic='test12', value="68844".encode('utf-8'), key="3".encode('utf-8'), partition=8)
    producer.send(topic='test12', value="68933".encode('utf-8'), key="1".encode('utf-8'), partition=9)


    # producer.send(topic='test12',value="58".encode('utf-8'),key="4".encode('utf-8'),partition=4)
    # producer.send(topic='test12',value="68".encode('utf-8'),key="5".encode('utf-8'),partition=5)
    # producer.send(topic='test12',value="108".encode('utf-8'),key="6".encode('utf-8'),partitionrm -rf=6)
    # producer.send(topic='test12',value="58".encode('utf-8'),key="7".encode('utf-8'),partition=7)
    # producer.send(topic='test12',value="68".encode('utf-8'),key="8".encode('utf-8'),partition=8)
    # producer.send(topic='test12',value="108".encode('utf-8'),key="9".encode('utf-8'),partition=9)
    # producer.send(topic='test12',value="58".encode('utf-8'),key="0".encode('utf-8'),partition=0)
    producer.flush()


if __name__ == '__main__':
    main()