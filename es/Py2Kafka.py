from kafka import KafkaClient
from kafka.producer import SimpleProducer



def send_data_2_kafka(datas):
    '''
        向kafka解析队列发送数据
    '''
    KAFKABROKER=['120.27.241.54','120.27.243.224','114.55.226.34']
    PARTNUM=5
    TOPICNAME='test01'
    # TOPICNAME='logtest3'
    client = KafkaClient(hosts=KAFKABROKER, timeout=30)

    producer = SimpleProducer(client, async=False)

    curcount = len(datas ) //PARTNUM
    print(curcount)
    print(len(datas))
    for i in range(0, PARTNUM):
        start = i* curcount
        if i != PARTNUM - 1:
            end = (i + 1) * curcount
            curdata = datas[start:end]
            producer.send_messages(TOPICNAME, *curdata)
        else:
            curdata = datas[start:]
            producer.send_messages(TOPICNAME, *curdata)
        print(datetime.datetime.now())

    producer.stop()
    client.close()


if __name__ ==   '__main__':
    import datetime
    import  json
    print(datetime.datetime.now())
    data2=[]
    for i in range(3000):
        # jsda={'name': 'jackaaa',
        #   'age': i,
        #   'sex': 'female',
        #   'address': u'西安'}
        jsda={
                    'name1': 'jackaaa',
                    'name2': 'jackaaa',
                    'name3': 'jackaaa',
                    'name4': 'jackaaa',
                    'name5': 'jackaaa',
                    'name6': 'jackaaa',
                    'name7': 'jackaaa',
                    'name8': 'jackaaa',
                    'name9': 'jackaaa',
                    'name10': 'jackaaa',
                    'name11': 'jackaaa',
                    'name12': 'jackaaa',
                    'name13': 'jackaaa',
                    'name14': 'jackaaa',
                    'name15': 'jackaaa',
                    'name16': 'jackaaa',
                    'name17': 'jackaaa',
                    'name18': 'jackaaa',
                    'name19': 'jackaaa',
                    'name20': 'jackaaa',
                    'name21': 'jackaaa',
                    'name22': 'jackaaa',
                    'name23': 'jackaaa',
                    'name24': 'jackaaa',
                    'name25': 'jackaaa',
                    'name26': 'jackaaa',
                    'name27': 'jackaaa',
                    'name28': 'jackaaa',
                    'age': i,
                    'sex': 'female',
                    'address': u'西安'}

        # print(json.dumps(json.dumps(jsda)))
        # print(str(json.dumps(jsda)))
        # data3=json.dumps(json.dumps(jsda)).encode('utf-8')
        data3=json.dumps(jsda).encode('utf-8')

        data2.append(data3)

    # for i in range(10000):
    #    data2.append(('aa%s'%(i)).encode('utf-8'))


    data=[
        'bbccdd1'.encode('utf-8'),
        'bbccdd2'.encode('utf-8')
        # {'aa3','bbccdd3'},
        # {'aa4','bbccdd4'},
        # {'aa5','bbccdd5'},
        # {'aa5','bbccdd6'}

    ]


    print(datetime.datetime.now(),'开始')
    send_data_2_kafka(data2)
    # send_data_2_kafka(data)
    print(datetime.datetime.now(),'结束')
