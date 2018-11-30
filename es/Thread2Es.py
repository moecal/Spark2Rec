import threading
from elasticsearch import Elasticsearch
import  datetime


def bulk2es():
    _index = 'logtest21'
    _type = 'doc'
    doc = []
    for i in range(1000):
        doc.append({"index": {}})
        doc.append(

            {
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
        )
    print(datetime.datetime.now())

    Elasticsearch(['120.27.241.54', '120.27.243.224', '114.55.226.34']).bulk(index=_index, doc_type=_type, body=doc,
                                                                                 request_timeout=100)
    print(datetime.datetime.now())



if __name__ == '__main__':
    print(datetime.datetime.now())
    threads=[]
    for i in range(10):
        t=threading.Thread(target=bulk2es)
        threads.append(t)
    for i in threads:
        i.start()
    for i in threads:
        i.join()
    print(datetime.datetime.now(),"结束")
