
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk,parallel_bulk
import datetime
from collections import deque




def set_data(n):
    for i in range(n):
        yield {
        "_index": 'demo38',
        "_type": 'test',
        "_source": {
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
            'address': u'西安'
        }
    }


if __name__ == '__main__':
    print(datetime.datetime.now())
    # es=Elasticsearch(hosts=['120.27.241.54','120.27.243.224','114.55.226.34'],timeout=5000)
    es=Elasticsearch(hosts=['10.29.186.116'],timeout=5000)
    # for i in set_data(5):
    #     print(i)
    bulk(es,set_data(10000),chunk_size=5000)
    # bulk(es,set_data(3000))

    deque(parallel_bulk(es, set_data(3000)), maxlen=0)
    # deque(parallel_bulk(es, set_data(6000), thread_count=8, queue_size=8), maxlen=0)
    # print(list(set_data(6000))[:10])
    print(datetime.datetime.now())


