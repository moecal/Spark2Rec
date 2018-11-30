from elasticsearch import Elasticsearch
from elasticsearch import helpers
import datetime

if __name__ == '__main__':
    es=Elasticsearch(hosts=['120.27.241.54:9200'])
    query = {'query': {'match': {'Uid': 58}}}
    print(datetime.datetime.now())
    rs=helpers.scan(
        client=es,
        query=query,
        scroll='5m',
        index='demo11',
        doc_type='test',
        size=100


    )
    # print(rs)
    # print(len(rs))
    ls=[]
    cout=0
    print(datetime.datetime.now())

    for i in rs:
        ls.append(i['_source'])
        # cout+=1
        # print(cout)

    print(ls[0])
    print(datetime.datetime.now())


    # doc=[i for i in rs]
    # print(len(doc))