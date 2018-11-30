#!/usr/bin/python
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk



class ElasticObj:
    def __init__(self,index_name,index_type,ip="120.27.241.54"):
        '''

        :param index_name: 索引名称
        :param index_type: 索引类型
        :param ip:
        '''
        self.index_name=index_name
        self.index_type=index_type
        # 无用户名密码状态
        self.es = Elasticsearch([ip])
        # 用户名密码状态
        # self.es = Elasticsearch([ip], http_auth=('elastic', 'password'), port=9200)


    def create_index(self,index_name="ott",index_type="ott_type"):
        '''
        创建索引,创建索引名称为ott，类型为ott_type的索引
        :param index_name:
        :param index_type:
        :return:
        '''
        _index_mappings = {
            "mappings": {
                self.index_type: {
                    "properties": {
                        "title": {
                            "type": "text",
                            "index": True,
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "date": {
                            "type": "text",
                            "index": True
                        },
                        "keyword": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "source": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "link": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                    }
                }

            }
        }

        if self.es.indices.exists(index=self.index_name) is not True:
            res=self.es.indices.create(index=self.index_name,body=_index_mappings)
            print(res)



    def IndexData(self):
        es=self.es
