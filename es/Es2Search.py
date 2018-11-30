
from elasticsearch import Elasticsearch

class ElasticSearchUtil:
    def __init__(self, host):
        self.host = host
        self.conn = Elasticsearch([self.host])

    def __del__(self):
        self.close()

    def check(self):
        '''
        输出当前系统的ES信息
        :return:
        '''
        return self.conn.info()

    def searchDoc(self, index=None, type=None, body=None):
        '''
        查找index下所有符合条件的数据
        :param index:
        :param type:
        :param body: 筛选语句,符合DSL语法格式
        :return:
        '''
        return self.conn.search(index=index, doc_type=type, body=body,from_=0,size=5)
        # return self.conn.search(index=index, doc_type=type, body=body)



    def close(self):
     if self.conn is not None:
        try:
            self.conn.close()
        except Exception as e:
            pass
        finally:
            self.conn = None

    def test(self, index, type, body):
        query = self.conn.update_by_query(index=index, doc_type=type, body=body)
        print(query)


if __name__ == '__main__':
    # host = 'localhost:9200'
    host = '120.27.241.54:9200'
    esAction = ElasticSearchUtil(host)
    print (esAction.check())
    _index = 'demo38'
    _type = 'test'
    # query={
    #     'query':{
    #         'match_all':{}
    #     }
    # }

    # query={
    #     'query':{
    #         'term':{
    #             'cate2':12,
    #             'PreSizeID':27597
    #         }
    #     }
    # }

    query={
        'query':{
            'bool':{
                'must':[
                    # {'term':{'sex':'female'}},
                    # {'match':{'address':'西安你好'}},
                    {'term':{'Cate1':'1'}},


                ]

            }

        },
        'sort': {
            'Score': {
                # 'order': 'asc'
                'order': 'desc'
            }
        }
    }
    print(esAction.searchDoc(_index,_type,query)['hits']['hits'])



