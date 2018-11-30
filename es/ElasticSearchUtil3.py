
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

    def insertDocument(self, index, type, body, id=None):
        '''
        插入一条数据body到指定的index、指定的type下;可指定Id,若不指定,ES会自动生成
        :param index: 待插入的index值
        :param type: 待插入的type值
        :param body: 待插入的数据 -> dict型
        :param id: 自定义Id值
        :return:
        '''
        return self.conn.index(index=index, doc_type=type, body=body, id=id)

    def insertDataFrame(self, index, type, dataFrame):
        '''
        批量插入接口;
        bulk接口所要求的数据列表结构为:[{{optionType}: {Condition}}, {data}]
        其中optionType可为index、delete、update
        Condition可设置每条数据所对应的index值和type值
        data为具体要插入/更新的单条数据
        :param index: 默认插入的index值
        :param type: 默认插入的type值
        :param dataFrame: 待插入数据集
        :return:
        '''
        dataList = dataFrame.to_dict(orient='records')
        print(dataList)
        insertHeadInfoList = [{"index": {}} for i in range(len(dataList))]
        print(insertHeadInfoList)
        temp = [dict] * (len(dataList) * 2)
        temp[::2] = insertHeadInfoList
        temp[1::2] = dataList
        print(temp)
        try:
            return self.conn.bulk(index=index, doc_type=type, body=temp)
        except Exception as e:
            return str(e)

    def deleteDocById(self, index, type, id):
        '''
        删除指定index、type、id对应的数据
        :param index:
        :param type:
        :param id:
        :return:
        '''
        return self.conn.delete(index=index, doc_type=type, id=id)

    def deleteDocByQuery(self, index, query, type=None):
        '''
        删除idnex下符合条件query的所有数据
        :param index:
        :param query: 满足DSL语法格式
        :param type:
        :return:
        '''
        return self.conn.delete_by_query(index=index, body=query, doc_type=type)

    def deleteAllDocByIndex(self, index, type=None):
        '''
        删除指定index下的所有数据
        :param index:
        :return:
        '''
        try:
            query = {'query': {'match_all': {}}}
            return self.conn.delete_by_query(index=index, body=query, doc_type=type)
        except Exception as e:
            return str(e) + ' -> ' + index

    def searchDoc(self, index=None, type=None, body=None):
        '''
        查找index下所有符合条件的数据
        :param index:
        :param type:
        :param body: 筛选语句,符合DSL语法格式
        :return:
        '''
        return self.conn.search(index=index, doc_type=type, body=body)

    def getDocById(self, index, type, id):
        '''
        获取指定index、type、id对应的数据
        :param index:
        :param type:
        :param id:
        :return:
        '''
        return self.conn.get(index=index, doc_type=type, id=id)

    def updateDocById(self, index, type, id, body=None):
        '''
        更新指定index、type、id所对应的数据
        :param index:
        :param type:
        :param id:
        :param body: 待更新的值
        :return:
        '''
        return self.conn.update(index=index, doc_type=type, id=id, body=body)


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
    host = 'localhost:9200'
    # host = '120.27.243.224:9200'
    esAction = ElasticSearchUtil(host)
    print (esAction.check())
    print
    _index = 'test2'
    _type = 'doc'
    #     aa="安徽11"
    #     doc={
    #         "script": {
    #             "source": "ctx._source.VendorColor=params.VendorColor",
    #             "params": {
    #                 "VendorColor": "1234"
    #             },
    #             "lang": "painless"
    #
    #         },
    #         "query": {
    #             # "match_all":{}
    #
    #         "term":{
    #
    #             # "Uid":"58"
    #             "_id":"eq_w4mYBV1jihOLov_Gh"
    #
    #         }
    #
    #
    #         #     "multi_match":{
    #         #         "query":"jack",
    #         #         "fields":["name"],
    #         #         "fuzziness": "AUTO"
    #         #     }
    #         #     "wildcard": {
    #         #         "name": "jack*"
    #         #     }
    #
    #         }
    #
    # }
    #
    #     esAction.test(_index,_type,doc)

    # Index API
    import datetime
    print(datetime.datetime.now())

    # for i in range(1000):
    #
    #     body = {"name": 'lucy', 'sex': 'female', 'age': 10}
    #     esAction.insertDocument('demo21', 'test', body)
    #     # print (esAction.insertDocument('demo21', 'test', body))
    # print(datetime.datetime.now())


    # Search API
    # query = {'query': {'match_all': {}}}
    # print (esAction.searchDoc('demo', 'test', query)['hits']['hits'][0])

    # query = {'query': {'term': {'name': 'jackaaa'}}}
    # query = {'query': {'term': {'name': 'lucy'}}}
    # print (esAction.searchDoc(_index, _type, query))


    # query = {'query': {'range': {'age': {'gt': 11}}}}
    # query = {'query': {'range': {'age': {'lt': 11}}}}
    # query = {'query': {'match': {'age': 1000}}}
    # print (esAction.searchDoc('physique_city', 'physique', query))
    # print(esAction.searchDoc())

    # 批量插入接口
    # doc = [
    #     {"index": {}},
    #     {'name': 'jackaaa', 'age': 2000, 'sex': 'female', 'address': u'西安'},
    #     {"index": {}},
    #     {'name': 'jackbbb', 'age': 3000, 'sex': 'male', 'address': u'合肥'},
    #     {"index": {}},
    #     {'name': 'jackccc', 'age': 4000, 'sex': 'female', 'address': u'安徽'},
    #     {"index": {}},
    #     {'name': 'jackddd', 'age': 1000, 'sex': 'male', 'address': u'阜阳'},
    # ]

    doc=[]
    for i in range(6000):
        doc.append( {"index": {}})
        doc.append({'name': 'jackaaa', 'age': i, 'sex': 'female', 'address': u'西安'})
        # doc.append(
        #
        #     {
        #         'name1': 'jackaaa',
        #         'name2': 'jackaaa',
        #         'name3': 'jackaaa',
        #         'name4': 'jackaaa',
        #         'name5': 'jackaaa',
        #         'name6': 'jackaaa',
        #         'name7': 'jackaaa',
        #         'name8': 'jackaaa',
        #         'name9': 'jackaaa',
        #         'name10': 'jackaaa',
        #         'name11': 'jackaaa',
        #         'name12': 'jackaaa',
        #         'name13': 'jackaaa',
        #         'name14': 'jackaaa',
        #         'name15': 'jackaaa',
        #         'name16': 'jackaaa',
        #         'name17': 'jackaaa',
        #         'name18': 'jackaaa',
        #         'name19': 'jackaaa',
        #         'name20': 'jackaaa',
        #         'name21': 'jackaaa',
        #         'name22': 'jackaaa',
        #         'name23': 'jackaaa',
        #         'name24': 'jackaaa',
        #         'name25': 'jackaaa',
        #         'name26': 'jackaaa',
        #         'name27': 'jackaaa',
        #         'name28': 'jackaaa',
        #         'age': i,
        #         'sex': 'female',
        #         'address': u'西安'}
        # )

    # print(doc)

    # doc = [
    #     {"update": {'_id': '1a9t02YBV1jihOLok81n'}},
    #     {'doc': {'age': '12345'}ll}
    # ]
    #llll
    #
    #

    # import sys
    # print(sys.getsizeof(doc))

    # doc=doc[:1000:1]
    # print(len(doc))


    print(Elasticsearch([host]).bulk(index=_index, doc_type=_type, body=doc,request_timeout=100))
    print(datetime.datetime.now())


    # Get API
    # print (esAction.getDocById('demo', 'test', '0a9X02YBV1jihOLoXM0Y'))

    # Update API
    # body = {'script': "ctx._source.remove('age')"}#删除字段
    # body = {'script': "ctx._source.address = '合肥1'"}#增加字段
    # body = {"doc": {"name": 'jackaaa123'}}#修改部分字段
    # print (esAction.updateDocById('demo', 'test', '0a9X02YBV1jihOLoXM0Y', body))

    # Delete API
    # body = {"query": {"name": 'jackbbb', 'sex': 'male'}}
    # print esAction.deleteDocById('demo', 'test', 'grULY2ABJus46JkUMEH1')

    # Delete_By_Query API
    # query = {'query': {'match': {'sex': 'famale'}}}
    # query = {'query': {'range': {'age': {'lt': 11}}}}
    # print esAction.deleteDocByQuery(_index, query=query, type=_type)
    # print(esAction.deleteDocByQuery('physique_school', {'query': {'match_all': {}}}))
    # print(esAction.deleteAllDocByIndex('demo', 'test'))

    # _index = 'demo'
    # _type = 'test_df'
    # import pandas as pd
    # frame = pd.DataFrame({'name': ['tomaaa', 'tombbb', 'tomccc'],
    #                       'sex': ['male', 'famale', 'famale'],
    #                       'age': [3, 6, 9],
    #                       'address': [u'合肥', u'芜湖', u'安徽']})
    #
    # print (esAction.insertDataFrame(_index, _type, frame))

    # Index DataFrame
    # esAction.insertDataFrame(_index, _type, frame)
