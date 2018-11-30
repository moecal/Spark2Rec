
from elasticsearch import Elasticsearch
import datetime

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
        query = self.conn.update_by_query(index=index, doc_type=type, body=body,request_timeout=60)
        print(query)


if __name__ == '__main__':
    # host = 'localhost:9200'
    host = '120.27.241.54:9200'
    esAction = ElasticSearchUtil(host)
    print (esAction.check())
    print
    _index = 'demo11'
    _type = 'test'
    aa="测试测试"
    doc={
            "script": {
                "source": "ctx._source.Tag_like=params.VendorColor",
                "params": {
                    "VendorColor": aa
                },
                "lang": "painless"

            },
            "query": {
                # "match_all":{}

            "term":{

                "Uid":"58"
                # "_id":"eq_w4mYBV1jihOLov_Gh"

            }


            #     "multi_match":{
            #         "query":"jack",
            #         "fields":["name"],
            #         "fuzziness": "AUTO"
            #     }
            #     "wildcard": {
            #         "name": "jack*"
            #     }

            }

    }
    print(datetime.datetime.now())
    print(esAction.test(_index,_type,doc))
    print(datetime.datetime.now())

