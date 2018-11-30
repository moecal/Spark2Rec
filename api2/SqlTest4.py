from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from api2.recommendProduct import recommendProduct
from pyspark.sql import SQLContext,HiveContext
from pyspark.sql.types import *

import time
import os
import json
import datetime
import pymysql
from api2.mysql import mysql
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk,parallel_bulk
from collections import deque

def agr():
    pass

def f(list):

    # es = Elasticsearch(['localhost'], timeout=6000)
    es = Elasticsearch(['120.27.241.54', '120.27.243.224', '114.55.226.34'], timeout=6000)
    # es = Elasticsearch(['120.27.241.54'], timeout=6000)
    ACTIONS=[]
    print("当前时间=====》", datetime.datetime.now())
    for line in list:
        action = {
            "_index": 'demo38',
            "_type": 'test',
            "_source": {
                "Uid": line['Uid'],
                "Pid": line['Pid'],
                "Score": line['Score'],
                "Tag_like": line['Tag_like'],
                "Tag_ray": line['Tag_ray'],
                "Brand": line['Brand'],
                "Cate1": line['Cate1'],
                "Cate2": line['cate2'],
                "Cate3": line['Cate3'],
                "CreatedDate": line['CreatedDate'],
                "Description": line['Description'],
                "Introduction": line['Introduction'],
                "LatestUPCTime": line['LatestUPCTime'],
                "Material": line['Material'],
                "MsrPrice": line['MsrPrice'],
                "NAME": line['NAME'],
                "Note": line['Note'],
                "Position": line['Position'],
                "PreSizeID": line['PreSizeID'],
                "Price": line['Price'],
                "PurchPrice": line['PurchPrice'],
                "SKCID": line['SKCID'],
                "SPUID": line['SPUID'],
                "SerialNumber": line['SerialNumber'],
                "type": line['Type'],
                "UpdatedDate": line['UpdatedDate'],
                "VendorColor": line['VendorColor'],
                "color": line['color'],
                "img_type": line['img_type'],
                "img_version": line['img_version'],
                "is_special": line['is_special'],
                "label": line['label'],
                "CreTime":line["CreTime"]
            }
        }
        ACTIONS.append(action)
        # 批量处理

    # if len(ACTIONS) == 10000:
    # print("当前时间中=====》", datetime.datetime.now())
    # bulk(es, ACTIONS, chunk_size=2500,thread_count=4, raise_on_error=False, stats_only=True)
    bulk(es, ACTIONS, raise_on_error=False, stats_only=True)


    # deque(parallel_bulk(es, ACTIONS, thread_count=8, queue_size=8),maxlen=0)
    # deque(parallel_bulk(es, ACTIONS,chunk_size=2000),maxlen=0)




    # print(len(ACTIONS)*10)
    # print("当前时间=====》",datetime.datetime.now())
    # bulk(es, ACTIONS, raise_on_error=False, stats_only=True)

    # doc = []
    # for i in range(len(ACTIONS)*10):
    #     doc.append({"index": {}})
    #     doc.append({
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
    #         'address': u'西安'})
    # host = '120.27.241.54:9200'
    #
    # _index = 'demo1'
    # _type = 'test'
    # print("当前时间=====》",datetime.datetime.now())

    # Elasticsearch([host]).bulk(index=_index, doc_type=_type, body=doc, request_timeout=100)

    print("当前时间结束=====》",datetime.datetime.now())
if __name__ == '__main__':
    # conf=SparkConf().setMaster('local[8]').setAppName('sqlTest')
    # sc = SparkContext(master='local[8]',appName='sqlTest3')
    sc = SparkContext(appName='sqlTest5')
    sqlContext= SQLContext(sc)
    product=recommendProduct.asynComputedUserRecommendProd(68)
    # product=recommendProduct.asynComputedUserRecommendProd(1000006)
    schema = StructType([StructField('Uid', IntegerType(), True), StructField('Pid', IntegerType(), True),StructField('Score', IntegerType(), True),StructField('Tag_like', StringType(), True),StructField('Tag_ray', StringType(), True)])
    prods=sc.parallelize(product)
    sqlContext.createDataFrame(prods,schema).createOrReplaceTempView('prods')
    print(datetime.datetime.now())
    sql1='''
       SELECT
  sku.id AS skuid ,
  sku.SPUID ,
  sku.SKCID , 
  tm1.AttributeStatus AS spu_status ,
  tm2.ContentStatus AS skc_status ,
  sku.SizeStatus AS sku_status ,
  sku.BachName AS NAME ,
  tm1.Cate1 ,
  tm1.cate2 ,
  tm1.Cate3 ,
  tm1.cate3name,
  tm1.Brand ,
  tm1.Description ,
  tm1.SerialNumber ,
  tm2.pcolour AS pcolor ,
  tm2.color AS color ,
  GROUP_CONCAT(
    tm1.typename ,
    '-' ,
    tm1. NAME SEPARATOR ' '
  ) label ,
  tm2.Material ,
  tm1.Introduction ,
  tm2.VendorColor ,
  sku.PreSizeID ,
  cast(sku.MsrPrice as signed) MsrPrice ,
  cast(sku.PurchPrice as signed) PurchPrice ,
  cast(sku.Price as signed)  Price,
  sku.Type ,
  sku.IsSpecial AS is_special ,
  tm2.Version AS img_version ,
  tm2.ImgType AS img_type ,
  psp.Position ,
  sku.Note ,
  sku.LatestUPCTime ,
  sku.CreatedDate ,
  sku.UpdatedDate,
  sku.Quantity
FROM
  product sku
LEFT JOIN(
  SELECT
    spu.id AS id1 ,
    spu.AttributeStatus AS AttributeStatus ,
    spu.Cate1 AS cate1 ,
    spu.cate2 AS cate2 ,
    spu.Cate3 AS cate3 ,
    prc.name as cate3name,
    spu.Brand AS brand ,
    spu.Description AS Description ,
    spu.SerialNumber AS SerialNumber ,
    AT .TypeName AS typename ,
    pa. NAME AS NAME ,
    spu.Introduction AS Introduction
  FROM
    spu
  LEFT JOIN spuattribute sa ON spu.id = sa.SPUID
  LEFT JOIN productattribute pa ON sa.AttributeID = pa.id
  LEFT JOIN attributetype AT ON pa.Type = AT .typeID
  LEFT JOIN productcate prc ON spu.cate3 = prc .id
) tm1 ON sku.SPUID = tm1.id1
LEFT JOIN(
  SELECT
    skc.id AS id2 ,
    skc.ContentStatus AS ContentStatus ,
    pc.ParentID AS pcolour ,
    skc.Color AS color ,
    skc.Material AS Material ,
    skc.VendorColor AS VendorColor ,
    skc.Version AS Version ,
    skc.ImgType AS ImgType
  FROM
    productcolor pc
  LEFT JOIN skc ON skc.Color = pc.id
) tm2 ON sku.skcid = tm2.id2
LEFT JOIN productskuposition psp ON sku.id = psp.ProdID
GROUP BY
  sku.id
    '''
    # prod_detail=mysql.get(sql)
    # prod_detail = sqlContext.read.format("jdbc").options(
    #     url="jdbc:mysql://rm-bp12n2u9o7432w52b.mysql.rds.aliyuncs.com:3306/operation",
    #     driver="com.mysql.jdbc.Driver",
    #     dbtable=sql,
    #     user="champzee",
    #     password="JiuhQI9HvegvEiCM").load()
    sql2 = '''

    SELECT
    	t1.id AS cate1 ,
    	t1. NAME AS cate1name ,
    	t2.id AS cate2 ,
    	t2.name as cate2name,
    	t3.id AS cate3,
    	t3.name as cate3name
    FROM
    	(
    		productcate AS t1
    		INNER JOIN productcate AS t2 ON t1.id = t2.parentid
    	)
    INNER JOIN productcate AS t3 ON t2.id = t3.parentid
    WHERE
    	t1.DELETEd = 0
    AND t2.deleted = 0
    AND t3.deleted = 0
    AND floor(t3.id / 100) > 0


        '''
    pd1=mysql.get(sql1)

    pd2=mysql.get(sql2)


    print(datetime.datetime.now())
    rdd1=sc.parallelize(pd1)
    rdd2=sc.parallelize(pd2)
    sqlContext.createDataFrame(rdd1).createOrReplaceTempView('pd')
    # sqll='select a.*,b.*, %s from prods a left join pd b on a.Pid=b.skuid'%("date_format(now(),'%y-%m-%d')")
    # res=sqlContext.sql('select * from prods left join pd on prods.Pid=pd.skuid ')
    res=sqlContext.sql('select a.*,b.*, now() as CreTime from prods a left join pd b on a.Pid=b.skuid')
    res.createOrReplaceTempView('tm1')
    sqlContext.createDataFrame(rdd2).createOrReplaceTempView('tm2')

    rss1=sqlContext.sql('select tm1.*,tm2.cate1name as cate1name,tm2.cate2name as cate2name from tm1 left join tm2 on tm1.cate1=tm2.cate1 and tm1.cate2=tm2.cate2 and tm1.cate3=tm2.cate3 where Quantity!=0 ')

    rss1.createOrReplaceTempView('tm3')
    rss2=sqlContext.sql('select uid, cate1, cate1name,cate2,cate2name,cate3,cate3name,count(1) as nn from  tm3 where cate1 is not null group by cate1,cate1name,cate2,cate2name,cate3,cate3name,uid')
    cols=['uid','cate1','cate1name','cate2','cate2name','cate3','cate3name']
    rss3=rss1.dropDuplicates(cols).select(cols)

    res.show()
    rss1.show()
    rss2.show(500)
    rss3.show(500)

    jsD={

    }
    lsD=rss2.collect()
    for i in lsD:
        uid=i['uid']
        cate1=i['cate1']
        cate1name=i['cate1name']
        cate2=i['cate2']
        cate2name=i['cate2name']
        cate3=i['cate3']
        cate3name=i['cate3name']
        if uid not in jsD:
            jsD[uid]={}
        if cate1 not in jsD[uid]:
            jsD[uid][cate1]={}
            jsD[uid][cate1]['ID']=cate1
            jsD[uid][cate1]['ParentID']=0
            jsD[uid][cate1]['Name']=cate1name
            jsD[uid][cate1]['children']={}
        if cate2 not in jsD[uid][cate1]['children']:
            jsD[uid][cate1]['children'][cate2]={}
            jsD[uid][cate1]['children'][cate2]['ID']=cate2
            jsD[uid][cate1]['children'][cate2]['ParentID']=cate1
            jsD[uid][cate1]['children'][cate2]['Name']=cate2name
            jsD[uid][cate1]['children'][cate2]['children']={}
        if cate3 not in jsD[uid][cate1]['children'][cate2]['children']:
            jsD[uid][cate1]['children'][cate2]['children'][cate3]={}
            jsD[uid][cate1]['children'][cate2]['children'][cate3]["ID"]=cate3
            jsD[uid][cate1]['children'][cate2]['children'][cate3]["ParentID"]=cate2
            jsD[uid][cate1]['children'][cate2]['children'][cate3]["Name"]=cate3name


    print(jsD)
    print(json.dumps(jsD,ensure_ascii=False))

    import redis
    pool = redis.ConnectionPool(host='120.27.241.54', port=6379, decode_responses=True)

    r = redis.Redis(connection_pool=pool)
    print(datetime.datetime.now(),'redis1')
    for k,v in jsD.items():
        r.set(k,json.dumps(v,ensure_ascii=False))
    print(datetime.datetime.now(),'redis2')




    print(res.count())
    print(rss1.count())

    # res.groupBy


    # print(list)
    # res.createOrReplaceTempView('res')
    # sql2="select GROUP_CONCAT('{',cate1,':',cate2,'}' , ',') cate from (SELECT cate1,GROUP_CONCAT('{',cate2,':',cate3,'}' , ',') cate2 from (SELECT cate1,cate2,GROUP_CONCAT('{',cate3,':',name,'}' , ',') cate3 from  (SELECT cate1,cate2,cate3,cate3name from res GROUP BY cate3) a group by cate2) b GROUP BY cate1) c"
    # sql2="select concat_ws(',',collect_set(cate1)) from  (select  concat_ws('','{',cate1,':',cate2,'}') cate1 from (select cate1,concat_ws(',',collect_set(cate2)) cate2  from (select cate1,concat_ws('','{',cate2,':',cate3,'}') cate2 from (select cate1,cate2,concat_ws(',',collect_set(cate3))cate3 from (SELECT cate1,cate2,CONCAT_WS('','{',cate3,':',cate3name,'}') cate3 FROM (SELECT cate1,cate2,cate3,cate3name FROM res ) a) b group by cate1,cate2) c) d group by cate1) e) f"
    # print(res.rdd.getNumPartitions())
    print('开始导入数据')
    print(datetime.datetime.now())

    # print(res.toJSON)
    # res.write.format("org.elasticsearch.spark.sql").option('es.nodes','master:9200,slave1:9200,slave2:9200')\
    #     .mode('append')\
    #     .save('demo3/test')


    # res.foreachPartition(f)

    # ACTIONS=[]
    # es=Elasticsearch(['master','slave1','slave2'],timeout=6000)
    # for line in list:
    #     action = {
    #         "_index": 'demo32',
    #         "_type": 'test',
    #         "_source": {
    #             "pid": line['skuid'],
    #             "name": line['NAME'],
    #             "introduction": line['Introduction'],
    #             "label": line['label']}
    #     }
    #     ACTIONS.append(action)
    #     # 批量处理
    # bulk(es, ACTIONS, index='demo32')

    print(datetime.datetime.now())
    # print(res.count())
    time.sleep(1000)






    # recommendProduct.asynComputedUserRecommendProd(int(6))
