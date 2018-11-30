#!/usr/bin/python
# coding=utf-8
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from api2.recommendProduct import recommendProduct
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from api2.mysql import mysql

import os
import json

# broker_list = "master:9092,slave1:9092,slave2:9092"
broker_list = "120.27.241.54:9092,120.27.243.224:9092,114.55.226.34:9092"
topic_name = "test11"
timer = 30
sqlContext=SQLContext
offsetRanges=[]


def set_data(list):
    for line in list:
        yield  {
                "_index": 'demo1',
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
                    "CreTime": line["CreTime"]
                }
            }



def deal(datalist):
    es = Elasticsearch(['10.29.186.116'], timeout=6000)
    bulk(es, set_data(datalist), raise_on_error=False, stats_only=True,chunk_size=5000)

def save_redis(lsD):
    jsD={}
    for i in lsD:
        uid = i['uid']
        cate1 = i['cate1']
        cate1name = i['cate1name']
        cate2 = i['cate2']
        cate2name = i['cate2name']
        cate3 = i['cate3']
        cate3name = i['cate3name']
        if uid not in jsD:
            jsD[uid] = {}
        if cate1 not in jsD[uid]:
            jsD[uid][cate1] = {}
            jsD[uid][cate1]['ID'] = cate1
            jsD[uid][cate1]['ParentID'] = 0
            jsD[uid][cate1]['Name'] = cate1name
            jsD[uid][cate1]['children'] = {}
        if cate2 not in jsD[uid][cate1]['children']:
            jsD[uid][cate1]['children'][cate2] = {}
            jsD[uid][cate1]['children'][cate2]['ID'] = cate2
            jsD[uid][cate1]['children'][cate2]['ParentID'] = cate1
            jsD[uid][cate1]['children'][cate2]['Name'] = cate2name
            jsD[uid][cate1]['children'][cate2]['children'] = {}
        if cate3 not in jsD[uid][cate1]['children'][cate2]['children']:
            jsD[uid][cate1]['children'][cate2]['children'][cate3] = {}
            jsD[uid][cate1]['children'][cate2]['children'][cate3]["ID"] = cate3
            jsD[uid][cate1]['children'][cate2]['children'][cate3]["ParentID"] = cate2
            jsD[uid][cate1]['children'][cate2]['children'][cate3]["Name"] = cate3name
    print(json.dumps(jsD,ensure_ascii=False))


def sync_data2es(rdd):


    if not rdd.isEmpty():

        # rdd=rdd.cache()
        schema = StructType([StructField('Uid', IntegerType(), True), StructField('Pid', IntegerType(), True),StructField('Score', IntegerType(), True),StructField('Tag_like', StringType(), True),StructField('Tag_ray', StringType(), True)])
        sqlContext.createDataFrame(rdd,schema).createOrReplaceTempView('prods')
        # res = sqlContext.sql('select * from prods left join pd on prods.Pid=pd.skuid')
        res = sqlContext.sql('select a.*,b.*, now() as CreTime from prods a left join pd b on a.Pid=b.skuid')
        res=res.cache()
        res.createOrReplaceTempView('tm1')

        sqlContext.sql(
            'select tm1.*,cd.cate1name as cate1name,cd.cate2name as cate2name from tm1 left join cd on tm1.cate1=cd.cate1 and tm1.cate2=cd.cate2 and tm1.cate3=cd.cate3 ')\
            .createOrReplaceTempView('tm2')
        rss2 = sqlContext.sql(
            'select uid, cate1, cate1name,cate2,cate2name,cate3,cate3name,count(1) as nn from  tm2 where cate1 is not null group by cate1,cate1name,cate2,cate2name,cate3,cate3name,uid')

        lsd=rss2.collect()

        save_redis(lsd)

        res.foreachPartition(deal)
        save_offset()







def store_offset_ranges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd



def save_offset():
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    data = dict()
    f = open(record_path, "w")
    for o in offsetRanges:
        data = {"topic": o.topic, "partition": o.partition, "fromOffset": o.fromOffset, "untilOffset": o.untilOffset}
        f.write(json.dumps(data) + '\n')
    f.close()





def reco(data):
    # rdd.
    user_id = data[1]
    print(user_id)
    product=recommendProduct.asynComputedUserRecommendProd(int(user_id))
    # product=product[:10:1]
    return product





def save_by_spark_streaming():
    async=True
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    from_offsets = {}

    if os.path.exists(record_path):

        datas = []
        with open(record_path,'r') as f:
            for line in f:
                datas.append(line)

        for ii in datas:
            offset_data=json.loads(ii)
            topic_partion = TopicAndPartition(offset_data["topic"], offset_data["partition"])
            if topic_partion not in from_offsets:
                from_offsets[topic_partion]=int(offset_data["untilOffset"])



    sc = SparkContext(appName="Rec2champzee")
    global sqlContext
    sqlContext= SQLContext(sc)


    ssc = StreamingContext(sc, int(timer))

    kvs = KafkaUtils.createDirectStream(ssc=ssc, topics=[topic_name], fromOffsets=from_offsets,
                                        kafkaParams={"metadata.broker.list": broker_list})


    sql1 = '''
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

    prod_detail=mysql.get(sql1)
    cate_detail=mysql.get(sql2)
    sqlContext.createDataFrame(sc.parallelize(prod_detail)).createOrReplaceTempView('pd')
    sqlContext.createDataFrame(sc.parallelize(cate_detail)).createOrReplaceTempView('cd')
    sqlContext.cacheTable('pd')
    sqlContext.cacheTable('cd')
    kvs.transform(store_offset_ranges).flatMap(reco).foreachRDD(sync_data2es)


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


if __name__ == '__main__':
    save_by_spark_streaming()