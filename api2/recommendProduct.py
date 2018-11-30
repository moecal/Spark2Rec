#coding=utf-8
#!/usr/bin/python
# -*-coding : UTF-8 -*-
import logging  #
import json
import datetime
import sys
import pandas as pd
import os

import happybase

from api2.mysql import mysql

# connection = happybase.Connection(host='120.27.241.54', transport='framed', protocol='compact')
# connection.open()
# table = connection.table('usersize_recommend')

pool = happybase.ConnectionPool(size=10, host='120.27.241.54', transport='framed', protocol='compact')


class recommendProduct(object):

    """docstring for ClassName"""
    def __init__(self, ):
        pass

    def computedUserRecommendProd(userid):
        print(userid)
        # userid = userid.decode('utf8')
        # userid = json.loads(userid)
        # userid = userid['userid']
        logging.info(userid)

        import threading
        t = threading.Thread(target=recommendProduct.asynComputedUserRecommendProd, args=(userid,))
        t.start()
        # return {'msg': 'Signal received', 'status': 200}

    def asynComputedUserRecommendProd(userid):
            print(userid)
            BoxStartTime = datetime.datetime.now().strftime('%Y-%m-%d')
            t1 = datetime.datetime.now()
            print(t1)

            # f1 = open('/home/alpha_ml/sparkjob/log/1.txt', 'r+')
            # # f1 = open('/Users/ganlin/Dev_Doc/1.txt', 'r+')
            # f1.read()
            # f1.write('\n' + str(userid))
            # f1.write('\n' + str(t1))


            # 通过用户ID获取用户推荐尺码的推荐商品   document http://confluence.champzee.com/pages/viewpage.action?pageId=4102378
            # import requests
            # logging.info('recommendProduct start')
            #
            # data = {"user_id": userid,
            #         "token": "eyJhbGciOiJIUzI1NiIsImlhdCI6MTUzNDU1ODYxMywiZXhwIjoxNTM0NTYyMjEzfQ.eyJpZCI6MX0.kcZYxB2rLtolWM9tuw2PiRlgodiBNCgz_7gQ5Sx2a4M"}
            # res = requests.post('http://120.27.241.37:8816/api/v0.1/size_recomd', json=data)
            # t11 = datetime.datetime.now()
            # # f1.write('\n' + str(t11))
            # # res = requests.post('http://120.27.243.224:8000/api/v0.1/size_recomd', json=data)
            #
            # print('成功请求')
            # # 检测是否请求成功
            # if res.status_code == 200 and res.json()['status'] != 200:
            #     return res.json()
            #
            # prodInfo = res.json()['size_sorted']
            #
            # logging.info('travese query data start')
            # products = {}
            # for k, v in enumerate(res.json()['size_sorted']):
            #     info = {}
            #     info['Score'] = int(v['prob'] * 1000)
            #     info['Tag_like'] = []
            #     info['Tag_ray'] = []
            #     products[int(v['product_id'])] = info
            #
            # logging.info('get prodids start')
            # print('get prodids start')
            # # 后续查询商品信息的时候用到
            # prodids = []
            # for k, v in enumerate(res.json()['size_sorted']):
            #     prodids.append(str(int(v['product_id'])))
            # logging.info('get prodids end')
            # t3 = datetime.datetime.now()
            # print(t3)

            # cells = eval(table.cells(str(userid), 'results:pids')[0].decode())
            products = {}
            prodids=[]

            with pool.connection() as conn:
                table = conn.table('usersize_recommend')
                # table = conn.table('size_test')
                cells = eval(table.cells(str(userid), 'results:pids')[0].decode())
                for k in cells.keys():
                    info = {}
                    info['Score'] = int(cells.get(k) * 1000)
                    info['Tag_like'] = []
                    info['Tag_ray'] = []
                    products[k] = info
                prodids=[str(x) for x in list(cells.keys())]
            print("合适的尺码推荐结果商品——————————————————————————————————————————")
            print(len(prodids))



            # 通过用户ID获取用户的标签信息


            # 颜色喜好  已购买的商品中反馈为颜色喜欢 超过两个的 获取cate2 和颜色
            sql = 'SELECT\
                        spu.Cate2,\
                        skc.Color\
                    FROM\
                        boxdetail bd\
                    LEFT JOIN boxdetailfeedback bdf ON bd.ID = bdf.BoxDetailID\
                    LEFT JOIN feedback fb ON fb.ID = bdf.FeedbackID\
                    LEFT JOIN product p ON p.ID = bd.ProdID\
                    LEFT JOIN spu ON spu.ID = p.SPUID\
                    LEFT JOIN skc ON skc.ID = p.SKCID\
                    WHERE\
                        bd.UserID = %s\
                    AND bd.Deleted = 0\
                    AND bd.`Status` = 3\
                    AND bdf.Deleted = 0\
                    AND fb.`Name` = "喜欢这个颜色"\
                    AND fb.Deleted = 0\
                    GROUP BY\
                        skc.Color,\
                        bdf.BoxDetailID\
                    HAVING\
                        count(bdf.ID) > 1' % (userid)
            res = mysql.get(sql)
            prefColor = {}
            if res:
                for v in res:
                    if v['Cate2'] not in prefColor:
                        prefColor[v['Cate2']] = []

                    if v['Cate2'] in prefColor:
                        prefColor[v['Cate2']].append(v['Color'])
            print("获取颜色喜好")

            # 面料喜好 已购买商品中反馈为面料舒适 超过亮的 获取cate2 和面料
            sql = 'SELECT\
                        spu.Cate2,\
                        pa.ID\
                    FROM\
                        boxdetail bd\
                    LEFT JOIN boxdetailfeedback bdf ON bd.ID = bdf.BoxDetailID\
                    LEFT JOIN feedback fb ON bdf.FeedbackID = fb.ID\
                    LEFT JOIN product p ON p.ID = bd.ProdID\
                    LEFT JOIN spuattribute spua ON spua.SPUID = p.SPUID\
                    LEFT JOIN productattribute pa ON pa.ID = spua.AttributeID\
                    LEFT JOIN spu on spu.ID = p.SPUID\
                    WHERE\
                        bd.UserID = %s\
                    AND bd.`Status` = 3\
                    and bd.Deleted = 0\
                    AND pa.Type = 25\
                    and pa.Deleted = 0\
                    and fb.`Name` = "面料舒适"\
                    and fb.Deleted = 0\
                    GROUP BY\
                        pa.ID,\
                        bdf.BoxDetailID\
                    HAVING\
                        count(bdf.ID) > 1' % (userid)
            res = mysql.get(sql)
            prefMaterial = {}
            if res:
                for v in res:
                    # 面料非天然不考虑
                    if v['ID'] == 202:
                        continue

                    if v['Cate2'] not in prefMaterial:
                        prefMaterial[v['Cate2']] = []

                    if v['Cate2'] in prefMaterial:
                        prefMaterial[v['Cate2']].append(v['ID'])
            print("获取面料喜好")

            # 活雷标签
            sql = 'SELECT Data FROM useranalysis WHERE UserID = %s AND Deleted = 0' % (userid)
            res = mysql.get(sql)
            userAnalysis = None
            if res:
                userAnalysis = json.loads(res[0]['Data'])
            print("活雷标签")
            # 已有类似
            sql = "SELECT\
                        spu.Cate2,\
                        spu.Cate3\
                    FROM\
                        boxdetailfeedback bfb\
                    LEFT JOIN feedback fb ON fb.ID = bfb.FeedbackID\
                    LEFT JOIN boxdetail bd ON bd.ID = bfb.BoxDetailID\
                    LEFT JOIN product p ON p.ID = bd.ProdID\
                    LEFT JOIN spu ON spu.ID = p.SPUID\
                    WHERE\
                        bfb.Deleted = 0\
                    AND fb.Deleted = 0\
                    AND fb.`Name` in( '已有类似')\
                    AND bd.UserID = %s\
                    GROUP BY spu.Cate3" % (userid)
            res = mysql.get(sql)
            hadSimilar = []
            if res:
                res = recommendProduct.confirmGeneralizeCate(res)
                for v in res:
                    hadSimilar.append(v[0])
            print("活雷-已有类似")
            print( datetime.datetime.now())
            # 活雷-用户朋友留过
            # sql = "SELECT distinct(p.SKCID) FROM boxdetail bd LEFT JOIN product p on bd.ProdID = p.ID WHERE bd.UserID IN (SELECT ID FROM `user` WHERE ReferredBy = %s OR ID \
            # IN (SELECT ReferredBy FROM `user` WHERE ID = %s)) AND bd.`Status` = 3 AND bd.Deleted = 0" % (userid, userid)
            sql = "SELECT DISTINCT(p.SKCID) FROM boxdetail bd LEFT JOIN product p ON bd.ProdID = p.ID WHERE( bd.UserID IN( SELECT ID FROM `user` WHERE ReferredBy = %s) \
                        OR bd.UserID IN( SELECT ReferredBy FROM `user` WHERE ID = %s)) AND bd.`Status` = 3 AND bd.Deleted = 0" % (
            userid, userid)
            res = mysql.get(sql)
            friendHad = []
            if res:
                for v in res:
                    friendHad.append(v['SKCID'])
            print("活雷-用户朋友留过")
            print( datetime.datetime.now())
            # 活雷-抗拒面料
            sql = "SELECT\
                        spu.Cate2,\
                        spu.Cate3,\
                        pa.ID\
                    FROM\
                        boxdetailfeedback bfb\
                    LEFT JOIN feedback fb ON fb.ID = bfb.FeedbackID\
                    LEFT JOIN boxdetail bd ON bd.ID = bfb.BoxDetailID\
                    LEFT JOIN product p ON p.ID = bd.ProdID\
                    LEFT JOIN spuattribute spa on spa.ID = p.SPUID\
                    LEFT JOIN productattribute pa ON pa.ID = spa.AttributeID\
                    LEFT JOIN spu ON spu.ID = p.SPUID\
                    WHERE\
                    bd.UserID = %s\
                    AND bfb.Deleted = 0\
                    AND fb.Deleted = 0\
                    AND fb.`Name` in( '面料不舒服','面料不好打理','太透','质量差')\
                    AND pa.`Name` <> '纯棉'\
                    GROUP BY spu.Cate3,pa.ID" % (userid)
            res = mysql.get(sql)
            resistMarterial = []
            if res:
                resistMarterial = recommendProduct.confirmGeneralizeCate(res)
            print("活雷-抗拒面料")
            # 死雷标签-反馈
            # 不穿这个品类    参与泛化品类逻辑筛选
            sql = "SELECT\
                        spu.Cate2,\
                        spu.Cate3\
                    FROM\
                        boxdetailfeedback bfb\
                    LEFT JOIN feedback fb ON fb.ID = bfb.FeedbackID\
                    LEFT JOIN boxdetail bd on bd.ID = bfb.BoxDetailID\
                    LEFT JOIN product p ON p.ID = bd.ProdID\
                    LEFT JOIN spu on spu.ID = p.SPUID\
                    WHERE\
                        bfb.Deleted = 0\
                    AND fb.Deleted = 0\
                    AND fb.`Name` = '不穿这个品类'\
                    AND bd.UserID = %s\
                    GROUP BY spu.Cate3" % (userid)
            res = mysql.get(sql)
            resistCate = []
            if res:
                tmp = recommendProduct.confirmGeneralizeCate(res)
                for v in tmp:
                    resistCate.append(v[0])
            # logging.info(resistCate)
            print("死雷-不穿这个品类")
            # 抗拒领型      参与泛化品类逻辑筛选
            sql = "SELECT\
                        spu.Cate2,\
                        spu.Cate3,\
                        spua.AttributeID\
                    FROM\
                        boxdetailfeedback bfb\
                    LEFT JOIN feedback fb ON fb.ID = bfb.FeedbackID\
                    LEFT JOIN boxdetail bd ON bd.ID = bfb.BoxDetailID\
                    LEFT JOIN product p ON p.ID = bd.ProdID\
                    LEFT JOIN spu ON spu.ID = p.SPUID\
                    LEFT JOIN spuattribute spua ON spua.SPUID = p.SPUID\
                    LEFT JOIN productattribute pa ON pa.ID = spua.AttributeID\
                    WHERE\
                        bfb.Deleted = 0\
                    AND fb.Deleted = 0\
                    AND fb.`Name` = '抗拒领型'\
                    AND bd.UserID = %s\
                    AND spu.Deleted = 0\
                    and pa.Type = 7\
                    AND pa.Deleted = 0\
                    GROUP BY spu.Cate3,spua.AttributeID" % (userid)
            res = mysql.get(sql)
            resistNeckShape = []
            if res:
                resistNeckShape = recommendProduct.confirmGeneralizeCate(res, 1)
            # logging.info(resistNeckShape)
            print("死雷-抗拒领型")
            # 抗拒风格      参与泛化品类逻辑筛选
            sql = "SELECT\
                        spu.Cate2,\
                        spu.Cate3,\
                        skc.Color,\
                        spua.AttributeID\
                    FROM\
                        boxdetailfeedback bfb\
                    LEFT JOIN feedback fb ON fb.ID = bfb.FeedbackID\
                    LEFT JOIN boxdetail bd ON bd.ID = bfb.BoxDetailID\
                    LEFT JOIN product p ON p.ID = bd.ProdID\
                    LEFT JOIN spu ON spu.ID = p.SPUID\
                    LEFT JOIN spuattribute spua ON spua.SPUID = p.SPUID\
                    LEFT JOIN productattribute pa ON pa.ID = spua.AttributeID\
                    LEFT JOIN skc ON skc.id = p.skcid\
                    WHERE\
                        bfb.Deleted = 0\
                    AND fb.Deleted = 0\
                    AND fb.`Name` IN (\
                        '风格太潮流',\
                        '风格太休闲',\
                        '风格太正式',\
                        '风格太老气'\
                    )\
                    AND bd.UserID = %s\
                    AND pa.Type = 1\
                    AND pa.Deleted = 0\
                    GROUP BY spu.Cate2,spu.cate3,skc.Color,spua.AttributeID" % (userid)
            res = mysql.get(sql)
            resistStyle = None
            if res:
                resistStyle = recommendProduct.confirmGeneralizeCate(res)
            # logging.info(resistStyle)
            print("死雷-抗拒风格")

            # 抗拒品牌+品牌黑名单
            sql = "SELECT\
                        spu.Brand\
                    FROM\
                        boxdetailfeedback bfb\
                    LEFT JOIN feedback fb ON fb.ID = bfb.FeedbackID\
                    LEFT JOIN boxdetail bd ON bd.ID = bfb.BoxDetailID\
                    LEFT JOIN product p ON p.ID = bd.ProdID\
                    LEFT JOIN spu ON spu.ID = p.SPUID\
                    WHERE\
                        bfb.Deleted = 0\
                    AND fb.Deleted = 0\
                    AND fb.`Name` in( '品牌档次低','我的品牌黑名单')\
                    and bd.UserID = %s\
                    GROUP BY spu.Brand" % (userid)
            res = mysql.get(sql)
            resistBrand = []
            if res:
                for v in res:
                    resistBrand.append(v['Brand'])
            # logging.info(resistBrand)
            print("死雷-抗拒品牌")
            # # 抗拒颜色      参与泛化品类逻辑筛选    #等新版色块上线后再上线该逻辑
            # sql =   "SELECT\
            #             spu.Cate2,\
            #             spu.Cate3,\
            #             skc.Color\
            #         FROM\
            #             boxdetailfeedback bfb\
            #         LEFT JOIN feedback fb ON fb.ID = bfb.FeedbackID\
            #         LEFT JOIN boxdetail bd ON bd.ID = bfb.BoxDetailID\
            #         LEFT JOIN product p ON p.ID = bd.ProdID\
            #         LEFT JOIN spu ON spu.ID = p.SPUID\
            #         LEFT JOIN skc ON skc.id = p.skcid\
            #         WHERE\
            #             bfb.Deleted = 0\
            #         AND fb.Deleted = 0\
            #         AND fb.`Name` IN (\
            #             '就是不喜欢这个颜色',\
            #             '太明亮',\
            #             '太暗沉'\
            #         )\
            #         AND bd.UserID = %s\
            #         GROUP BY spu.Cate3,skc.Color" % (userid)
            # row = get_db().session.execute(sql)
            # res = row.fetchall()
            # # type = list[tuple], tuple[0] = Cate2, tuple[1] = Cate3, tuple[3] = Color
            # resistColor = None
            # if res:
            #     resistColor = self.confirmGeneralizeCate(res)

            # 死雷标签-用户资料

            # 用户发过
            sql = "SELECT p.SKCID FROM boxdetail bd LEFT JOIN product p ON bd.ProdID = p.ID WHERE bd.Deleted = 0 AND bd.UserID = %s and bd.`Status` in(2,3,4,6)  GROUP BY p.SKCID" % (
                userid)
            res = mysql.get(sql)
            sentSKC = []
            if res:
                for v in res:
                    sentSKC.append(v['SKCID'])
            logging.info('sentSKC')

            # 用户留过
            sql = "SELECT p.SKCID FROM boxdetail bd LEFT JOIN product p ON bd.ProdID = p.ID WHERE bd.Deleted = 0 AND bd.UserID = %s AND bd.`Status` = 3 GROUP BY p.SKCID" % (
                userid)
            res = mysql.get(sql)
            soldSKC = []
            if res:
                for v in res:
                    soldSKC.append(v['SKCID'])
            logging.info('soldSKC')

            # 抗拒该款式
            DislikeStyleCate = []
            if userAnalysis:
                if 'DislikeStyle' in userAnalysis:
                    sql = "SELECT `Value` FROM conf WHERE `Key` = 'analysis.DislikeStyle' AND Deleted = 0"
                    res = mysql.get(sql)
                    DislikeStyleCate = []
                    if res:
                        DislikeStyleList = json.loads(res[0]['Value'])
                        DislikeStyleListName = []
                        for v in userAnalysis['DislikeStyle']:
                            DislikeStyleListName.append(str(DislikeStyleList[str(v)]))

                        if DislikeStyleListName:
                            strDislikeStyleListName = str(tuple(DislikeStyleListName))
                            if ',)' in strDislikeStyleListName:
                                strDislikeStyleListName = strDislikeStyleListName.replace(",)", ")")

                            sql = "SELECT ID FROM productcate WHERE Name in %s AND Deleted = 0" % (strDislikeStyleListName)
                            res = mysql.get(sql)
                            # 通过userAnalysis里面填写的数据去找寻对应的Cate2、Cate3
                            if res:
                                for v in res:
                                    # list [Cate*]
                                    DislikeStyleCate.append(v['ID'])

            # 抗拒该图案
            DislikePattern = []
            if userAnalysis:
                if 'DislikePattern' in userAnalysis:
                    # type = dict, key = userAnalysis['DislikePattern']
                    DislikePatternList = {5: "大图案", 6: "明显字母数字", 101: "竖条纹", 102: "横条纹", 103: "格纹", 104: "格纹", 105: "精致印花",
                                          106: "休闲印花", 107: "迷彩", 9: "波点", 7: "明显LOGO徽章"}
                    DislikePatternListName = []
                    for v in userAnalysis['DislikePattern']:
                        DislikePatternListName.append(str(DislikePatternList[int(v)]))

                    if DislikePatternListName:
                        strDislikePatternListName = str(tuple(DislikePatternListName))
                        if ',)' in strDislikePatternListName:
                            strDislikePatternListName = strDislikePatternListName.replace(",)", ")")

                        sql = "SELECT ID FROM productattribute WHERE Type = 5 and Name in %s AND Deleted = 0" % (
                            strDislikePatternListName)
                        res = mysql.get(sql)
                        # 通过userAnalysis里面填写的数据去找寻对应的AttributeID
                        if res:
                            for v in res:
                                # list [Cate*]
                                DislikePattern.append(v['ID'])

            # 反季商品
            # ???

            # 获取商品相关信息
            productInfo = []
            sql = "SELECT p.ID,spu.ID as SPUID,spu.Cate2,spu.Cate3,spu.Brand,skc.Color,p.PreSizeID,p.SKCID FROM Product p LEFT JOIN spu ON spu.ID = p.SPUID LEFT JOIN skc on skc.ID = p.SKCID WHERE spu.ID > 0 AND p.AvaQuantity > 0 AND p.ID in (%s)" % (
                ','.join(prodids))
            res = mysql.get(sql)

            spuids = []
            if res:
                productInfo = res
                for v in productInfo:
                    v = list(v.values())
                    # if v[1] not in spuids:
                    if str(v[1]) not in spuids:

                        spuids.append(str(v[1]))
            # logging.info(spuids)

            # 获取商品标签信息
            sql = "SELECT SPUID,AttributeID FROM spuattribute WHERE Deleted = 0 AND SPUID IN (%s) GROUP BY SPUID,AttributeID" % (
                ','.join(spuids))
            res = mysql.get(sql)

            attribute = {}
            if res:
                for v in res:
                    if v['SPUID'] not in attribute:
                        attribute[v['SPUID']] = []
                    if v['AttributeID'] not in attribute[v['SPUID']]:
                        attribute[v['SPUID']].append(v['AttributeID'])
            # logging.info(attribute)

            logging.info(len(productInfo))

            for v in productInfo:
                # 死雷-不穿这个品类
                if resistCate:
                    if v in productInfo:
                        if v['SPUID'] in resistCate or v['Cate2'] in resistCate:
                            productInfo.remove(v)
                            del products[v['ID']]
                            logging.info('Cate removed')

                # 死雷-抗拒领型
                if resistNeckShape:
                    for vns in resistNeckShape:
                        if v in productInfo:
                            if v['SPUID'] in attribute:
                                if (v['Cate2'] == vns[0] or v['Cate3'] == vns[0]) and vns[1] in attribute[v['SPUID']]:
                                    productInfo.remove(v)
                                    del products[v['ID']]
                                    logging.info('NeckShape removed')

                # 死雷-抗拒风格
                if resistStyle:
                    for vs in resistStyle:
                        if v in productInfo:
                            if v['SPUID'] in attribute:
                                if (v['Cate2'] == vs[0] or v['Cate3'] == vs[0]) and vs[2] in attribute[v['SPUID']] and v[
                                    'Color'] == vs[1]:
                                    productInfo.remove(v)
                                    del products[v['ID']]
                                    logging.info('Style removed')

                # 死雷-抗拒品牌 品牌黑名单
                if resistBrand:
                    if v in productInfo:
                        if v['Brand'] in resistBrand:
                            productInfo.remove(v)
                            del products[v['ID']]
                            logging.info('Brand removed')

                # 死雷-抗拒颜色
                # 色块上线后做

                # 死雷-用户发过
                if sentSKC:
                    if v in productInfo:
                        if v['SKCID'] in set(sentSKC):
                            productInfo.remove(v)
                            del products[v['ID']]
                            logging.info('Sent removed')

                # 死雷-用户留过
                if soldSKC:
                    if v in productInfo:
                        if v['SKCID'] in set(soldSKC):
                            productInfo.remove(v)
                            del products[v['ID']]
                            logging.info('Sold removed')

                # 死雷-抗拒款式
                if DislikeStyleCate:
                    if v in productInfo:
                        if v['SPUID'] in attribute:
                            if ((v['Cate2'] in set(DislikeStyleCate) or v['Cate3'] in (DislikeStyleCate))
                                    or ("1" in userAnalysis['DislikeStyle'] and 66 in attribute[v['SPUID']] and v[
                                        'Cate2'] == 12)
                                    or ("99" in userAnalysis['DislikeStyle'] and 65 in attribute[v['SPUID']] and v[
                                        'Cate2'] == 12)):
                                productInfo.remove(v)
                                del products[v['ID']]
                                logging.info('Cate removed')

                # 死雷-抗拒图案
                if DislikePattern:
                    if v in productInfo:
                        for vp in DislikePattern:
                            if vp not in [36, 39, 168]:
                                if v['SPUID'] in attribute:
                                    if vp in attribute[v['SPUID']]:
                                        if v in productInfo:
                                            productInfo.remove(v)
                                            del products[v['ID']]
                                            logging.info('Pattern removed')


                # 死雷-反季

                # 算分规则 喜好+100 型录命中+10 活雷—1000  强制依据排序优先级

                # 活雷-抗拒字母
                if 36 in DislikePattern:
                    if v['SPUID'] in attribute:
                        if 36 in attribute[v['SPUID']]:
                            if v['ID'] in products:
                                products[v['ID']]['Score'] = products[v['ID']]['Score'] - 1000
                                products[v['ID']]['Tag_ray'].append('抗拒字母图案')
                                logging.info('Letter hate')

                # 活雷-抗拒logo
                if 39 in DislikePattern:
                    if v['SPUID'] in attribute:
                        if 39 in attribute[v['SPUID']]:
                            if v['ID'] in products:
                                products[v['ID']]['Score'] = products[v['ID']]['Score'] - 1000
                                products[v['ID']]['Tag_ray'].append('抗拒品牌logo')
                                logging.info('Logo hate')

                # 活雷-抗拒大面积图案
                if 168 in DislikePattern:
                    if v['SPUID'] in attribute:
                        if 168 in attribute[v['SPUID']]:
                            if v['ID'] in products:
                                products[v['ID']]['Score'] = products[v['ID']]['Score'] - 1000
                                products[v['ID']]['Tag_ray'].append('抗拒大面积图案')
                                logging.info('Big Pattern hate')

                # 活雷-抗拒该颜色 #等新版商品颜色上线后再上线该逻辑

                # 活雷-已有类似
                if v['Cate2'] in hadSimilar or v['Cate3'] in hadSimilar:
                    if v['ID'] in products:
                        products[v['ID']]['Score'] = products[v['ID']]['Score'] - 1000
                        products[v['ID']]['Tag_ray'].append('已有类似')
                        logging.info('Had Similar')

                # 活雷-用户朋友留过
                if v['SKCID'] in friendHad:
                    if v['ID'] in products:
                        products[v['ID']]['Score'] = products[v['ID']]['Score'] - 1000
                        products[v['ID']]['Tag_ray'].append('用户朋友留过')
                        logging.info('Friend Had')

                # 活雷-抗拒面料
                for vm in resistMarterial:
                    if v['SPUID'] in attribute:
                        if (v['Cate2'] == vm[0] or v['Cate3'] == vm[0]) and vm[1] in attribute[v['SPUID']]:
                            if v['ID'] in products:
                                products[v['ID']]['Score'] = products[v['ID']]['Score'] - 1000
                                products[v['ID']]['Tag_ray'].append('抗拒面料')
                                logging.info('Material Hate')

                # 活雷-旧版标签

                # 喜好-颜色喜好
                if prefColor:
                    if v in productInfo:
                        if v['Cate2'] in prefColor:
                            if v['Color'] in prefColor[v['Cate2']]:
                                products[v['ID']]['Score'] = products[v['ID']]['Score'] + 100
                                products[v['ID']]['Tag_like'].append('喜欢颜色')
                                logging.info('Color love')

                # 喜好-面料喜好
                if prefMaterial:
                    if v in productInfo:
                        if v['SPUID'] in attribute:
                            if v['Cate2'] in prefMaterial:
                                for vm in prefMaterial[v['Cate2']]:
                                    if vm in attribute[v['SPUID']]:
                                        products[v['ID']]['Score'] = products[v['ID']]['Score'] + 100
                                        products[v['ID']]['Tag_like'].append('喜欢面料')
                                        logging.info('Material love')
            print("分数计算完毕")
            t3 = datetime.datetime.now()
            print(t3)
            # print(products)
            prods = []
            for k, v in products.items():
                message = {}
                message['Uid'] = int(userid)
                message['Pid'] = int(k)
                message['Score'] = int(v['Score'])
                message['Tag_like'] = ' '.join(v['Tag_like'])
                message['Tag_ray'] = ' '.join(v['Tag_ray'])
                message['BoxStartTime']=BoxStartTime
                prods.append(message)
            logging.info(len(productInfo))
            t12 = datetime.datetime.now()

            return prods


            # 写入redis队列
            # from api.redis import redis
            # conn = redis.get_conn()
            #
            # for k, v in products.items():
            #     now = int(time.time())  # 查询的数据必须要在这个deadline以前
            #     now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
            #     message = {}
            #     message['UserID'] = int(userid)
            #     message['ProdID'] = int(k)
            #     message['Score'] = int(v['Score'])
            #     message['Tag_like'] = ' '.join(v['Tag_like'])
            #     message['Tag_ray'] = ' '.join(v['Tag_ray'])
            #     message['Time'] = now
            #     message = json.dumps(message)
            #     message = bytes(message, encoding="utf8")
            #
            #     conn.lpush('bach_shop_user_sku', message)
            #     # producer.produce(message)
            #     logging.info(message)

    # 确定泛化的Cate到底是Cate2还是Cate3
    # return list1[list2], list2[0] = Cate, list2[?] = other args
    def confirmGeneralizeCate(data, neckShape=0):
        Cate3ToCate2List = [
            [116, 123, 124, 127, 128, 133, 134, 152, 203, 204, 207, 208, 224, 225, 231, 236],
            [105, 106, 116, 123, 124, 127, 196, 197, 198, 199, 200, 224, 225]
        ]
        # 遍历list,如果其对应Cate3在需要查探Cate2的列表里,则删除其Cate3信息
        res = []
        for v in data:
            v = list(v.values())
            tmp = []
            for j in v:
                tmp.append(j)

            if v[1] in Cate3ToCate2List[neckShape]:
                del tmp[1]
            else:
                del tmp[0]

            res.append(tmp)

        return res



    # def test2(userid):
    #     cells = eval(table.cells(str(userid), 'results:pids')[0].decode())
    #     # print(cells)
    #     products = []
    #     for k in cells.keys():
    #         info = {}
    #         info['Uid']=userid
    #         info['Pid'] = k
    #         info['Score'] = int(cells.get(k) * 1000)
    #         info['Tag_like'] = []
    #         info['Tag_ray'] = []
    #         products.append(info)
    #     print('test2----------------')
    #     print(products)
    #     return products


    # def test3(userid):
    #     cells = eval(table.cells(str(userid), 'results:pids')[0].decode())
    #     print(cells)
    #     products = {}
    #     for k in cells.keys():
    #         info = {}
    #         info['Score'] = int(cells.get(k) * 1000)
    #         info['Tag_like'] = []
    #         info['Tag_ray'] = []
    #         products[k] = info
    #
    #     prods=[]
    #     for k, v in products.items():
    #         message = {}
    #         message['Uid'] = int(userid)
    #         message['Pid'] = int(k)
    #         message['Score'] = int(v['Score'])
    #         message['Tag_like'] = ' '.join(v['Tag_like'])
    #         message['Tag_ray'] = ' '.join(v['Tag_ray'])
    #         prods.append(message)
    #     print('test3----------------')
    #     print(prods)

if __name__ == '__main__':

        # userid=58
        # if len(sys.argv) == 2:
        #     userid=int(sys.argv[1])
        #
        # print(userid)
        # t1 = datetime.datetime.now()
    prods=recommendProduct.asynComputedUserRecommendProd(68045)
    print('最终结果-----------------------')
    print(prods)
        # print(datetime.datetime.now().strftime('%Y-%m-%d'))
