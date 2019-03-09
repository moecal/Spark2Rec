#!/usr/bin/python
#coding=utf-8


import pymysql

# from config import Config   #导入配置文件111
import os

import logging  #引入日志

# pymysql在一个connection期间只能使用一个cursor,例如执行两条sql的时候cursor2会覆盖cursor1,所以在使用的时候不可同时存在两个cursor,只能单线程声明调用
class mysql(object):

    # 为了处理多次调用连接池的问题，这里声明了db来暂存当前的连接和cursor,在get_db 方法和 get_cursor 方法里做了检测
    db = None               # 数据库连接池
    cursor = None           # cusror

    conn = None             # 数据库连接配置
    database = None         # 选择数据库

    def __init__(self):
        logging.info('instancing')

    # 设置数据库连接配置
    # private 禁止外部调用
    def set_conn(conn = None):
        # 是否设置成功
        ret = False
        
        if conn == None:
            conn = 0

        # 是否存在连接配置,如果不存在连接配置,直接设置连接配置
        if not mysql.conn:
            mysql.conn = conn
            ret = True
        else:
            # 如果存在连接配置,则检测连接配置是否和之前一致,如果不一致直接设置新连接配置
            if mysql.conn != conn:
                mysql.conn = conn
                ret = True

        return ret

    # 选择数据库
    # private 禁止外部调用
    def set_database(database = None):
        # 是否设置成功
        ret = False

        if not database or database == 'default':
            database = 'operation'
            # database = Config.DB_DATABASE

        if not mysql.database:
            mysql.database = database
            ret = True
        else:
            if mysql.database != database:
                mysql.database = database
                ret = True

        return ret

    # 设置数据库连接池
    # private 禁止外部调用
    def set_db(database = None, conn = None):
        connRet = mysql.set_conn(conn)
        databaseRet = mysql.set_database(database)
        dbRet = False

        if not mysql.db:
            dbRet = True
        else:
            # 如果重新设置了连接配置,那么重新设置连接
            if connRet or databaseRet:
                dbRet = True

        if dbRet:
            if mysql.get_conn() == 0:
                host ='rm-bp12n2u9o7432w52b.mysql.rds.aliyuncs.com'
                # host = Config.DB_HOST
                username = 'champzee'
                # username = Config.DB_USERNAME
                password = 'JiuhQI9HvegvEiCM'
                # password = Config.DB_PASSWORD
                database = mysql.get_database()
            elif mysql.get_conn() == 1:
                # host = Config.DB_ANALYSIS_HOST
                # username = Config.DB_ANALYSIS_USERNAME
                # password = Config.DB_ANALYSIS_PASSWORD

                host = 'rm-bp12n2u9o7432w52b.mysql.rds.aliyuncs.com'
                username = 'champzee'
                password = 'JiuhQI9HvegvEiCM'
                database = mysql.get_database()
            
            mysql.db = pymysql.connect(host, username, password, database,charset='utf8mb4')
            mysql.get_cursor()

    # 获取当前数据库连接配置
    def get_conn():
        return mysql.conn

    # 获取当前选择的数据库
    def get_database():
        return mysql.database

    # 获取数据库连接池
    # @description
    # 切换mysql连接地址
    # 切换数据库
    # 获取数据库连接
    # @return 数据库连接池
    def get_db(database = None, conn = None):
        if conn == None:
            conn = mysql.get_conn()
        if not database:
            database = mysql.get_database()

        mysql.set_db(database, conn)
        # logging.info('%s %s' % (mysql.conn,mysql.database))
        return mysql.db

    # 声明cursor
    def get_cursor():
        if mysql.db:
            mysql.cursor = mysql.db.cursor(cursor=pymysql.cursors.DictCursor)
        else:
            mysql.cursor = mysql.get_db().cursor(cursor=pymysql.cursors.DictCursor)
        return mysql.cursor

    # 关闭连接
    def close_db():
        if mysql.db:
            mysql.db.close()
            mysql.db = None

    # 读操作
    def get(sql):
        # logging.info(sql)
        if not mysql.cursor:
            mysql.get_cursor()
        mysql.cursor.execute(sql)
        res = mysql.cursor.fetchall()
        return res

    # 写操作
    def set(sql, param = []):
        if not mysql.cursor:
            mysql.get_cursor()
        if param:
            try:
                affectedRows = mysql.cursor.executemany(sql, param)
                mysql.db.commit()
                return affectedRows
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                mysql.db.rollback()
                logging.info(pymysql.err.ProgrammingError, pymysql.err.InternalError)
                return 0
        else:
            try:
                affectedRows = mysql.cursor.execute(sql)
                mysql.db.commit()
                return affectedRows
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                mysql.db.rollback()
                logging.info(pymysql.err.ProgrammingError, pymysql.err.InternalError)
                return 0

    # pandas.readsql转dataframe
    def pandas_readsql(sql):
        import pandas as pd
        res = pd.read_sql(sql, con = mysql.get_db())
        return res

    # 析构
    # def __del__():
    #     mysql.close_db()

# pymysql查询出的结果集是tuple(tuple,tuple...) 可以直接转换格式