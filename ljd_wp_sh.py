#!/usr/bin/env python
# -*- coding:utf-8 -*-
'''
同时处理多个流数据
'''
import sys,os
import json
import cx_Oracle
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession,SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.readwriter import DataFrameWriter,DataFrameReader

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

#spark = SparkSession.builder.appName("PythonStreamingKafkaCarLjd").getOrCreate()
#sc = spark.sparkContext
#sc = SparkContext(appName="PythonStreamingKafkaCarLjd")
#sqlContext = SQLContext(sc)
sc = SparkContext(appName="PythonStreamingKafkaCarLjd")
ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)



brokers, topic = "192.168.52.31:6667", "ljd_car"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])
car_location = lines.map(lambda veh: (eval(veh)['location']))

brokers, topic = "192.168.52.31:6667", "ljd_mac"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])
mac_location = lines.map(lambda veh: (eval(veh)['location']))


host = '192.168.1.225'
user = 'test'
pwd = 'test'
url = 'jdbc:oracle:thin:@%s:1521:ORCL' % host
properties = {'user': user, 'password': pwd, 'driver': 'oracle.jdbc.driver.OracleDriver'}
dtr = DataFrameReader(sqlContext)
df_ljd_sfz_wp_dict = dtr.jdbc(url=url, table='ljd_sfz_wp_dict', properties=properties)
print('df_ljd_sfz_wp_dict',type(df_ljd_sfz_wp_dict))
df_ljd_sfz_wp_dict.show()
df_ljd_sfz_wp_dict.createOrReplaceTempView("tmp_ljd_sfz_wp_dict")

def process(time, rdd):
    print("========= %s =========" % str(time))    
    try:
        spark = SparkSession.builder.config(conf=rdd.context.getConf()).getOrCreate()
        rowRdd = rdd.map(lambda w: json.dumps(w))
        wplocation = spark.read.json(rowRdd)
        print('wplocation',type(wplocation),wplocation.dtypes)    
        wplocation.show()
        wplocation.createOrReplaceTempView("tmp_kafka_wp")
        #sql_kafka_wp = spark.sql("SELECT * FROM tmp_kafka_wp")
        #print('sql_kafka_wp',type(sql_kafka_wp))
        #sql_kafka_wp.show()
        
        conn = cx_Oracle.connect(user,pwd,host+":1521/orcl")
        
        print(11111)
        sqlDF = spark.sql("SELECT t1.zjhm,t1.wphm,t2.gd_jd,t2.gd_wd,t2.cjdbh,t2.cjdmc,t2.cjddz,t2.geohash6,t2.geohash,t2.cjsj,unix_timestamp(t2.cjsj) cjsj_int,t1.lx_dm,t1.lx FROM tmp_ljd_sfz_wp_dict t1 join tmp_kafka_wp t2 on t1.WPHM=t2.WPHM and t1.lx_dm=t2.lx_dm")
        print('sqlDF',type(sqlDF))
        sqlDF.show()
        wpinfo = sqlDF.rdd.map(lambda p: "'"+p.zjhm+"','"+p.wphm+"',"+str(p.gd_jd)+","+str(p.gd_wd)+",'"+p.cjdbh+"','"+p.cjdmc+"','"+p.cjddz+"','"+p.geohash6+"','"+p.geohash+"','"+p.cjsj+"',"+str(p.cjsj_int)+",'1','"+p.lx_dm+"','"+p.lx+"'").collect()
        for i in wpinfo:
            print('wpinfo',type(i),i)
            conn.cursor().execute("insert into ljd_sfz_result (zjhm,wphm,gd_jd,gd_wd,cjdbh,cjdmc,cjddz,gd_geohash6,gd_geohash,cjsj,cjsj_int,Sjdw,Lx_dm,lx) VALUES (" + i  +")")
            
        conn.commit()
        print(22222)
    except:
        print(33333)
        pass
        

car_location.foreachRDD(process)
mac_location.foreachRDD(process)
ssc.start()
ssc.awaitTermination()