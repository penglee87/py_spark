#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys,os
import json
import cx_Oracle
from pyspark import SparkConf,SparkContext
from pyspark.sql import Row,SparkSession,SQLContext,HiveContext
from pyspark.sql.readwriter import DataFrameWriter,DataFrameReader
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


#os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

spark = SparkSession.builder.appName("PythonStreamingKafkaWpLjd").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 10)

#brokers = sys.argv[1]
#topic = sys.argv[2]
#host = sys.argv[3]
#user = sys.argv[4]
#pwd = sys.argv[5]
#t_read = sys.argv[6]
#t_write = sys.argv[7]

brokers, topic = "192.168.52.31:6667", "ljd_mac"
host = '192.168.1.225'
user = 'test'
pwd = 'test'
t_read = 'id_mapping.ljd_sfz_wp_dict'
t_write = 'LJD_SFZ_RESULT_NEW'

kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])
wplocation = lines.map(lambda veh: (eval(veh)['location']))


url = 'jdbc:oracle:thin:@%s:1521:ORCL' % host
properties = {'user': user, 'password': pwd, 'driver': 'oracle.jdbc.driver.OracleDriver'}

df_ljd_sfz_wp_dict = spark.sql("SELECT * FROM " + t_read)
print('df_ljd_sfz_wp_dict',type(df_ljd_sfz_wp_dict))
df_ljd_sfz_wp_dict.show()
df_ljd_sfz_wp_dict.createOrReplaceTempView("tmp_ljd_sfz_wp_dict")

def process(time, rdd):
    print("========= %s =========" % str(time))
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'
    try:
        spark = SparkSession.builder.config(conf=rdd.context.getConf()).getOrCreate()
        rowRdd = rdd.map(lambda w: json.dumps(w))
        wplocation = spark.read.json(rowRdd)
        print('wplocation',type(wplocation),wplocation.dtypes)
        #wplocation.show()
        wplocation.createOrReplaceTempView("tmp_kafka_wp")
        #print('tmp_kafka_wp',type(tmp_kafka_wp))
        #sql_kafka_wp = spark.sql("SELECT * FROM tmp_kafka_wp")
        #print('sql_kafka_wp',type(sql_kafka_wp))
        
        sqlDF = spark.sql("SELECT T1.ZJHM,T1.WPHM,T2.GD_JD,T2.GD_WD,T2.CJDBH,T2.CJDMC,T2.CJDDZ,T2.GEOHASH6 GD_GEOHASH6,T2.GEOHASH GD_GEOHASH,T2.CJSJ,UNIX_TIMESTAMP(T2.CJSJ) CJSJ_INT,T1.ZDRYBS SJDW,T1.LX_DM,T1.LX,T2.XZQH_DM,T1.ZDRYBS,T1.ZDRYXM FROM TMP_LJD_SFZ_WP_DICT T1 JOIN TMP_KAFKA_WP T2 ON T1.WPHM=T2.WPHM AND T1.LX_DM=T2.LX_DM")
        #sqlDF = spark.sql("SELECT t1.ZJHM ,t1.WPHM  FROM tmp_ljd_sfz_wp_dict t1 join tmp_kafka_wp t2 on t1.WPHM=t2.WPHM and t1.lx_dm=t2.lx_dm")
        
        print('sqlDF',type(sqlDF))
        sqlDF.show()
        dtw = DataFrameWriter(sqlDF)
        print(10000)
        dtw.jdbc(url=url, table='LJD_SFZ_RESULT_NEW', mode='append', properties=properties)
        print(11111)
        conn = cx_Oracle.connect(user,pwd,host+":1521/orcl")
        sql1="TRUNCATE TABLE LJD_SFZ_RESULT_LATEST "
        sql2='''insert into LJD_SFZ_RESULT_LATEST
        select ZJHM,WPHM,GD_JD,GD_WD,CJDBH,CJDMC,CJDDZ,GD_GEOHASH6,GD_GEOHASH,
        CJSJ,CJSJ_INT,SJDW,LX_DM,LX,XZQH_DM,ZDRYBS,ZDRYXM
        from(
        select t.*,row_number() over (partition by t.zjhm order by cjsj_int desc) rn
        from LJD_SFZ_RESULT_NEW t
        )tt 
        where tt.rn=1'''
        
        sql3='''merge into LJD_SFZ_RESULT t1 using LJD_SFZ_RESULT_LATEST t2 on (t1.ZJHM=t2.ZJHM)
        when matched then
        update set 
        t1.WPHM=t2.WPHM,
        t1.GD_JD=t2.GD_JD,
        t1.GD_WD=t2.GD_WD,
        t1.CJDBH=t2.CJDBH,
        t1.CJDMC=t2.CJDMC,
        t1.CJDDZ=t2.CJDDZ,
        t1.GD_GEOHASH6=t2.GD_GEOHASH6,
        t1.GD_GEOHASH=t2.GD_GEOHASH,
        t1.CJSJ=t2.CJSJ,
        t1.CJSJ_INT=t2.CJSJ_INT,
        t1.SJDW=t2.SJDW,
        t1.LX_DM=t2.LX_DM,
        t1.LX=t2.LX,
        t1.XZQH_DM=t2.XZQH_DM,
        t1.ZDRYBS=t2.ZDRYBS,
        t1.ZDRYXM=t2.ZDRYXM
        when not matched then
        insert values(t2.ZJHM,t2.WPHM,t2.GD_JD,t2.GD_WD,t2.CJDBH,t2.CJDMC,t2.CJDDZ,t2.GD_GEOHASH6,t2.GD_GEOHASH,
        t2.CJSJ,t2.CJSJ_INT,t2.SJDW,t2.LX_DM,t2.LX,t2.XZQH_DM,t2.ZDRYBS,t2.ZDRYXM)'''
        sql4="TRUNCATE TABLE LJD_SFZ_RESULT_NEW "
        print(22222)
        conn.cursor().execute(sql1)
        conn.cursor().execute(sql2)
        conn.cursor().execute(sql3)
        conn.cursor().execute(sql4)
        #conn.commit()

        #conn = cx_Oracle.connect(user,pwd,host+":1521/orcl")
        #
        #sqlDF = spark.sql("SELECT t1.zjhm,t1.wphm,t2.gd_jd,t2.gd_wd,t2.cjdbh,t2.cjdmc,t2.cjddz,t2.geohash6,t2.geohash,t2.cjsj,unix_timestamp(t2.cjsj) cjsj_int,t1.lx_dm,t1.lx FROM tmp_ljd_sfz_wp_dict t1 join tmp_kafka_wp t2 on t1.WPHM=t2.WPHM and t1.lx_dm=t2.lx_dm")
        #wpinfo = sqlDF.rdd.map(lambda p: "'"+p.zjhm+"','"+p.wphm+"',"+str(p.gd_jd)+","+str(p.gd_wd)+",'"+p.cjdbh+"','"+p.cjdmc+"','"+p.cjddz+"','"+p.geohash6+"','"+p.geohash+"','"+p.cjsj+"',"+str(p.cjsj_int)+",'1','"+p.lx_dm+"','"+p.lx+"'").collect()
        #for i in wpinfo:
        #    conn.cursor().execute("insert into " + t_write + "(zjhm,wphm,gd_jd,gd_wd,cjdbh,cjdmc,cjddz,gd_geohash6,gd_geohash,cjsj,cjsj_int,Sjdw,Lx_dm,lx) VALUES (" + i  +")")
        #conn.commit()
        print(33333)

    except:
        print('eeeee')
        pass
        

wplocation.foreachRDD(process)
ssc.start()
ssc.awaitTermination()