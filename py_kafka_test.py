
###################
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


sc = SparkContext(appName="PythonStreamingKafkaWordCount")
ssc = StreamingContext(sc, 1)
print(11111)
zkQuorum, topic = "192.168.52.79:9092", "test"
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
print(22222)
print('kvs',kvs)
lines = kvs.map(lambda x: x[1])
print('lines',type(lines),lines)
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
#counts = lines.flatMap(lambda line: line.split(" "))
print('counts',counts)
#print('cv',counts.value)
counts.pprint()
print(33333)
ssc.start()
ssc.awaitTermination()


###################
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
ssc = StreamingContext(sc, 2)

brokers, topic = "192.168.52.79:9092", "test"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
print('counts',counts)
counts.pprint()
print(33333)
ssc.start()
ssc.awaitTermination()



#######################################
#jupyter页面运行
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

ssc=StreamingContext(sc,10)

brokers, topic = "192.168.52.31:6667", "ljd_mac"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
#kvs = KafkaUtils.createStream(ssc, brokers, "spark-streaming-consumer", {topic: 1})   #createStream 会返回一个错误,但会继续运行
lines = kvs.map(lambda x: x[1])
lines.pprint()
#fl = lines.flatMap(lambda line: eval(line))
#srcVehRecord = lines.map(lambda word: (eval(word)['srcVehRecord']))
#srcVehRecord.pprint()

ssc.start()
ssc.awaitTermination()
#ssc.stop()

##########################################
#命令行运行
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


sc = SparkContext(appName="PythonStreamingKafkaWordCount")
ssc = StreamingContext(sc, 1)
print(11111)
brokers, topic = "192.168.52.34:6667", "test"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
#kvs = KafkaUtils.createStream(ssc, brokers, "spark-streaming-consumer", {topic: 1})   #createStream存在问题
print(22222)
print('kvs',kvs)
lines = kvs.map(lambda x: x[1])
print('lines',type(lines),lines)
#counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
#counts.pprint()

ssc.start()
ssc.awaitTermination()





#############################################
#pyspark_kafka
import sys
import json
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

ssc=StreamingContext(sc,10)

brokers, topic = "192.168.52.31:6667", "test"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])
#fl = lines.flatMap(lambda line: eval(line))
srcVehRecord = lines.map(lambda word: (eval(word)['srcVehRecord']))
srcVehRecord.pprint()

def process(time, rdd):
    print("========= %s =========" % str(time))

    try:
        # Get the singleton instance of SparkSession
        #spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: json.dumps(w))
        print(11111)
        srcVehDataFrame = spark.read.json(rowRdd)
        print('srcVehDataFrame',type(srcVehDataFrame))

        srcVehDataFrame.show()
        srcVehDataFrame.createOrReplaceTempView("tmp_kafka_car")
        sqlDF = spark.sql("SELECT * FROM tmp_kafka_car")
        sqlDF.show()
        print(22222)
    except:
        pass
        print('pass')

srcVehRecord.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
#ssc.stop()



##############################################
#可执行
##############################################
#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys,os
import json
import cx_Oracle
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.readwriter import DataFrameWriter,DataFrameReader

#os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'
ssc=StreamingContext(sc,1)

brokers, topic = "192.168.52.31:6667", "ljd_car"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])
wplocation = lines.map(lambda veh: (eval(veh)['location']))
print(11111)
print('wplocation',type(wplocation))
#wplocation.pprint()


host = '192.168.1.225'
user = 'test'
pwd = 'test'
url = 'jdbc:oracle:thin:@%s:1521:ORCL' % host
properties = {'user': user, 'password': pwd, 'driver': 'oracle.jdbc.driver.OracleDriver'}
dtr = DataFrameReader(sqlContext)
df_sfz_wp_dict = dtr.jdbc(url=url, table='ljd_sfz_wp_dict', properties=properties)
print('df_sfz_wp_dict',type(df_sfz_wp_dict))
#df_sfz_wp_dict.show()
df_sfz_wp_dict.createOrReplaceTempView("tmp_sfz_wp_dict")

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        rowRdd = rdd.map(lambda w: json.dumps(w))
        srcVehDataFrame = spark.read.json(rowRdd)
        print('srcVehDataFrame',type(srcVehDataFrame))
        #srcVehDataFrame.show()
        srcVehDataFrame.createOrReplaceTempView("tmp_kafka_car")
        print('tmp_kafka_car',type(tmp_kafka_car))
        sql_kafka_car = spark.sql("SELECT * FROM tmp_kafka_car")
        print('sql_kafka_car',type(sql_kafka_car))
        
        #conn = cx_Oracle.connect(user,pwd,host+":1521/orcl")
        #oracle源数据表可进行update,但kafka源数据表update时失败,即使无中文字符
        #conn.cursor().execute("update sf_car_test2 t2 set ljd= (select distinct ljd from sf_car_test3 t3 where t2.hphm=t3.hphm)")
        #conn.cursor().execute("update sf_car_test2 t2 set ljd= (select distinct locationName ljd from tmp_kafka_car tk where t2.hphm=tk.carNum)")
        #conn.commit()
        
        #sqlDF = spark.sql("SELECT t1.gmsfhm,t1.car_num,t1.hpzl,t1.hphm,t2.locationName ljd FROM tmp_sf_car t1 join tmp_kafka_car t2 on t1.hphm=t2.carNum")
        #print('sqlDF',type(sqlDF))
        #sqlDF.show()
        #carinfo = sqlDF.rdd.map(lambda p: "'"+p.gmsfhm+"','"+p.car_num+"','"+p.hpzl+"','"+p.hphm+"','"+p.ljd+"','"+str(time)+"'").collect()
        #for i in carinfo:
        #    print('carinfo',type(i),i)
        #    conn.cursor().execute("insert into sf_car_test3 (gmsfhm,car_num,hpzl,hphm,ljd,last_time) VALUES (" + i  +")")
        
        #conn.commit()
        
        print(22222)
        
        sqlDF = spark.sql("SELECT t1.zjhm,t1.wphm FROM tmp_sf_car t1 ")
        print('sqlDF',type(sqlDF))
        #sqlDF.show()
        sqlDF.write.mode(saveMode="append").jdbc(url=url,table='sf_car_test4',properties=properties)
        #dtw = DataFrameWriter(sqlDF)
        #print('dtw',type(dtw))
        #写入失败,即使无中文字符
        #dtw.jdbc(url=url, table='sf_car_test2', mode='append', properties=properties)
        #dtw.jdbc(url=url, table='sf_car_test3', mode='append', properties=properties)
        print(33333)
    except:
        print(44444)
wplocation.foreachRDD(process)
ssc.start()
ssc.awaitTermination()


'''
-------------------------------------------
Time: 2018-01-19 14:32:00
-------------------------------------------
('{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5251430001,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5251430001.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"Z","carBrand":10,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙A86S39","capTime":"2017/04/08 10:04:00"}}', '{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5251430001,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5251430001.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"Z","carBrand":10,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙A86S39","capTime":"2017/04/08 10:04:00"}}')
('{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5253607681,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5253607681.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"K3","carBrand":118,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙AT0266","capTime":"2017/04/08 11:56:00"}}', '{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5253607681,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5253607681.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"K3","carBrand":118,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙AT0266","capTime":"2017/04/08 11:56:00"}}')
('{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5253609551,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5253609551.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"K5","carBrand":8,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙AT9A11","capTime":"2017/04/08 11:56:00"}}', '{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5253609551,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5253609551.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"K5","carBrand":8,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙AT9A11","capTime":"2017/04/08 11:56:00"}}')
('{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5254072951,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5254072951.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"K3","carBrand":70,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙A729LR","capTime":"2017/04/08 12:22:00"}}', '{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5254072951,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5254072951.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"K3","carBrand":70,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙A729LR","capTime":"2017/04/08 12:22:00"}}')

-------------------------------------------
Time: 2018-01-19 14:32:00
-------------------------------------------
{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5251430001,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5251430001.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"Z","carBrand":10,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙A86S39","capTime":"2017/04/08 10:04:00"}}
{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5253607681,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5253607681.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"K3","carBrand":118,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙AT0266","capTime":"2017/04/08 11:56:00"}}
{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5253609551,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5253609551.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"K5","carBrand":8,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙AT9A11","capTime":"2017/04/08 11:56:00"}}
{"image":{"width":0,"id":"","url":"","height":0,"binData":""},"srcVehRecord":{"recordId":5254072951,"devId":9223372036854775807,"wayId":3,"imgUrl":"http://192.168.50.12/images/images2/5254072951.jpg","locationName":"建国北路朝晖路南口3","carNumType":2,"carType":"K3","carBrand":70,"locationId":9223372036854775807,"carNumColor":2,"carNum":"浙A729LR","capTime":"2017/04/08 12:22:00"}}

-------------------------------------------
Time: 2018-01-19 14:32:10
-------------------------------------------
'''


#coding=utf8
'''
 读取kafka数据 -> 解析 -> 统计 -> 返回driver写入redis
 关于redis连接池在集群模式下的处理问题是将特定的连接写到了方法内去调用

 已测试local、standalone模式可行
'''
from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re
import redis
# 解析日志
def parse(logstring):
    regex = '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*ip=\/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*tbl=([a-zA-Z0-9_]+)'
    pattern = re.compile(regex)
    m1 = pattern.search(str(logstring))
    if m1 is not None:
        m = m1.groups()
    else:
        m = None
    return m

class RedisClient:
    pool = None
    def __init__(self):
        self.getRedisPool()
    def getRedisPool(self):

        redisIp='10.10.10.230'
        redisPort=6379
        redisDB=0
        self.pool = redis.ConnectionPool(host=redisIp, port=redisPort, db=redisDB)
        return self.pool
    def insertRedis(self, key, value):
        if self.pool is None:
            self.pool = self.getRedisPool()
        r = redis.Redis(connection_pool=self.pool)
        r.hset('hsip', str(key), value)

if __name__ == '__main__':
    zkQuorum = '10.10.10.230:2181'
    topic = 'mytopic'
    sc = SparkContext(appName="pyspark kafka-streaming-redis")
    ssc = StreamingContext(sc, 15)
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "kafka-streaming-redis", {topic: 1})
    # 利用正则解析日志获取到结果为 (访问时间,访问ip,访问表名)
    # kafka读取返回的数据为tuple,长度为2，tuple[1]为实际的数据，tuple[1]的编码为Unicode
    # kvs.map(lambda x:x[1]).map(parse).pprint()
    # 预处理，如果需要多次计算则使用缓存
    ips = kvs.map(lambda line: line[1]).map(lambda x:parse(x)).filter(lambda x:True if x is not None and len(x) == 3 else False).map(lambda ip:(ip[1],1))
    ipcount = ips.reduceByKey(lambda a, b: a+b).map(lambda x:x[0]+':'+str(x[1]))
    # 传入rdd进行循坏，即用于foreachRdd(insertRedis)
    r = RedisClient()
    def echo(time,rdd):
        if rdd.isEmpty() is False:
            rddstr = "{"+','.join(rdd.collect())+"}"
            print (str(time)+":"+rddstr)
            r.insertRedis(str(time), rddstr)
    ipcount.foreachRDD(echo)
    # 各节点的rdd的循坏
    # wordCounts.foreachRDD(lambda rdd: rdd.foreach(sendRecord))
    ssc.start()
    ssc.awaitTermination()
    
    



