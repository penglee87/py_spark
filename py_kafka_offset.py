#!/usr/bin/python
# coding=utf-8

'''https://www.cnblogs.com/dplearning/p/7762481.html
'''
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import time
import os
import json
broker_list = "192.168.52.31:6667"
topic_name = "ljd_mac"
timer = 5
offsetRanges = []


def store_offset_ranges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


def save_offset_ranges(rdd):
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    data = dict()
    f = open(record_path, "a")
    for o in offsetRanges:
        data = {"topic": o.topic, "partition": o.partition, "fromOffset": o.fromOffset, "untilOffset": o.untilOffset}
    f.write(json.dumps(data))
    f.close()


def deal_data(rdd):
    data = rdd.collect()
    for d in data:
        # do something
        # print(d)
        pass

def save_by_spark_streaming():
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    from_offsets = {}
    # 获取已有的offset，没有记录文件时则用默认值即最大值
    if os.path.exists(record_path):
        f = open(record_path, "r")
        offset_data = json.loads(f.read())
        f.close()
        if offset_data["topic"] != topic_name:
            raise Exception("the topic name in offset.txt is incorrect")

        topic_partion = TopicAndPartition(offset_data["topic"], offset_data["partition"])
        print('topic_partion',type(topic_partion))
        from_offsets = {str(topic_partion): int(offset_data["untilOffset"])}  # 设置起始offset的方法(topic_partion不转为str时不能作为字典的key)
        print ("start from offsets: %s" % from_offsets)
        print ("type(from_offsets)", type(from_offsets))


    sc = SparkContext(appName="Realtime-Analytics-Engine")
    ssc = StreamingContext(sc, int(timer))
    
    '''
    createDirectStream方法中读取from_offsets时,提示AttributeError: 'str' object has no attribute '_jTopicAndPartition'
    与上面将topic_partion转为str矛盾，具体原因如下:
    jfromOffsets = dict([(k._jTopicAndPartition(helper),
                              v) for (k, v) in fromOffsets.items()])
    '''
    #kvs = KafkaUtils.createDirectStream(ssc=ssc, topics=[topic_name], fromOffsets=from_offsets,kafkaParams={"metadata.broker.list": broker_list})  
    kvs = KafkaUtils.createDirectStream(ssc=ssc, topics=[topic_name], kafkaParams={"metadata.broker.list": broker_list})
    kvs.foreachRDD(lambda rec: deal_data(rec))
    kvs.transform(store_offset_ranges).foreachRDD(save_offset_ranges)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


if __name__ == '__main__':
    save_by_spark_streaming()