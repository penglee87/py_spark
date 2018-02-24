#!/usr/bin/env python
# -*- coding:utf-8 -*-

import datetime
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import StringType, StructType, StructField

#初始化
conf = SparkConf().setAppName("First_in_car")
sc = SparkContext(conf=conf)
ssc = SQLContext(sc)

host = '192.168.1.225'
user = 'test'
pwd = 'test'
table = 'sf_car_test'


#读取oracle数据
from pyspark.sql.readwriter import DataFrameWriter,DataFrameReader
#数据库连接参数
url = 'jdbc:oracle:thin:@%s:1521:ORCL' % host
properties = {'user': user, 'password': pwd, 'driver': 'oracle.jdbc.driver.OracleDriver'}

#读取oracle中历史初次入城数据
dtr = DataFrameReader(ssc)
df_his_car = dtr.jdbc(url=url, table=table, properties=properties)
print('df_his_car',df_his_car)
df_his_car.show()
print(111111111111)

#sc.stop()
