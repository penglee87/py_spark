####################################
#python连kafka并将数据写入ORACLE
import sys
import json
import cx_Oracle
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
#connect to Kafka server and pass the topic we want to consume
#reload(sys) 
#sys.setdefaultencoding("utf8")
host = '192.168.1.225'
user = 'test'
pwd = 'test'
conn = cx_Oracle.connect(user,pwd,host+":1521/orcl")
#spark = SparkSession.builder.appName("PythonWordCount").config("spark.some.config.option", "some-value").getOrCreate()
#sc = SparkContext
#sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")

consumer = KafkaConsumer('test', bootstrap_servers = ['192.168.52.34:6667'])
for msg in consumer:    
    d = eval(bytes.decode(msg.value))['srcVehRecord']
    print(type(d),d)
    print(d['carNum'], d['locationName'])
    #conn.cursor().execute("insert into sf_car_test3 (carNum, locationName) VALUES ('carNum', 'locationName')")
    conn.cursor().execute("insert into sf_car_test3 (carNum, locationName) VALUES ('" + d['carNum']+"','"+ d['locationName'] +"')")
    conn.commit()
    j = json.dumps(d,ensure_ascii=False)
    print(type(j),j)
    #lines = sc.textFile("examples/src/main/resources/people.txt")
    #print('lines',type(lines),lines)
    #parts = lines.map(lambda l: l.split(","))
    #print('parts',type(parts),parts)
    #people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
    #df1 = spark.read.json("examples/src/main/resources/people.json")
    #print(type(df1),df1)
    #df2 = spark.read.load("examples/src/main/resources/people.json", format="json")
    #print(type(df2),df2)
    #df3 = spark.read.load(j, format="json")
    #print(type(df3),df3)
    #df = spark.read.json(j)
    #print(type(df),df)




####################################
#pyspark连hive,oracle
from pyspark.sql.readwriter import DataFrameWriter,DataFrameReader

user = 'test'
pwd = 'test'
url = 'jdbc:oracle:thin:@192.168.1.225:1521:ORCL'
#host = '192.168.1.225'
#url = 'jdbc:oracle:thin:@%s:1521:ORCL' % host
properties = {'user': user, 'password': pwd, 'driver': 'oracle.jdbc.driver.OracleDriver'}
#oracle数据写回oracle
dtr = DataFrameReader(sqlContext)
sf_car_test = dtr.jdbc(url=url, table='sf_car_test1', properties=properties)
#sf_car_test = spark.read.jdbc(url=url, table='sf_car_test1', properties=properties)
print('sf_car_test',type(sf_car_test))
sf_car_test.show()
dtw = DataFrameWriter(sf_car_test)
dtw.jdbc(url=url, table='sf_car_test2', mode='overwrite', properties=properties)
#dtw.jdbc(url=url, table='sf_car_test2', mode='append', properties=properties)
#sf_car_test.write.jdbc(url=url, table='sf_car_test2', properties=properties)  #append 方式写入
#sf_car_test.write.mode(saveMode="overwrite").jdbc(url=url, table='sf_car_test2', properties=properties)  #overwrite 方式写入


#转换后的表写回oracle
sf_car_test.createOrReplaceTempView("sf_car")
sf_car = spark.sql("SELECT gmsfhm,hphm FROM sf_car ")
print('sf_car',type(sf_car))
sf_car.show()
sf_car.write.jdbc(url=url, table='sf_car_test2', properties=properties)

dtw = DataFrameWriter(sf_car)
dtw.jdbc(url=url, table='sf_car_test4', mode='overwrite', properties=properties)


#hive数据写入oracle
my_dataframe = spark.sql("Select zjhm1,xm1 from family_rel.family_rel_01") 
#my_dataframe = sqlContext.sql("Select zjhm1 from family_rel.family_rel_01") 
print('my_dataframe',type(my_dataframe))
my_dataframe.show() 
dtw = DataFrameWriter(my_dataframe)
dtw.jdbc(url=url, table='sf_car_test4', mode='overwrite', properties=properties)
#my_dataframe.write.jdbc(url=url, table='sf_car_test2', properties=properties)  #append 方式写入

#hive数据写入hive
#my_dataframe.write.saveAsTable("id_mapping.test")  #创建hive表,存在权限问题
#spark.sql("insert into table id_mapping.dm_dn_id_mapping_dd_sub SELECT * FROM id_mapping.dm_dn_id_mapping_dd limit 10 ")  #权限问题
print(33333)


##############################
pyspark --driver-class-path ojdbc6-11.2.0.2.0.jar --jars ojdbc6-11.2.0.2.0.jar spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar examples/src/main/python/streaming/kafka_wordcount.py localhost:9092 test
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar --master local k_word_count.py
spark-submit --driver-class-path ojdbc6-11.2.0.2.0.jar --jars ojdbc6-11.2.0.2.0.jar test.py
spark-submit --driver-class-path ojdbc6.jar --jars ojdbc6.jar test.py
spark-submit --driver-class-path classes12.jar --jars classes12.jar test.py

spark-submit --master yarn  --total-executor-cores=4 --executor-memory=1G --jars=/home/public/jars/ojdbc6.jar
spark-submit --jars /home/public/jars/ojdbc6.jar,/home/public/jars/spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar ljd_mac_sh.py 192.168.52.31:6667 ljd_mac 192.168.1.225 test test ljd_sfz_wp_dict ljd_sfz_result
spark-submit --jars /home/public/jars/ojdbc6.jar,/home/public/jars/spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar ljd_result.py 192.168.52.31:6667 ljd_mac 192.168.1.225 test test id_mapping.ljd_sfz_wp_dict ljd_sfz_result
spark-submit --jars /home/public/jars/ojdbc6.jar,/home/public/jars/spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar ljd_result.py 192.168.52.31:6667 ljd_car 192.168.1.225 test test id_mapping.ljd_sfz_wp_dict ljd_sfz_result
pyspark --jars ojdbc6.jar,spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar

#brokers, topic = "192.168.52.31:6667", "ljd_car"
#host = '192.168.1.225'
#user = 'test'
#pwd = 'test'
#t_read = 'ljd_sfz_wp_dict'
#t_write = 'ljd_sfz_result'

#产生流数据(预先下载kafka包并解压)
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning


#创建topic,配置数据存储时间
sh /usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper 192.168.52.31:2181,192.168.52.32:2181,192.168.52.33:2181,192.168.52.34:2181,192.168.52.35:2181,192.168.52.36:2181 --replication-factor 3 --partitions 1 --topic ljd_car  --config log.retention.hours=12
sh /usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper 192.168.52.34:2181 --replication-factor 3 --partitions 1 --topic kktest
#查看topic列表
sh /usr/lib/kafka/bin/kafka-topics.sh --list --zookeeper 192.168.52.31:2181,192.168.52.32:2181,192.168.52.33:2181,192.168.52.34:2181,192.168.52.35:2181,192.168.52.36:2181
#查看指定Topic状态
sh /usr/lib/kafka/bin/kafka-topics.sh --describe --zookeeper 192.168.52.34:2181 --topic ljd_car
#修改topic对应数据存储时间
sh /usr/lib/kafka/bin/kafka-topics.sh  --zookeeper 192.168.52.31:2181 --alter --topic ljd_car --config delete.retention.ms=86400000   #已弃用
sh /usr/lib/kafka/bin/kafka-configs.sh --alter --zookeeper 192.168.52.31:2181 --entity-name ljd_car --entity-type topics --add-config delete.retention.ms=86400000

#####################
SPARK_HOME=/usr/lib/spark
[lip@dev conf]$ vi /usr/bin/pyspark
[lip@dev conf]$ vi /opt/python3.5/bin/pyspark

[lip@dev bin]$ whereis spark
spark: /etc/spark /usr/lib/spark
[lip@dev conf]$ ll /usr/lib/spark/conf
lrwxrwxrwx 1 root root 15 Apr  6  2017 /usr/lib/spark/conf -> /etc/spark/conf
[lip@dev conf]$ cd /usr/lib/spark/conf
[lip@dev conf]$ ll
total 64
-rw-r--r-- 1 root  root    987 Feb 10  2017 docker.properties.template
-rw-r--r-- 1 root  root   1105 Feb 10  2017 fairscheduler.xml.template
-rw-r--r-- 1 spark spark   894 Apr  6  2017 hive-site.xml
-rw-r--r-- 1 spark spark  1315 May 10  2017 log4j.properties
-rw-r--r-- 1 root  root   2025 Feb 10  2017 log4j.properties.template
-rw-r--r-- 1 spark spark  4956 Apr  6  2017 metrics.properties
-rw-r--r-- 1 root  root   7239 Feb 10  2017 metrics.properties.template
-rw-r--r-- 1 root  root    865 Feb 10  2017 slaves.template
-rw-r--r-- 1 spark spark   846 Jan 18 14:21 spark-defaults.conf
-rw-r--r-- 1 root  root   1292 Feb 10  2017 spark-defaults.conf.template
-rw-r--r-- 1 spark spark  1962 Nov  9 20:58 spark-env.sh
-rwxr-xr-x 1 root  root   3861 Feb 10  2017 spark-env.sh.template
-rwxr-xr-x 1 spark spark   209 Apr  6  2017 spark-thrift-fairscheduler.xml
-rw-r--r-- 1 hive  hadoop 1046 Jan 18 14:21 spark-thrift-sparkconf.conf


[lip@dev conf]$ vi spark-env.sh
# The java implementation to use.
export JAVA_HOME=/opt/leap-jdk

# pyspark  add by songguantao
export PYSPARK_PYTHON=/opt/python3.5/bin/python3
export PYSPARK_DRIVER_PYTHON=/opt/python3.5/bin/ipython3
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip 0.0.0.0 --port 9999"
export SPARK_CLASSPATH=/home/public/jars/



[lip@dev bin]$ ll /usr/lib/spark/bin
total 36
-rwxr-xr-x 1 root root 1099 Feb 10  2017 beeline
-rwxr-xr-x 1 root root 2143 Feb 10  2017 load-spark-env.sh
-rwxr-xr-x 1 root root 3265 Feb 10  2017 pyspark
-rwxr-xr-x 1 root root 1040 Feb 10  2017 run-example
-rwxr-xr-x 1 root root 2754 Feb 10  2017 spark-class
-rwxr-xr-x 1 root root 1049 Feb 10  2017 sparkR
-rwxr-xr-x 1 root root 3026 Feb 10  2017 spark-shell
-rwxr-xr-x 1 root root 1075 Feb 10  2017 spark-sql
-rwxr-xr-x 1 root root 1050 Feb 10  2017 spark-submit


from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

或
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()
spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
sc = spark.sparkContext
