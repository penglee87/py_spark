#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys,os
import json
import cx_Oracle

#os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


host = '192.168.1.225'
user = 'test'
pwd = 'test'


conn = cx_Oracle.connect(user,pwd,host+":1521/orcl")
sql1="truncate table TMP_LJD_SFZ_RESULT "
sql2='''insert into TMP_LJD_SFZ_RESULT
select ZJHM,WPHM,GD_JD,GD_WD,CJDBH,CJDMC,CJDDZ,GD_GEOHASH6,GD_GEOHASH,
CJSJ,CJSJ_INT,SJDW,LX_DM,LX,XZQH_DM,ZDRYBS,ZDRYXM
from(
select t.*,row_number() over (partition by t.zjhm order by cjsj_int desc) rn
from LJD_SFZ_RESULT_TEST t
)tt 
where tt.rn=1'''

sql3='''merge into LJD_SFZ_RESULT t1 using TMP_LJD_SFZ_RESULT t2 on (t1.ZJHM=t2.ZJHM)
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
print(11111)
conn.cursor().execute(sql1)
print(22222)
conn.cursor().execute(sql2)
print(33333)
conn.cursor().execute(sql3)
conn.commit()
print('eeeee')
