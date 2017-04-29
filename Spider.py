# -*- coding: utf-8 -*-
from __future__ import absolute_import, with_statement
from wcf.xml2records import XMLParser
from wcf.records import dump_records, Record, print_records
from io import StringIO, BytesIO

import data as all_data
import json, re, base64, zlib, datetime
import requests, pymysql, xmltodict, redis

# 所需要的请求以及相关格式
# GET action:GetCitiesByPid?pid=11 获得省份城市
# POST action:GetProvincePublishLives <pid> 获取省份所有站点信息
# POST action:GetCityDayAqiHistoryByCondition <cityCode> 城市历史信息-最近两周
# POST action:GetAreaAQIPublishLive <area>杭州市 城市最新的各站点信息
# POST action:GetAreaIaqiPublishLive iAQI <area>杭州市 IAQI

# 关于各空气质量等级的建议
air_me = {'优':{'measure':'各类人群可正常活动','unhealthful':'空气质量令人满意，基本无空气污染'},
          '良':{'measure':'极少数异常敏感人群应减少户外活动','unhealthful':'空气质量可接受，但某些污染物可能对极少数异常敏感人群健康有较弱影响'},
          '轻度污染':{'measure':'儿童、老年人及心脏病、呼吸系统疾病患者应减少长时间、高强度的户外锻炼','unhealthful':'易感人群症状有轻度加剧，健康人群出现刺激症状'},
          '中度污染':{'measure':'儿童、老年人及心脏病、呼吸系统疾病患者应减少长时间、高强度的户外锻炼，一般人群适量减少户外活动','unhealthful':'进一步加剧易感人群症状，可能对健康人群心脏、呼吸系统有影响'},
          '重度污染':{'measure':'老年人及心脏病、肺病患者应停留在室内，停止户外活动，一般人群减少户外活动','unhealthful':'心脏病和肺病患者症状显著加剧，运动耐力降低，健康人群普遍出现症状'}}

class SpiderMain(object):

    def __init__(self):
        # self.provinceArray = json.loads(data.provinceList_json)
        # self.cityArray = json.loads(data.cityList_json)
        # self.stationArray = json.loads(data.stationList_json)
        self.cityDict = all_data.cityList_json
        self.connection = pymysql.Connect(host='127.0.0.1',
                                          user='root', passwd=‘xxxxx’,
                                          charset="utf8")
        self.con_cursor = self.connection.cursor()
        self.dt = datetime.datetime
        self.now = self.dt.now().strftime('%Y%m')
        self.con_cursor.execute("USE AQI")

        self.redis_conn = redis.Redis(host='localhost', port=6379,
                                      db=0, decode_responses=False,
                                      encoding='utf-8')

        self.url = 'http://106.37.208.233:20035/ClientBin/Env-CnemcPublish-RiaServices-EnvCnemcPublishDomainService.svc/binary/'
        self.headers = {'user-agent': "User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; rv:11.0) like Gecko",
                        'accept-language': "zh-CN,zh;q=0.8",
                        'referer': "http://106.37.208.233:20035/ClientBin/cnemc.xap",
                        'content-type': "application/msbin1",
                        'cache-control': "no-cache"}

    # 准备监测点数据表
    def createSiteTable(self):
        # execute的字符串转义，强制利用python的转义
        self.con_cursor.execute("CREATE TABLE if not exists site_table_%s" % self.now +
                                "(`siteID` VARCHAR(11) NOT NULL DEFAULT '',"
                                "`Time` DATETIME NOT NULL,"
                                "`AQI` INT(11) DEFAULT NULL,"
                                "`PM2_5` INT(11) DEFAULT NULL,"
                                "`PM10` INT(11) DEFAULT NULL,"
                                "`SO2` INT(11) DEFAULT NULL,"
                                "`NO2` INT(11) DEFAULT NULL,"
                                "`CO` FLOAT DEFAULT NULL,"
                                "`O3` INT(11) DEFAULT NULL,"
                                "`PrimaryPollutant` VARCHAR(30) DEFAULT NULL,"
                                "`Quality` VARCHAR(11) DEFAULT NULL"
                                ") ENGINE=InnoDB DEFAULT charset=utf8 "
                                "PARTITION BY RANGE (DAY(time)) \
                              (PARTITION p1 VALUES LESS THAN (2) ENGINE = InnoDB,\
                               PARTITION p2 VALUES LESS THAN (3) ENGINE = InnoDB,\
                               PARTITION p3 VALUES LESS THAN (4) ENGINE = InnoDB,\
                               PARTITION p4 VALUES LESS THAN (5) ENGINE = InnoDB,\
                               PARTITION p5 VALUES LESS THAN (6) ENGINE = InnoDB,\
                               PARTITION p6 VALUES LESS THAN (7) ENGINE = InnoDB,\
                               PARTITION p7 VALUES LESS THAN (8) ENGINE = InnoDB,\
                               PARTITION p8 VALUES LESS THAN (9) ENGINE = InnoDB,\
                               PARTITION p9 VALUES LESS THAN (10) ENGINE = InnoDB,\
                               PARTITION p10 VALUES LESS THAN (11) ENGINE = InnoDB,\
                               PARTITION p11 VALUES LESS THAN (12) ENGINE = InnoDB,\
                               PARTITION p12 VALUES LESS THAN (13) ENGINE = InnoDB,\
                               PARTITION p13 VALUES LESS THAN (14) ENGINE = InnoDB,\
                               PARTITION p14 VALUES LESS THAN (15) ENGINE = InnoDB,\
                               PARTITION p15 VALUES LESS THAN (16) ENGINE = InnoDB,\
                               PARTITION p16 VALUES LESS THAN (17) ENGINE = InnoDB,\
                               PARTITION p17 VALUES LESS THAN (18) ENGINE = InnoDB,\
                               PARTITION p18 VALUES LESS THAN (19) ENGINE = InnoDB,\
                               PARTITION p19 VALUES LESS THAN (20) ENGINE = InnoDB,\
                               PARTITION p20 VALUES LESS THAN (21) ENGINE = InnoDB,\
                               PARTITION p21 VALUES LESS THAN (22) ENGINE = InnoDB,\
                               PARTITION p22 VALUES LESS THAN (23) ENGINE = InnoDB,\
                               PARTITION p23 VALUES LESS THAN (24) ENGINE = InnoDB,\
                               PARTITION p24 VALUES LESS THAN (25) ENGINE = InnoDB,\
                               PARTITION p25 VALUES LESS THAN (26) ENGINE = InnoDB,\
                               PARTITION p26 VALUES LESS THAN (27) ENGINE = InnoDB,\
                               PARTITION p27 VALUES LESS THAN (28) ENGINE = InnoDB,\
                               PARTITION p28 VALUES LESS THAN (29) ENGINE = InnoDB,\
                               PARTITION p29 VALUES LESS THAN (30) ENGINE = InnoDB,\
                               PARTITION p30 VALUES LESS THAN (31) ENGINE = InnoDB,\
                               PARTITION p31 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)")

    # 准备城市实时信息数据表
    def createCityRealTimeTable(self):
        self.con_cursor.execute("CREATE TABLE if not exists city_rt_table_%s" % self.now +
                                "(`cityID` INT(11) NOT NULL,"
                                "`Time` DATETIME NOT NULL,"
                                "`AQI` INT(11) DEFAULT NULL,"
                                "`PM2_5` INT(11) DEFAULT NULL,"
                                "`PM10` INT(11) DEFAULT NULL,"
                                "`SO2` INT(11) DEFAULT NULL,"
                                "`NO2` INT(11) DEFAULT NULL,"
                                "`CO` FLOAT DEFAULT NULL,"
                                "`O3` INT(11) DEFAULT NULL,"
                                "`PrimaryPollutant` VARCHAR(30) DEFAULT NULL,"
                                "`Quality` VARCHAR(11) DEFAULT NULL"
                                ") ENGINE=InnoDB DEFAULT charset=utf8 "
                                "PARTITION BY RANGE (DAY(time)) \
                                (PARTITION p1 VALUES LESS THAN (2) ENGINE = InnoDB,\
                               PARTITION p2 VALUES LESS THAN (3) ENGINE = InnoDB,\
                               PARTITION p3 VALUES LESS THAN (4) ENGINE = InnoDB,\
                               PARTITION p4 VALUES LESS THAN (5) ENGINE = InnoDB,\
                               PARTITION p5 VALUES LESS THAN (6) ENGINE = InnoDB,\
                               PARTITION p6 VALUES LESS THAN (7) ENGINE = InnoDB,\
                               PARTITION p7 VALUES LESS THAN (8) ENGINE = InnoDB,\
                               PARTITION p8 VALUES LESS THAN (9) ENGINE = InnoDB,\
                               PARTITION p9 VALUES LESS THAN (10) ENGINE = InnoDB,\
                               PARTITION p10 VALUES LESS THAN (11) ENGINE = InnoDB,\
                               PARTITION p11 VALUES LESS THAN (12) ENGINE = InnoDB,\
                               PARTITION p12 VALUES LESS THAN (13) ENGINE = InnoDB,\
                               PARTITION p13 VALUES LESS THAN (14) ENGINE = InnoDB,\
                               PARTITION p14 VALUES LESS THAN (15) ENGINE = InnoDB,\
                               PARTITION p15 VALUES LESS THAN (16) ENGINE = InnoDB,\
                               PARTITION p16 VALUES LESS THAN (17) ENGINE = InnoDB,\
                               PARTITION p17 VALUES LESS THAN (18) ENGINE = InnoDB,\
                               PARTITION p18 VALUES LESS THAN (19) ENGINE = InnoDB,\
                               PARTITION p19 VALUES LESS THAN (20) ENGINE = InnoDB,\
                               PARTITION p20 VALUES LESS THAN (21) ENGINE = InnoDB,\
                               PARTITION p21 VALUES LESS THAN (22) ENGINE = InnoDB,\
                               PARTITION p22 VALUES LESS THAN (23) ENGINE = InnoDB,\
                               PARTITION p23 VALUES LESS THAN (24) ENGINE = InnoDB,\
                               PARTITION p24 VALUES LESS THAN (25) ENGINE = InnoDB,\
                               PARTITION p25 VALUES LESS THAN (26) ENGINE = InnoDB,\
                               PARTITION p26 VALUES LESS THAN (27) ENGINE = InnoDB,\
                               PARTITION p27 VALUES LESS THAN (28) ENGINE = InnoDB,\
                               PARTITION p28 VALUES LESS THAN (29) ENGINE = InnoDB,\
                               PARTITION p29 VALUES LESS THAN (30) ENGINE = InnoDB,\
                               PARTITION p30 VALUES LESS THAN (31) ENGINE = InnoDB,\
                               PARTITION p31 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)")

    # 准备城市日信息数据表
    def createCityDayTable(self):
        self.con_cursor.execute("CREATE TABLE if not exists city_table_%s" % self.now +
                                "(`cityID` INT(11) NOT NULL,"
                                "`Time` DATETIME NOT NULL,"
                                "`AQI` INT(11) DEFAULT NULL,"
                                "`PM2_5` INT(11) DEFAULT NULL,"
                                "`PM10` INT(11) DEFAULT NULL,"
                                "`SO2` INT(11) DEFAULT NULL,"
                                "`NO2` INT(11) DEFAULT NULL,"
                                "`CO` FLOAT DEFAULT NULL,"
                                "`O3` INT(11) DEFAULT NULL,"
                                "`PrimaryPollutant` VARCHAR(30) DEFAULT NULL,"
                                "`Quality` VARCHAR(11) DEFAULT NULL"
                                ") ENGINE=InnoDB DEFAULT charset=utf8 "
                                "PARTITION BY RANGE (DAY(time)) \
                                (PARTITION p1 VALUES LESS THAN (2) ENGINE = InnoDB,\
                               PARTITION p2 VALUES LESS THAN (3) ENGINE = InnoDB,\
                               PARTITION p3 VALUES LESS THAN (4) ENGINE = InnoDB,\
                               PARTITION p4 VALUES LESS THAN (5) ENGINE = InnoDB,\
                               PARTITION p5 VALUES LESS THAN (6) ENGINE = InnoDB,\
                               PARTITION p6 VALUES LESS THAN (7) ENGINE = InnoDB,\
                               PARTITION p7 VALUES LESS THAN (8) ENGINE = InnoDB,\
                               PARTITION p8 VALUES LESS THAN (9) ENGINE = InnoDB,\
                               PARTITION p9 VALUES LESS THAN (10) ENGINE = InnoDB,\
                               PARTITION p10 VALUES LESS THAN (11) ENGINE = InnoDB,\
                               PARTITION p11 VALUES LESS THAN (12) ENGINE = InnoDB,\
                               PARTITION p12 VALUES LESS THAN (13) ENGINE = InnoDB,\
                               PARTITION p13 VALUES LESS THAN (14) ENGINE = InnoDB,\
                               PARTITION p14 VALUES LESS THAN (15) ENGINE = InnoDB,\
                               PARTITION p15 VALUES LESS THAN (16) ENGINE = InnoDB,\
                               PARTITION p16 VALUES LESS THAN (17) ENGINE = InnoDB,\
                               PARTITION p17 VALUES LESS THAN (18) ENGINE = InnoDB,\
                               PARTITION p18 VALUES LESS THAN (19) ENGINE = InnoDB,\
                               PARTITION p19 VALUES LESS THAN (20) ENGINE = InnoDB,\
                               PARTITION p20 VALUES LESS THAN (21) ENGINE = InnoDB,\
                               PARTITION p21 VALUES LESS THAN (22) ENGINE = InnoDB,\
                               PARTITION p22 VALUES LESS THAN (23) ENGINE = InnoDB,\
                               PARTITION p23 VALUES LESS THAN (24) ENGINE = InnoDB,\
                               PARTITION p24 VALUES LESS THAN (25) ENGINE = InnoDB,\
                               PARTITION p25 VALUES LESS THAN (26) ENGINE = InnoDB,\
                               PARTITION p26 VALUES LESS THAN (27) ENGINE = InnoDB,\
                               PARTITION p27 VALUES LESS THAN (28) ENGINE = InnoDB,\
                               PARTITION p28 VALUES LESS THAN (29) ENGINE = InnoDB,\
                               PARTITION p29 VALUES LESS THAN (30) ENGINE = InnoDB,\
                               PARTITION p30 VALUES LESS THAN (31) ENGINE = InnoDB,\
                               PARTITION p31 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)")

    # 写入监测站信息到数据库
    def WTDB_siteinfo(self, data):
        self.con_cursor.execute("INSERT INTO site_table_%s" % self.now + " \
                                            (`siteID`,`Time`,`AQI`,`PM2_5`,`PM10`,`SO2`,`NO2`,`CO`,`O3`,`PrimaryPollutant`,`Quality`) \
                                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
            data['StationCode'], data['Time'], data['AQI'],
            data['PM2_5'], data['PM10'], data['SO2'], data['NO2'],
            data['CO'], data['O3'], data['PrimaryPollutant'], data['Quality']))

    # 写入城市信息到数据库
    def WTDB_cityinfo(self, data):
        self.con_cursor.execute("INSERT INTO city_table_%s" % self.now + " \
                                                    (`cityID`, `Time`,`AQI`,`PM2_5`,`PM10`,`SO2`,`NO2`,`CO`,`O3`,`PrimaryPollutant`,`Quality`) \
                                                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
            data['CityCode'], data['Time'], data['AQI'],
            data['PM2_5'], data['PM10'], data['SO2'], data['NO2'],
            data['CO'], data['O3'], data['PrimaryPollutant'],
            data['Quality']))

    # 写入实时城市信息到数据库
    def WTDB_cityrealtimeinfo(self, data):
        self.con_cursor.execute("INSERT INTO city_rt_table_%s" % self.now + " \
                                                    (`cityID` ,`Time`,`AQI`,`PM2_5`,`PM10`,`SO2`,`NO2`,`CO`,`O3`,`PrimaryPollutant`,`Quality`) \
                                                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
            data['CityCode'], data['Time'], data['AQI'],
            data['PM2_5'], data['PM10'], data['SO2'], data['NO2'],
            data['CO'], data['O3'], data['PrimaryPollutant'],data['Quality']))

    # 爬取函数
    # para
    # action: 爬取方式
    # data: POST数据
    def craw(self, action, data):
        res = StringIO()
        res.write('<'+action+' xmlns="http://tempuri.org/">'+data+'</'+action+'>')
        res.seek(0)

        # usage from wcfbin-python
        res_r = XMLParser.parse(res)
        req = dump_records(res_r)

        # request
        request = requests.post(url=self.url+action,
                                data=req,
                                headers = self.headers)

        records = Record.parse(BytesIO(request.content))

        print_records(records, fp=res)
        res.seek(0)

        temp = res.readlines()

        pat = re.compile('<[^>]+>')
        enc = pat.sub('', temp[1][1:])[:-1]
        # print(type(enc))

        enc = base64.b64decode(enc)
        enc = zlib.decompress(enc)

        dict = xmltodict.parse(enc)
        # print(json.dumps(dict))
        return dict

    # 处理得到的city-rt-JSON数据
    def dealWithCityRealTimeData(self, dict):
        data = {}
        data['CityCode'] = int(dict['CityCode'])
        data['Area'] = dict['Area']
        data['Time'] = dict['TimePoint'].replace('T', ' ')
        data['PrimaryPollutant'] = dict['PrimaryPollutant']
        data['Quality'] = dict['Quality']
        # 排除没有具体数据的情况
        try:
            data['CO'] = float(dict['CO'])
        except:
            data['CO'] = 0.0

        list = ['AQI', 'PM2_5', 'PM10', 'SO2', 'NO2', 'O3']
        for index in list:
            try:
                data[index] = int(dict[index])
            except:
                data[index] = 0

        self.WTDB_cityrealtimeinfo(data)

        try:
            data['Measure'] = air_me[data['Quality']]['measure']
            data['Unhealthful'] = air_me[data['Quality']]['unhealthful']
        except:
            data['Measure'] = '—'
            data['Unhealthful'] = '—'

        json_formated = json.dumps(data, ensure_ascii=False)
        data_to_redis = '{ "CityCode": ' + str(data['CityCode']) + ', "Detail": ' + json_formated + '}'
        self.redis_conn.set(data['CityCode'], data_to_redis)

    # 处理得到的site-JSON数据
    def dealWithSiteData(self, dict):
        time = self.dt.strptime(dict['TimePoint'], '%Y-%m-%dT%H:%M:%S')
        data = {}
        data['StationCode'] = dict['StationCode']
        data['PositionName'] = dict['PositionName']
        data['Latitude'] = dict['Latitude']
        data['Longitude'] = dict['Longitude']
        data['Time'] = time
        data['Area'] = dict['Area']
        data['PrimaryPollutant'] = dict['PrimaryPollutant']
        data['Quality'] = dict['Quality']

        # 排除没有具体数据的情况
        try:
            data['CO'] = float(dict['CO'])
        except:
            data['CO'] = 0.0

        # O3_24h ---> O3
        try:
            data['O3'] = int(dict['O3_24h'])
        except:
            data['O3'] = 0

        list = ['AQI', 'PM2_5', 'PM10', 'SO2', 'NO2']
        for index in list:
            try:
                data[index] = int(dict[index])
            except:
                data[index] = 0

        self.WTDB_siteinfo(data)
        try:
            data['Measure'] = air_me[data['Quality']]['measure']
            data['Unhealthful'] = air_me[data['Quality']]['unhealthful']
        except:
            data['Measure'] = '—'
            data['Unhealthful'] = '—'

        data['Time'] = data['Time'].strftime('%Y-%m-%d %H:%M:%S')

        json_formated = json.dumps(data, ensure_ascii=False)

        # Redis-写入到最新24小时站点信息
        redis_name = data['StationCode'] + '_24h'
        # print(json_formated)
        self.redis_conn.lpush(redis_name, json_formated)
        if self.redis_conn.llen(redis_name) >= 24:
            self.redis_conn.rpop(redis_name)

        # Redis-写入到最新站点信息
        data_to_redis = '{ "StationCode": "' +data['StationCode'] + '", "Detail": ' + json_formated + '}'
        self.redis_conn.set(data['StationCode'], data_to_redis)

        return json.loads(json_formated)

    # 获得城市实时信息
    def getCityRealTimeInfo(self):
        self.createCityRealTimeTable()
        action = 'GetCityRealTimeAQIModelByCitycode'
        for key in self.cityDict.keys():
            request = requests.get(
                    url= self.url + action,
                    params={'cityCode': key},
                    headers=self.headers)
            records = Record.parse(BytesIO(request.content))
            res = StringIO()
            print_records(records, fp=res)
            res.seek(0)

            temp = res.readlines()
            str = ''
            for row in temp:
                    row = row.replace('[\'', '')
                    row = row.replace('\']', '')
                    row = row.replace('\\n\'', '')
                    row = row.replace('a:', '')
                    row = row.replace('b:', '')
                    row = row.replace('&mdash;', '—')
                    str = str+row

            dict = xmltodict.parse(str)
            json_d = json.loads(json.dumps(dict))

            result = json_d['GetCityRealTimeAQIModelByCitycodeResponse']['GetCityRealTimeAQIModelByCitycodeResult']['RootResults']['CityAQIPublishLive']

            # print(result)
            self.dealWithCityRealTimeData(result)


    # 获得所有站点的信息-通过省份查询所有站点
    def getAllSitesInfoByProvince(self):
        self.createSiteTable()
        for key in range(30):
            json_data = self.craw('GetProvincePublishLives','<pid>'+str(key+1)+'</pid>')

            for site in json_data['ArrayOfAQIDataPublishLive']['AQIDataPublishLive']:
                self.dealWithSiteData(site)


    # 获得所有站点的信息-通过城市查询所有站点
    def getAllSitesInfoByCity(self):
        self.createSiteTable()
        for key in self.cityDict.keys():
            json_data = self.craw('GetAreaAQIPublishLive', '<area>' + self.cityDict[key] + '</area>')

            try:
                get_data = json_data['ArrayOfAQIDataPublishLive']['AQIDataPublishLive']
            except:
                continue

            # print(key)
            # data_to_redis = {}
            # data_to_redis['ID'] = key+'_allsite'
            if isinstance(get_data, dict):
                # 格式化的站点信息(JSON)
                site_jsoned = self.dealWithSiteData(get_data)

                # Redis-写入到最新城市所有站点信息
                data_to_redis = '{ "CityCode_AllSites": ' + key + ', "Detail": ' + str(site_jsoned) + '}'
                self.redis_conn.set(key + '_allsite', data_to_redis)
            else:
                data_formated_dict = []
                for site in get_data:
                    # 格式化的站点信息(JSON)
                    site_jsoned = self.dealWithSiteData(site)

                    # 城市站点append
                    data_formated_dict.append(site_jsoned)

                # print(data_formated_dict)
                # print(json.dumps(data_formated_dict, ensure_ascii=False))

                # Redis-写入到最新城市所有站点信息
                data_to_redis = '{ "CityCode_AllSites": ' + key + ', "Detail": ' + json.dumps(data_formated_dict, ensure_ascii=False) + '}'
                self.redis_conn.set(key + '_allsite', data_to_redis)


    # 获得城市日信息
    def getAllCityInfo(self):
        self.createCityDayTable()
        for key in self.cityDict.keys():
            json_data = self.craw('GetCityDayAqiHistoryByCondition', '<cityCode>' + key + '</cityCode>')

            try:
                history_data = json_data['ArrayOfCityDayAQIPublishHistory']['CityDayAQIPublishHistory']
            except:
                continue

            latest_data = history_data[len(history_data) - 1]

            time = self.dt.strptime(latest_data['TimePoint'], '%Y-%m-%dT%H:%M:%S')
            data = {}
            data['CityCode'] = latest_data['CityCode']
            data['Area'] = latest_data['Area']
            data['Time'] = time
            data['PrimaryPollutant'] = latest_data['PrimaryPollutant']
            data['Quality'] = latest_data['Quality']

            # 排除没有具体数据的情况
            try:
                data['CO'] = float(latest_data['CO_24h'])
            except:
                data['CO'] = 0.0

            # AQI --> int
            try:
                data['AQI'] = int(latest_data['AQI'])
            except:
                data['AQI'] = 0

            # O3_8h_24h --> O3
            try:
                data['O3'] = int(latest_data['O3_8h_24h'])
            except:
                data['O3'] = 0

            list = ['NO2_24h', 'PM10_24h', 'PM2_5_24h', 'SO2_24h']
            for index in list:
                try:
                    data[index[0:-4]] = int(latest_data[index])
                except:
                    data[index[0:-4]] = 0

            self.WTDB_cityinfo(data)

            try:
                data['Measure'] = air_me[data['Quality']]['measure']
                data['Unhealthful'] = air_me[data['Quality']]['unhealthful']
            except:
                data['Measure'] = '—'
                data['Unhealthful'] = '—'

            data['Time'] = data['Time'].strftime('%Y-%m-%d %H:%M:%S')
            json_formated = json.dumps(data, ensure_ascii=False)

            # Redis-写入到最新一个月城市信息
            redis_name = data['CityCode'] + '_month'
            # print(json_formated)
            self.redis_conn.lpush(redis_name, json_formated)
            if self.redis_conn.llen(redis_name) >= 30:
                self.redis_conn.rpop(redis_name)


    # update json-formated redis data after crawing
    def updateGeneralRedis(self,data):
        self.redis_conn.set(data['ID'], data['detail'])

    # 更新城市所有站点的redis数据
    def updateCityAllSites(self,data):
        self.redis_conn.set(data['ID'], data['detail'])
