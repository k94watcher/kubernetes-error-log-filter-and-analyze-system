#!/usr/bin/python3
# coding:utf-8

'''
日志处理程序

功能：
1. 从mongodb数据库中检索日志，并进行统计
2. 本地将统计数据以变量的形式存在内存中
3. 为分析程序提供接口

部署：
只需部署一个
必须与数据库，日志提取程序，日志分析程序部署在同一网络下

命令格式：   cleaner.py --db_ip <db_ip> --db_user <db_user> --db_password <db_password>
示例命令：   nohup python3 cleaner.py --db_ip 39.98.235.147 --db_user admin --db_password 123456 &
'''

import getopt, sys
import threading
import time, json
import datetime

from flask import Flask, request, jsonify, send_file
import ast
import demjson
import logging

# 数据库连接类
from connect_class import db_connect

# 设置日志格式，等级等
LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(pathname)s %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a '  # 配置输出时间的格式，注意月份和天数不要搞乱了
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    filename=r"cleaner.log"  # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )

# 全局变量
INIT_TIME = time.time()
DETAIL = []
DETAIL_ERROR = []
SOURCE_STATUS = []
STATISTICS_ERROR = {}
STATISTICS_WARN = {}
STATISTICS_NORMAL = {}
ANALYSE_DATA = {}
HEALTH = {}

# ------------------------------------------辅助类------------------------------------------
# *******************************************************************************************

# 重定义datetime格式的编码方法
# 如果没有这一段，则会出现错误：TypeError: Object of type datetime is not JSON serializable
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)


# 查询具体日志条目（辅助LocalAPI类）
class GetDataFromMongoDB:
    def __init__(self, db_ip, db_user, db_password):
        print('GetDataFromMongoDB init')
        self.params = {}
        self.params['db_ip'] = db_ip
        self.params['db_user'] = db_user
        self.params['db_password'] = db_password

    # 获取具体日志条目
    # 请求：{"machine": {"source":[id1, id2]}}
    # 返回值：{"machine": {"source":{"id": "data"}}}
    def get_data(self, get_data_input):
        get_data_output = {}
        if get_data_input:
            for machine in get_data_input:
                get_data_output[machine] = {}
                for source in get_data_input[machine]:
                    get_data_output[machine][source] = \
                        self.get_data_from_database(machine, source, get_data_input[machine][source])
            print('=========================')
            print('GetDataFromMongoDB finish')
            return get_data_output

    def get_data_from_database(self, db_name, col_name, id_list):
        return_dict = {}
        conn = db_connect.DataBase(db_name, self.params['db_user'], self.params['db_password'],
                                   self.params['db_ip'])
        request_dict = conn._request(col_name, id_list)
        for seq in range(len(request_dict)):
            return_dict[request_dict[seq]['log id']] = request_dict[seq]['data']
        return return_dict

    # 获取所有数据库的初始化时间
    def get_db_init_time(self):
        conn = db_connect.DataBase(None, self.params['db_user'], self.params['db_password'],
                                   self.params['db_ip'], check=1)
        return conn.check_init_time()


# ------------------------------------------数据库类------------------------------------------
# *******************************************************************************************

# 从mongodb数据库中定时提取日志
class GetStatisticsFromMongoDB(threading.Thread):
    # 初始化参数： 本机名，数据库ip，数据库用户，数据库密码，自定义日志位置， 告警判断选项，告警规则，
    def __init__(self, db_ip, db_user, db_password, threadLock):
        threading.Thread.__init__(self)
        self.params = {}
        # 数据库连接参数
        self.params['db_ip'] = db_ip
        self.params['db_user'] = db_user
        self.params['db_password'] = db_password
        # 数据库名，表名
        self.db_content = {}
        self.threadLock = threadLock

    # 数据库监控主函数
    def run(self):
        global DETAIL, DETAIL_ERROR, SOURCE_STATUS, STATISTICS_ERROR, STATISTICS_WARN, STATISTICS_NORMAL
        while 1:
            start_time = time.time()
            # 更新数据库和数据表信息
            self.db_content = self.check_db_and_col_names()
            # 获取detail表和statistics表的信息
            temp_detail = []
            temp_detail_error = []
            temp_source_status = []
            temp_statistics_error = []
            temp_statistics_warn = []
            temp_statistics_normal = []
            for db_name in self.db_content:
                if db_name != 'init_time':
                    for col_name in self.db_content[db_name]:
                        for data in self.get_detail(db_name, col_name):
                            temp_detail.append(data)
                        for data in self.get_detail(db_name, col_name, limit=1):
                            del data['log id']
                            temp_source_status.append(data)
                        for data in self.get_detail(db_name, col_name, urgent_class=2, limit=-1):
                            temp_detail_error.append(data)
                        s_error, s_warn, s_normal = self.get_statistics(db_name, col_name)
                        if s_error:
                            for i in s_error:
                                temp_statistics_error.append(i)
                        if s_warn:
                            for i in s_warn:
                                temp_statistics_warn.append(i)
                        if s_normal:
                            for i in s_normal:
                                temp_statistics_normal.append(i)
            # 将结果保存在全局变量中，方便api类调用
            self.threadLock.acquire()
            DETAIL = temp_detail
            DETAIL_ERROR = temp_detail_error
            SOURCE_STATUS = temp_source_status
            STATISTICS_ERROR = temp_statistics_error
            STATISTICS_WARN = temp_statistics_warn
            STATISTICS_NORMAL = temp_statistics_normal
            self.threadLock.release()
            while time.time() - start_time < 3:
                time.sleep(1)

    # 返回最新的表名和数据库名
    def check_db_and_col_names(self):
        db_names = []
        db_content = {}
        # 获取数据库名和表名
        conn = db_connect.DataBase(None, self.params['db_user'], self.params['db_password'],
                                   self.params['db_ip'], check=1)
        # 获取数据库名（即节点名称）
        for name in conn.get_db_names():
            if name not in ['admin', 'config', 'local']:
                db_names.append(name)
        # 获取每个数据库中的表名（即日志来源名称）
        for db_name in db_names:
            col_name_list = []
            for name in conn.get_col_names(db_name):
                if name[:12] != '[log backup]':
                    col_name_list.append(name)
            db_content[db_name] = col_name_list
        return db_content

    # 获取detail数据
    def get_detail(self, db_name, col_name, urgent_class=-1, limit=100):
        return_list = []
        conn = db_connect.DataBase(db_name, self.params['db_user'], self.params['db_password'],
                                   self.params['db_ip'])
        for data in conn._request_detail(col_name, urgent_class=urgent_class, limit=limit):
            data['machine'] = db_name
            data['source'] = col_name
            return_list.append(data)
        return return_list

    # 日志统计
    def get_statistics(self, db_name, col_name):
        statistics_error = []
        statistics_warn = []
        statistics_normal = []
        conn = db_connect.DataBase(db_name, self.params['db_user'], self.params['db_password'],
                                   self.params['db_ip'])
        for data in conn._request_statistics(col_name):
            temp_dict = {}
            temp_dict['time'] = data['_id']['date']
            temp_dict['count'] = data['count']
            temp_dict['machine'] = db_name
            temp_dict['source'] = col_name
            if data['_id']['urgent class'] == 2:
                statistics_error.append(temp_dict)
            elif data['_id']['urgent class'] == 1:
                statistics_warn.append(temp_dict)
            else:
                statistics_normal.append(temp_dict)
        return statistics_error, statistics_warn, statistics_normal


# ----------------------------------------flask api类----------------------------------------
# *******************************************************************************************

class LocalAPI(threading.Thread):
    def __init__(self, GetData, threadLock):
        threading.Thread.__init__(self)
        self.threadLock = threadLock
        self.GetData = GetData
        print('LocalAPI init')

    def run(self):
        app = Flask(__name__)
        MY_URL = '/cleaner/'
        print('start service')

        # 请求详细数据（每个日志来源100条日志）
        # (可选)输入： {"source":<source>, "machine":<machine>}
        @app.route(MY_URL + 'request_detail', endpoint='request_detail', methods=['GET'])
        def request_detail():
            logging.debug('request_detail received')
            return_dict = {}
            request_data = request.get_data()
            print(request_data)
            # 若无参数，则返回所有日志来源的全部数据
            if request_data == b'':
                return_dict['msg'] = DETAIL
                return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)
            # 若包含参数，则返回指定日志来源的数据
            else:
                # 获取传入参数
                request_data = demjson.decode(request_data)
                return_dict['msg'] = [n for n in DETAIL if
                                      n['source'] == request_data['source']
                                      and n['machine'] == request_data['machine']]
                return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)

        # 请求详细数据——分析模块专用
        # 去除了collector程序启动后两秒之前的所有日志信息，以避免统计出错
        # 只提取了告警日志
        @app.route(MY_URL + 'request_detail_for_analyse', endpoint='request_detail_for_analyse', methods=['GET'])
        def request_detail_for_analyse():
            logging.debug('request_detail_for_analyse received')
            return_dict = {}

            # 第一步，筛选出最高告警级别的日志
            detail_1 = [x for x in DETAIL_ERROR if x['urgent class'] == 2]

            # 第二步。筛选出collector程序运行两秒后生成的日志
            init_time_dict = self.GetData.get_db_init_time()
            detail_2 = [x for x in detail_1 if init_time_dict[x['machine']] +
                        datetime.timedelta(seconds=2) < x['time']]

            # 第三步，按照时间排序
            detail_3 = sorted(detail_2, key=lambda i: i['time'])

            return_dict['msg'] = detail_3
            return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)

        # 获取最近100条异常数据
        @app.route(MY_URL + 'request_detail_error', endpoint='request_detail_error', methods=['GET'])
        def request_detail_error():
            logging.debug('request_detail_error received')
            return_dict = {}
            # 第一步，筛选出最高告警级别的日志
            detail_1 = [x for x in DETAIL_ERROR if x['urgent class'] == 2 or x['urgent class'] == 1]
            # 第三步，按照时间排序
            detail_2 = sorted(detail_1, key=lambda i: i['time'])
            # 第三步，选取最新的最多100条日志
            detail_3 = detail_2[len(detail_2) - 100:]
            return_dict['msg'] = detail_3
            return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)

        # 获取日志来源的状态
        @app.route(MY_URL + 'request_source_status', endpoint='request_source_status', methods=['GET'])
        def request_source_status():
            logging.debug('request_detail received')
            return_dict = {}
            return_dict['msg'] = SOURCE_STATUS
            return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)

        # 请求统计数据
        # (可选)输入： {"source":<source>, "machine":<machine>}
        @app.route(MY_URL + 'request_statistics', endpoint='request_statistics', methods=['GET'])
        def request_detail():
            logging.debug('request_statistics received')
            return_dict = {}
            request_data = request.get_data()
            # 若无参数，则返回所有日志来源的全部数据
            if request_data == b'':
                return_dict['statistics error'] = STATISTICS_ERROR
                return_dict['statistics warn'] = STATISTICS_WARN
                return_dict['statistics normal'] = STATISTICS_NORMAL
                return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)
            # 若包含参数，则返回指定日志来源的数据
            else:
                # 获取传入参数
                request_data = demjson.decode(request_data)
                return_dict['statistics error'] = [n for n in STATISTICS_ERROR if
                                                   n['source'] == request_data['source']
                                                   and n['machine'] == request_data['machine']]
                return_dict['statistics warn'] = [n for n in STATISTICS_ERROR if
                                                  n['source'] == request_data['source']
                                                  and n['machine'] == request_data['machine']]
                return_dict['statistics normal'] = [n for n in STATISTICS_ERROR if
                                                    n['source'] == request_data['source']
                                                    and n['machine'] == request_data['machine']]
                return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)

        # 请求日志条目
        @app.route(MY_URL + 'request_data', endpoint='request_data', methods=['GET'])
        def request_detail():
            # 请求：{"machine": {"source":[id1, id2]}}
            # 返回值：{"machine": {"source":{"id": "data"}}}
            logging.debug('request_data received')
            return_dict = {}
            # 判断参数是否为空
            if request.args is None:
                return_dict['msg'] = '请求参数为空'
                return json.dumps(return_dict, ensure_ascii=False)
            # 获取传入参数
            return_dict = self.GetData.get_data(demjson.decode(request.get_data()))
            # GET_DATA_INPUT = demjson.decode(request.get_data())
            return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)

        # 请求分析数据
        @app.route(MY_URL + 'request_analyse_data', endpoint='request_analyse_data', methods=['GET'])
        def request_detail():
            logging.debug('request_analyse_data request received')
            return_dict = {}
            return_dict = ANALYSE_DATA
            return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)

        # 存入分析数据
        @app.route(MY_URL + 'save_analyse_data', endpoint='save_analyse_data', methods=['POST'])
        def request_detail():
            global ANALYSE_DATA
            logging.debug('save_analyse_data received')
            return_dict = {}
            # 判断参数是否为空
            if request.args is None:
                return_dict['success'] = 0
                return_dict['msg'] = '请求参数为空'
                return json.dumps(return_dict, ensure_ascii=False)
            # 获取传入参数
            ANALYSE_DATA = ast.literal_eval(demjson.decode(request.get_data()))
            return_dict['success'] = 1
            return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)

        app.run(threaded=True, processes=True, host='0.0.0.0', port=5000, debug=False, use_reloader=False)


def main(argv):
    ################# 自定义参数 #################
    # 数据库的地址及端口
    db_ip = ''
    db_user = ''
    db_password = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["help", "db_ip=", 'db_user=', 'db_password='])
    except getopt.GetoptError:
        print('usage:')
        print('cleaner.py --db_ip <db_ip> --db_user <db_user> --db_password <db_password>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print('usage:')
            print('cleaner.py --db_ip <db_ip> --db_user <db_user> --db_password <db_password>')
            sys.exit()
        elif opt == "--db_ip":
            db_ip = str(arg)
        elif opt == "--db_user":
            db_user = str(arg)
        elif opt == "--db_password":
            db_password = str(arg)
    # 检查参数完整性
    if db_ip == '' or db_user == '' or db_password == '':
        print('invaild input')
        print('usage:')
        print('cleaner.py --db_ip <db_ip> --db_user <db_user> --db_password <db_password>')
        sys.exit()

    GetData = GetDataFromMongoDB(db_ip, db_user, db_password)

    # 多线程上锁
    threadLock = threading.Lock()

    thread_GetStatisticsFromMongoDB = GetStatisticsFromMongoDB(db_ip, db_user, db_password, threadLock)
    thread_LocalAPI = LocalAPI(GetData, threadLock)

    thread_GetStatisticsFromMongoDB.start()
    thread_LocalAPI.start()


if __name__ == '__main__':
    main(sys.argv[1:])
