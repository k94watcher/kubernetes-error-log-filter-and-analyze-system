# coding:utf-8

'''
数据库连接函数

功能：
被日志提取和日志处理程序调用，操作数据库
'''

import json, time
from pymongo import MongoClient
from dateutil import parser
import gridfs



# 数据库操作
class DataBase:
    # 输入数据库名，用户，密码，ip，数据库选项（=0直接使用数据库，=1使用gridfs），检索选项（=1时检索数据库名）
    def __init__(self, db_name, usr, passwd, ip, use_gridfs = 0, check = 0):
        # 建立连接
        self.db_connect = MongoClient('mongodb://%s:%s@%s' % (usr, passwd, ip))
        if check == 1:
            return

        self.db = self.db_connect[db_name]
        self.db_name = db_name

        # 直接使用数据库
        if use_gridfs == 0:
            self.fs = 0

        # 使用gridfs
        elif use_gridfs == 1:
            self.fs = gridfs.GridFS(self.db)


    # 向指定的表插入数据
    # 输入的参数为：表名，待上传的数据列表，告警级别列表，日志id
    def _insert(self, col_name, data, urgent_class_list, id_list):
        # 检查格式是否正确
        if len(data) != len(urgent_class_list) != len(id_list):
            print('input error: all list must have same length!')
            return 1
        # 计时
        time_start = time.time()
        time_request = parser.parse(time.strftime("%Y-%m-%d %H:%M:%S"))
        upload_dict = []
        for seq in range(len(id_list)):
            # 创建数据包
            request_dict = {}
            request_dict['log id'] = id_list[seq]
            request_dict['time'] = time_request
            request_dict['urgent class'] = urgent_class_list[seq]
            request_dict['data'] = data[seq]
            # 保存
            upload_dict.append(request_dict)
        # 上传
        up = self.db[col_name].insert_many(upload_dict)
        time_stop = time.time()
        print(col_name)

        print('%s   |   %s lines inserted, %s sec used'%
              (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), len(data), time_stop-time_start))
        return 0

    # 保存初始化时间（程序启动时会用启动的时间表示日志生成时间，为了避免这些日期影响分析程序，所以要进行启动时间标注
    def _insert_init_time(self):
        # 首先，删除原来的数据
        self.db_connect['init_time']['init_time'].delete_many({"machine": self.db_name})
        # 开始写入
        time_request = parser.parse(time.strftime("%Y-%m-%d %H:%M:%S"))
        upload_dict = {}
        upload_dict['machine'] = self.db_name
        upload_dict['init time'] = time_request
        # 上传
        up = self.db_connect['init_time']['init_time'].insert(upload_dict)
        return 0

    # 使用gridfs存储文件，仅用于定期备份清洗程序的小数据库
    def _insert_large(self, data):
        # 检查能否使用gridfs
        if self.fs == 0:
            print('not allowed to use gridfs, exited...')
            return 1

        # 计时
        time_start = time.time()
        time_request = parser.parse(time.strftime("%Y-%m-%d %H:%M:%S"))

        # 准备将待上传的所有内容装在一个json内
        request_dict = {}
        request_dict['time'] = time_request
        request_dict['data'] = data
        request_dict = json.dumps(request_dict, ensure_ascii=False)

        # 编码
        request_dict = request_dict.encode('utf-8')

        # 上传
        up = self.fs.put(data=request_dict, filename=str(request_dict['time']))

        time_stop = time.time()

        print('%s   |   backup generated, %s sec used'%
              (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), len(data), time_stop-time_start))
        return 0


    # 请求数据，返回一个包含日志字典的数组。可以设置id进行精确检索
    def _request(self, col_name, id_list = []):
        # print('request begin: %s  %s'%(self.db_name, col_name))
        return_list = []
        # 返回指定id的数据
        if id_list:
            for id in id_list:
                return_list.append(self.db[col_name].find_one({"log id": id},{"_id": 0}))
        # 返回所有数据
        else:
            for data in self.db[col_name].find({},{"_id": 0}):
            # 将查询到的所有数据读入read_dict字典
                print(data)
                return_list.append(data)
        return return_list

    # 请求详细数据（只选取其中的100条）
    def _request_detail(self, col_name, urgent_class=-1, limit=100):
        # 查询的结果形如：{'log id': '1597621166.121952510', 'time': datetime.datetime(2020, 8, 17, 7, 39, 26), 'urgent class': 2}
        # 返回值是多个查询结果组成的列表
        # print('request begin: %s  %s' % (self.db_name, col_name))
        return_list = []
        # 选取所有对应告警级别的日志
        if urgent_class > -1:
            for data in self.db[col_name].find({'urgent class': urgent_class},
                                               {"_id": 0, 'data': 0}).sort([("time", -1)]):
                # 将查询到的所有数据读入read_dict字典
                return_list.append(data)
        # 选取指定数量日志
        elif limit > 0:
            for data in self.db[col_name].find({}, {"_id": 0, 'data':0}).sort([("time", -1)]).limit(limit):
                # 将查询到的所有数据读入read_dict字典
                return_list.append(data)
        # 选取所有日志
        else:
            for data in self.db[col_name].find({}, {"_id": 0, 'data':0}).sort([("time", -1)]):
                # 将查询到的所有数据读入read_dict字典
                return_list.append(data)
        return return_list

    # 请求统计数据
    def _request_statistics(self, col_name):

        # ***************
        # 进行查询，查询语句pipeline中group代表查询格式，'_id'表示查询的内容，'count'表示计数
        # 查询的结果格式为：{'_id': {'date': '2020-08-17-07', 'urgent class': 2}, 'count': 3}
        # 返回值是多个查询结果组成的列表
        # 使用详情可参阅：
        # https://www.docs4dev.com/docs/zh/mongodb/v3.6/reference/reference-operator-aggregation-dateToString.html#%E6%A0%BC%E5%BC%8F%E8%AF%B4%E6%98%8E%E7%AC%A6
        # https://www.yangyanxing.com/article/aggregate-perday-mongodb.html
        # ***************

        return_list = []
        pipeline = [
            {'$group': {
                '_id': {'date': {"$dateToString": {'format': '%Y-%m-%d-%H', 'date': '$time'}}, 'urgent class': '$urgent class'},
                'count': {'$sum': 1}}},
            {'$sort': {"_id": -1}}
        ]
        for result in self.db[col_name].aggregate(pipeline):
            return_list.append(result)
        return return_list

    # 查询初始化时间（程序启动时会用启动的时间表示日志生成时间，为了避免这些日期影响分析程序，所以要进行启动时间标注
    def check_init_time(self):
        # 存储的数据库为init_time
        return_dict = {}
        for data in self.db_connect['init_time']['init_time'].find({}, {"_id": 0}):
            return_dict[data['machine']] = data['init time']
        return return_dict

    # 删除数据库
    def _delete(self):
        self.db.command("dropDatabase")
        print('database %s deleted' % str(self.db_name))

    # 获取mongodb中的所有数据库名
    def get_db_names(self):
        return self.db_connect.list_database_names()

    # 获取当前数据库下的所有表名
    def get_col_names(self, db_name = ''):
        if db_name:
            return self.db_connect[db_name].list_collection_names()
        else:
            return self.db.list_collection_names()

    # 数据库改名
    def change_name(self, col_name, new_name):
        '''
        for col_name in self.get_col_names():
            self.db.command('{renameCollection: "%s.%s", to: "%s.%s"}'%(self.db_name, col_name, new_name, col_name))
        '''
        self.db[col_name].rename(new_name)

