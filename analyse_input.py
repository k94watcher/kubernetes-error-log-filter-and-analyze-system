#!/usr/bin/python3
# coding:utf-8

'''
日志分析程序

功能：
从日志处理程序中提取统计数据，并返回分析数据
分析数据具体为两张表的参数：告警间隔统计图，告警日志时序图

部署：
只需部署一个即可

命令格式：   analyse_input.py --cleaner_ip <cleaner_ip> --cleaner_port <cleaner_port>
示例命令：   nohup python3 analyse_input.py --cleaner_ip xxx.xxx.xxx.xxx --cleaner_port 5000 &

'''

import getopt, sys
import requests
import time
import json
import demjson
import pandas as pd
import datetime
from connect_class import analyse

# 全局变量
SOURCE_NAME_DICT = {}

# 重定义datetime格式的编码方法
# 如果没有这一段，则会出现错误：TypeError: Object of type datetime is not JSON serializable
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self,obj)


class LocalApi:
    # 输入参数为：上传url，下载url，自定义查询语句
    def __init__(self, cleaner_ip, cleaner_port):
        self.cleaner_dir = 'http://' + cleaner_ip + ':' + str(cleaner_port)

    # 主函数，每5分钟最多运行1次
    def run(self):
        while 1:
            print('start analysing')
            start_time = time.time()
            return_list = []
            input_df, enough_data = self.get_detail_data()
            for source_name in SOURCE_NAME_DICT:
                temp_dict = {}
                temp_dict['source'] = SOURCE_NAME_DICT[source_name]['source']
                temp_dict['machine'] = SOURCE_NAME_DICT[source_name]['machine']
                # 返回值：一次大爆发中小爆发的数量，大爆发的总数量，集中报错的间隔统计表，对集中报错的最好的拟合方式名，拟合结果
                class_times_avg, class_times_num, data_second_times = \
                    analyse.main(input_df, source_name)
                temp_dict['class_times_avg'] = class_times_avg.tolist()
                temp_dict['class_times_num'] = class_times_num.tolist()
                temp_dict['data_second_times'] = data_second_times.to_dict()
                return_list.append(temp_dict)
            self.save_data(return_list)
            while time.time() - start_time < 300:
                time.sleep(1)

    # 获取统计数据
    def get_statistics_data(self):
        url = self.cleaner_dir + '/cleaner/request_statistics'
        headers = {'Content-Type': 'application/json'}
        result = requests.get(url, headers=headers)  # 发送查询

    # 获取详细数据
    def get_detail_data(self):
        global SOURCE_NAME_DICT
        enough_data = 0
        url = self.cleaner_dir + '/cleaner/request_detail_for_analyse'
        headers = {'Content-Type': 'application/json'}
        result = requests.get(url, headers=headers)  # 发送查询

        target_list = []
        cnt = 0
        for data in result.json()['msg']:
            temp_dict = {}
            temp_dict['Date'] = datetime.datetime.strptime(data['time'], "%Y-%m-%d %H:%M:%S")
            temp_dict['Source'] = data['machine'] + ' ' + data['source']
            SOURCE_NAME_DICT[temp_dict['Source']] = {'source': data['source'], 'machine': data['machine']}
            target_list.append(temp_dict)
            cnt += 1
        if cnt > 1000:
            enough_data = 1
        result_df = pd.DataFrame.from_dict(target_list)
        return result_df, enough_data


    # 保存数据
    def save_data(self, return_list):
        request_dict = {}
        request_dict['data'] = return_list
        request_dict = json.dumps(request_dict, cls=DateEncoder, ensure_ascii=False)
        url = self.cleaner_dir + '/cleaner/save_analyse_data'
        headers = {'Content-Type': 'application/json'}
        result = requests.post(url, headers=headers, data=json.dumps(request_dict, cls=DateEncoder, ensure_ascii=False))  # 发送查询


def main(argv):
    cleaner_ip = ''
    cleaner_port = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["help", "cleaner_ip=", 'cleaner_port='])
    except getopt.GetoptError:
        print('usage:')
        print('analyse_input.py --cleaner_ip <cleaner_ip> --cleaner_port <cleaner_port>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print('usage:')
            print('analyse_input.py --cleaner_ip <cleaner_ip> --cleaner_port <cleaner_port>')
            sys.exit()
        elif opt == "--cleaner_ip":
            cleaner_ip = str(arg)
        elif opt == "--cleaner_port":
            cleaner_port = int(arg)
    # 检查参数完整性
    if cleaner_ip == '' or cleaner_port == '':
        print('invaild input')
        print('usage:')
        print('analyse_input.py --cleaner_ip <cleaner_ip> --cleaner_port <cleaner_port>')
        sys.exit()

    api = LocalApi(cleaner_ip, cleaner_port)
    api.run()



if __name__ == '__main__':
    main(sys.argv[1:])
