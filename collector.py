#!/usr/bin/python3
# coding:utf-8

'''
日志收集程序

功能：
1. 提取所在节点/var/log/containers下的所有日志，以及宿主机日志（/var/log/messages）
2. 根据告警规则为日志标定告警级别
3. 将日志定时上传给日志分析程序

部署：
每个工作节点部署一个
必须与数据库，日志提取程序，日志分析程序部署在同一网络下

命令格式：   collector.py --local_name <local_name> --db_ip <db_ip> --db_user <db_user> --db_password <db_password>
示例命令：   nohup python3 collector.py --local_name m1 --db_ip 10.43.144.104 --db_user admin --db_password 123456 &

'''

import getopt
import os, sys, json, time
import logging

# 数据库连接类
from connect_class import db_connect

# 设置日志格式，等级等
LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(pathname)s %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a '  # 配置输出时间的格式，注意月份和天数不要搞乱了
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    filename=r"collector.log"  # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )


# 收集日志
class GetLog:
    # 初始化参数： 本机名，数据库ip，数据库用户，数据库密码，自定义日志位置， 告警判断选项，告警规则
    def __init__(self, local_name, db_ip, db_user, db_password, log_directory, judge, judge_rule):
        self.params = {}
        # 数据库参数(仅用于传参）
        self.params['local_name'] = local_name
        self.params['db_ip'] = db_ip
        self.params['db_user'] = db_user
        self.params['db_password'] = db_password

        self.judge = judge  # 告警判断选项，0为不判断1为判断
        self.judge_rule = judge_rule  # 告警规则

        self.log_directory = log_directory  # 自定义日志地址
        self.log_len = {}  # 日志长度

        self.verify_dict = {}  # 用于初始化时验证日志是否变动

        self.error = {}  # 标记监控程序是否出现错误

        # 初始化日志名称
        self.init_col_names()

        # 初始化日志条数
        for name in self.log_directory:
            self.log_len[name] = 0

        self.format_database()  # 格式化数据库

    # -----------------------------------------初始化数据-----------------------------------------
    # *******************************************************************************************

    # 获取日志目录名称
    def init_col_names(self):
        # 遍历/var/log/containers/，查找日志并添加日志目录
        for file_name in os.listdir(r'/var/log/containers/'):  # 不仅仅是文件，当前目录下的文件夹也会被认为遍历到
            new_dir = '/var/log/containers/' + file_name
            # 去除之前已经发现的文件以及临时文件
            if new_dir not in self.log_directory and new_dir[-4:] != '.swp':
                print("检测到日志文件: ", new_dir)
                self.log_directory.append(new_dir)
                self.log_len[new_dir] = 0

    # 初始化删库
    def format_database(self):
        conn = db_connect.DataBase(self.params['local_name'], self.params['db_user'], self.params['db_password'],
                                   self.params['db_ip'])
        conn._delete()
        conn._insert_init_time()

    # ------------------------------------------其它操作------------------------------------------
    # *******************************************************************************************

    # 检查日志是否变动
    def check_log(self):
        # 顺序检查每个日志文件
        delay = 0
        for seq in range(len(self.log_directory)):
            local_log = []
            try:
                with open(self.log_directory[seq - delay], 'r+') as file:
                    # 检查行数，看有无新增日志条目
                    count = 0
                    for index, line in enumerate(open(self.log_directory[seq - delay], 'r', errors='ignore')):
                        count += 1
                    diff = count - self.log_len[self.log_directory[seq - delay]]
                    # 如果无新增，结束函数
                    if diff == 0:
                        pass
                    # 如果出现新增，则追加日志
                    elif diff > 0:
                        local_log = self.get_log(seq - delay, 1)
                    # 如果出现减少，则默认日志被重置，读取全部日志
                    elif diff < 0:
                        local_log = self.get_log(seq - delay, 0)
                    # 对新日志进行判决并上传
                    if local_log:
                        self.judge_log(seq - delay, local_log)
                        # 完成全部操作后，更新日志行数
                        if self.error[str(self.log_directory[seq - delay])] == 0:
                            self.log_len[self.log_directory[seq - delay]] = count
            except FileNotFoundError:
                print('%s no longer exists on this machine!')
                del self.log_directory[seq - delay]
                delay += 1

    # 收集日志，seq为检索到的文件序号，option=0 时收集所有条目，option=1 时仅收集新生成的条目
    def get_log(self, seq, option):
        local_log = []
        if option == 0:
            with open(self.log_directory[seq], 'r+') as file:
                for line in file:
                    local_log.append(line.strip('\n'))

        elif option == 1:
            with open(self.log_directory[seq], 'r+') as file:
                cnt = 0
                for line in file:
                    if cnt >= self.log_len[self.log_directory[seq]]:
                        local_log.append(line.strip('\n'))
                    cnt += 1
        return local_log

    # 判断日志紧急与否并上传日志, 参数为检索到的日志序号和待上传的日志条目列表
    def judge_log(self, seq, local_log):
        # 初始化
        urgent_class_list = []  # 初始化单条日志的告警级别，0代表正常，1代表警告，2代表故障
        id_list = []  # 初始化日志id
        cnt = 0
        # 生成日志id
        for i in range(len(local_log)):
            urgent_class_list.append(0)
            id_list.append(str(time.time()) + str(cnt))
            cnt += 1

        # 进行告警判断
        for i in range(len(local_log)):  # 按行判断
            # 告警判断
            # 1) 故障
            for rule in self.judge_rule['error']:
                if local_log[i].count(str(rule)):
                    # 标记故障条目
                    urgent_class_list[i] = 2
            # 2) 警告
            if urgent_class_list[i] == 0:
                for rule in self.judge_rule['warn']:
                    if local_log[i].count(str(rule)):
                        # 标记告警条目
                        urgent_class_list[i] = 1

        # 把收集到的日志都丢到数据库里面
        self.upload_log(seq, local_log, urgent_class_list, id_list)

    # 上传日志到数据库，参数为检索到的日志序号，待上传的日志条目列表，告警级别
    def upload_log(self, seq, local_log, urgent_class_list, id_list):
        conn = db_connect.DataBase(self.params['local_name'], self.params['db_user'], self.params['db_password'],
                                   self.params['db_ip'], 0)
        upload_col_names = []
        # 上传日志，将主机名+文件名作为表名
        # 此处需控制地址长度在120字节内，以便作为数据库表名
        if len(self.log_directory[seq]) > 40:
            result = conn._insert(self.log_directory[seq][20:50] + str(hash(self.log_directory[seq][50:]))[:10],
                                local_log, urgent_class_list, id_list)
        else:
            result = conn._insert(self.log_directory[seq], local_log, urgent_class_list, id_list)
        # 上传情况报告
        if result:
            self.error[str(self.log_directory[seq])] = 1
        else:
            self.error[str(self.log_directory[seq])] = 0

def main(argv):
    ################# 自定义参数 #################
    # 本机名称（或ip地址）
    local_name = ''
    # 数据库的地址及端口
    db_ip = ''
    db_user = ''
    db_password = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["help", "local_name=", "db_ip=", 'db_user=', 'db_password='])
    except getopt.GetoptError:
        print('usage:')
        print('collector.py --local_name <local_name> --db_ip <db_ip> --db_user <db_user> --db_password <db_password>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print('usage:')
            print(
                'collector.py --local_name <local_name> --db_ip <db_ip> --db_user <db_user> --db_password <db_password>')
            sys.exit()
        elif opt == "--local_name":
            local_name = str(arg)
        elif opt == "--db_ip":
            db_ip = str(arg)
        elif opt == "--db_user":
            db_user = str(arg)
        elif opt == "--db_password":
            db_password = str(arg)
    # 检查参数完整性
    if local_name == '' or db_ip == '' or db_user == '' or db_password == '':
        print('invaild input')
        print('usage:')
        print('collector.py --local_name <local_name> --db_ip <db_ip> --db_user <db_user> --db_password <db_password>')
        sys.exit()

    # 需要提取的日志的绝对地址
    log_directory = ['/var/log/messages']

    # 日志收集选项
    time_lag = 1  # 单次查询的时间间隔，单位为秒。间隔越小对系统压力越大
    judge = 1  # 告警判断选项，0为不判断1为判断

    # 告警规则，这是一个字典，字典的索引为故障级别，第二项为告警关键字
    judge_rule = {'error': ['error', 'Error', 'ERROR', 'fail', 'fatal', 'Fatal', 'Critical', 'critical', 'CRITICAL'],
                  "warn": ['Warn', 'warn']}

    ################# 开始循环监测 #################

    # 初始化收集日志
    log_get = GetLog(local_name, db_ip, db_user, db_password, log_directory, judge, judge_rule)
    # 主循环
    while 1:
        # 定时检查日志
        log_get.init_col_names()
        log_get.check_log()
        time.sleep(time_lag)


if __name__ == '__main__':
    main(sys.argv[1:])
