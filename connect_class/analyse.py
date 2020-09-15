#!/usr/bin/python3
# coding:utf-8

'''
数据分析函数

功能：
被日志分析程序调用，生成告警间隔统计图，告警日志时序图所需的数据
'''

import pandas as pd
import numpy as np
import scipy.stats as st
from statsmodels.tsa.stattools import adfuller
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import copy
from statsmodels.stats.diagnostic import acorr_ljungbox
import seaborn as sns
from statsmodels.graphics.tsaplots import plot_acf, acf
import warnings
import statsmodels as sm
import matplotlib

import time

# ***************************************
# 准备工作函数
# ***************************************


# 1.平稳性检测
def adf_test(ts):
    print(len(ts))
    print(ts)
    adftest = adfuller(ts)
    adf_res = pd.Series(adftest[0:4], index=['Test Statistic', 'p-value', 'Lags Used', 'Number of Observations Used'])
    for key, value in adftest[4].items():
        adf_res['Critical Value (%s)' % key] = value
    return (adf_res)


# 2.差分函数
def generatedata(g):
    f = len(g)
    df = pd.DataFrame(pd.DataFrame({'Quantities': g}, index=map(str, range(f))))
    return df


def diff_getdata(array):  # 实际使用的函数，直接获取差分结果
    arrayA = generatedata(array)
    diffarray = arrayA.diff().dropna()
    diffarray1 = np.array(diffarray)
    diffarray1 = list(map(float, diffarray1))
    return diffarray1


# 3、差分还原函数
def dediff(num, array):
    b = []
    result = num
    for i in range(len(array)):
        result = result + array[i]
        b.append(result)
    b = np.hstack((num, b))
    return b


# 4、数据分类函数。返回值为某类中的某个元素对应原列表中的序号，以及该元素的值。
def threshold_cluster(Data_set, threshold):
    stand_array = np.asarray(Data_set).ravel('C')
    stand_Data = pd.Series(stand_array)
    index_list, class_k = [], []
    while stand_Data.any():
        if len(stand_Data) == 1:
            index_list.append(list(stand_Data.index))
            class_k.append(list(stand_Data))
            stand_Data = stand_Data.drop(stand_Data.index)
        else:
            class_data_index = stand_Data.index[0]
            class_data = stand_Data[class_data_index]
            stand_Data = stand_Data.drop(class_data_index)
            if (abs(stand_Data - class_data) <= threshold).any():
                args_data = stand_Data[abs(stand_Data - class_data) <= threshold]
                stand_Data = stand_Data.drop(args_data.index)
                index_list.append([class_data_index] + list(args_data.index))
                class_k.append([class_data] + list(args_data))
            else:
                index_list.append([class_data_index])
                class_k.append([class_data])
    return index_list, class_k


# 5、数据分类函数，分类间隔为数据的一阶原点矩的proportion倍，例如4/5倍。返回值为某类中的某个元素对应原列表中的序号，以及该元素的值。
def classify_FOM(Data_set, proportion):
    avg = np.mean(Data_set)
    fir_order_moment = np.mean(abs(Data_set - avg))
    threshold = fir_order_moment * proportion
    stand_array = np.asarray(Data_set).ravel('C')
    stand_Data = pd.Series(stand_array)
    index_list, class_k = [], []
    while stand_Data.any():
        if len(stand_Data) == 1:
            index_list.append(list(stand_Data.index))
            class_k.append(list(stand_Data))
            stand_Data = stand_Data.drop(stand_Data.index)
        else:
            class_data_index = stand_Data.index[0]
            class_data = stand_Data[class_data_index]
            stand_Data = stand_Data.drop(class_data_index)
            if (abs(stand_Data - class_data) <= threshold).any():
                args_data = stand_Data[abs(stand_Data - class_data) <= threshold]
                stand_Data = stand_Data.drop(args_data.index)
                index_list.append([class_data_index] + list(args_data.index))
                class_k.append([class_data] + list(args_data))
            else:
                index_list.append([class_data_index])
                class_k.append([class_data])
    return index_list, class_k


# 6、数据处理函数。输入为秒数向量second和次数向量times，二者相同位置相对应，表示某一时刻（精确到秒）发生多少次。
# proportion表示分类的比例，默认使用一阶原点矩作为分类的间距。
# 最终输出5个矩阵，分别为：class_times表示分好类之后每一类的每个时刻点的次数，class_second和class_times对应，表示
# 相应位置次数发生的时刻； class_times_avg表示每一类的平均发生次数，class_times_num表示每一类内时刻点的个数
def classify_ultimate(times, second, proportion):
    index_list, class_times = classify_FOM(times, proportion)
    class_second = copy.deepcopy(class_times)
    for i in range(len(index_list)):
        for j in range(len(index_list[i])):
            class_second[i][j] = second[index_list[i][j]]

    class_times_avg = np.zeros(len(class_times))
    class_times_num = np.zeros(len(class_times))
    for k in range(len(class_times)):
        class_times_avg[k] = np.mean(class_times[k])
        class_times_num[k] = len(class_times[k])

    class_second_interval = []
    for s in range(len(class_second)):
        class_second_interval.append(diff_getdata(class_second[s]))

    return (class_times, class_second, class_times_avg, class_times_num, class_second_interval)


# 7、寻找异常值。使用随机森林方法寻找异常值。绘图并返回异常值列表。其中，要求异常值至少要大于次数的众数。
def abnormal_find(times, second, accuracy):
    times_trans = times.reshape(-1, 1)
    model = IsolationForest(contamination=accuracy)
    model.fit(times_trans)
    abnormal_judge = model.predict(times_trans)
    data_second_times = pd.DataFrame({'Second': second, 'Times': times})
    data_second_times['abnor_judge'] = abnormal_judge
    abnor_matrix = data_second_times.loc[(data_second_times['abnor_judge'] == -1) & (
                data_second_times['Times'] > st.mode(data_second_times['Times'])[0][0]), ['Second', 'Times']]
    plt.figure(figsize=(16, 8))
    plt.plot(data_second_times['Second'], data_second_times['Times'], color='blue', label='正常值')
    plt.scatter(abnor_matrix['Second'], abnor_matrix['Times'], color='red', label='异常值')
    plt.legend()
    return (data_second_times)


# 8、自相关性分析。对通过#5分好的每一类的时间的差分结果（class_second_interval）进行自相关性分析（线性），输出强自相关性
# （相关系数大于等于0.5）的时间差t和对应的相关系数。
def acf_examine(data):
    data_used = np.array(data)
    acf_data = acf(data_used)

    acf_strong_number = []
    acf_strong_data = []
    for i in range(len(acf_data)):
        if abs(acf_data[i]) >= 0.5:
            acf_strong_number.append(i)
            acf_strong_data.append(acf_data[i])

    plot_acf(data_used)
    return (acf_strong_number, acf_strong_data)


# 9、遍历所有的分布，找到最合适的（！！！！！！！！！！数据量庞大的时候慎用，要算很长时间）
def best_fit_distribution(data, bins=200, ax=None):
    """Model data by finding best fit distribution to data"""
    # Get histogram of original data
    y, x = np.histogram(data, bins=bins, density=True)
    x = (x + np.roll(x, -1))[:-1] / 2.0

    # Distributions to check
    DISTRIBUTIONS = [
        st.alpha, st.expon, st.chi2
    ]

    # Best holders
    best_distribution = st.norm
    best_params = (0.0, 1.0)
    best_sse = np.inf

    # Estimate distribution parameters from data
    for distribution in DISTRIBUTIONS:

        # Try to fit the distribution
        try:
            # Ignore warnings from data that can't be fit
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')

                # fit dist to data
                params = distribution.fit(data)

                # Separate parts of parameters
                arg = params[:-2]
                loc = params[-2]
                scale = params[-1]

                # Calculate fitted PDF and error with fit in distribution
                pdf = distribution.pdf(x, loc=loc, scale=scale, *arg)
                sse = np.sum(np.power(y - pdf, 2.0))

                # if axis pass in add to plot
                try:
                    if ax:
                        pd.Series(pdf, x).plot(ax=ax)
                except Exception:
                    pass

                # identify if this distribution is better
                if best_sse > sse > 0:
                    best_distribution = distribution
                    best_params = params
                    best_sse = sse

        except Exception:
            pass

    return (best_distribution, best_params)


# 10、输入分布名称（st.name）和参数，给出长度为10000的平滑概率密度曲线。
def make_pdf(dist, params, size=10000):
    print('dist = ', dist)
    """Generate distributions's Probability Distribution Function """

    # Separate parts of parameters
    arg = params[:-2]
    loc = params[-2]
    scale = params[-1]

    # Get sane start and end points of distribution
    start = dist.ppf(0.01, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.01, loc=loc, scale=scale)
    end = dist.ppf(0.99, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.99, loc=loc, scale=scale)

    # Build PDF and turn into pandas Series
    x = np.linspace(start, end, size)
    y = dist.pdf(x, loc=loc, scale=scale, *arg)
    pdf = pd.Series(y, x)

    return pdf


# 11、简易版本的分布拟合，只能用指数分布“expon”和卡方分布“chi2”这两种。输出拟合参数以及平滑的概率密度函数曲线。
def dist_fitting_simple(data, standard):
    if standard == "expon":
        parameters = st.expon.fit(data)
        fitting_result = make_pdf(st.expon, parameters)
        plt.plot(fitting_result, color='k')
        sns.distplot(data)
        return (parameters, fitting_result)
    elif standard == "chi2":
        parameters = st.chi2.fit(data)
        fitting_result = make_pdf(st.chi2, parameters)
        plt.plot(fitting_result, color='k')
        sns.distplot(data)
        return (parameters, fitting_result)
    else:
        return ("No such Standard")

def main(input_df, source_name):

    # ***************************************
    # 数据导入和处理，并统计每秒发生的次数
    # ***************************************

    test_data = input_df
    data_used = test_data[['Date']][test_data['Source']==source_name]
    data_used['Times'] = 0
    data_list= data_used.groupby(['Date'],as_index=False).count()

    data_matrix = np.array(data_list).T

    date = data_matrix[0]
    times = data_matrix[1]

    # ***************************************
    # 使用随机森林算法寻找异常值，比例为数据总量的5%
    # ***************************************

    data_second_times = abnormal_find(times,date,0.05)

    # ***************************************
    # 对数据进行分类，同一时刻发生次数相近的时间点分为一类。
    # ***************************************

    class_times,class_second,class_times_avg,class_times_num,class_second_interval = classify_ultimate(times,date,4/5)

    print('finish')

    # 返回值：一次大爆发中小爆发的数量，大爆发的总数量，集中报错的间隔统计表，对集中报错的最好的拟合方式名，拟合结果
    return class_times_avg, class_times_num, data_second_times