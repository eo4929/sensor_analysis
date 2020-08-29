import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import numpy as np
import pandas as pd
import multiprocessing as mp
from multiprocessing import Pool
import csv
import gc
import time
import folium
from scipy.stats import pearsonr

num_cores = mp.cpu_count()
#print(num_cores) # 16

def parallel_dataframe(df, func):
    df_split = np.array_split(df, num_cores)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

'''
s = time.time()
df1 = pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/new_refined_data1_2019_2020_with_loc.csv') # 결론: 이게 더 빠르다 그냥 읽어서 하자 어차피 1분 정도라면
e = time.time()
print("Pandas Loading Time = {}".format(e-s))

'''

'''
s = time.time()
Lst = []
for ck in pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/new_refined_data1_2019_2020_with_loc.csv', chunksize=1000000):
    Lst.append(ck)
df2 = pd.concat(Lst)
e = time.time()
print("Pandas Loading Time = {}".format(e-s))

print(df2)
'''

#df1 = pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/new_refined_data1_2019_2020_with_loc.csv')
#print(df1)

#df_chunk = pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/new_refined_data1_2019_2020_with_loc.csv', iterator=True, chunksize=1000000)
#df1 = pd.concat([chunk for chunk in df_chunk])
#print(df1)


#ck1 = pd.DataFrame(columns=['Year','Month','Day','Mdate','Time_hour','Time_minute','DurationMinutes',
#                            'StreetId','StreetName','bay_id','In Violation','Vehicle Present','latitude','longitude'])

import matplotlib.pyplot as plt

class Data1():
    def __init__(self):
        self.df1 = None
        self.lst_loc = None

    def make_lst_loc(self):
        lst = []
        df1_lat = self.df1['Latitude'].unique()
        df1_loc = self.df1['Longitude'].unique()
        for i in range(len(df1_lat)):
            lst.append([df1_lat[i], df1_loc[i]])
        # print(lst)
        self.lst_loc = lst

    def load(self):
        self.df1 = pd.read_csv(
            'C:/Users/Dae-Young Park/sensor_dataset/refined_data_v4/new_data1_2019_nov_15_2020_may_25_vehicle_present.csv')
        self.df1['Total_date'] = pd.to_datetime(self.df1['Total_date'])
        self.make_lst_loc()

    def load_idx_datetime(self):
        self.df1 = pd.read_csv(
            'C:/Users/Dae-Young Park/sensor_dataset/refined_data_v4/new_data1_2019_nov_15_2020_may_25_vehicle_present.csv')
        self.df1['Total_date'] = pd.to_datetime(self.df1['Total_date'])
        self.df1 = self.df1.set_index('Total_date')  # 오케이 일단, 시계열로 분석할 수 있게 해놓았음 --> 이제 비쥬얼 툴로 화면에 뿌려보자
        self.make_lst_loc()

    def get_df1(self):
        return self.df1.copy()

    def describe_statistics(self):
        ds = self.df1.describe(include='all')
        return ds.copy()
    def get_sel_columns(self, lst_col):
        new_df = self.df1.loc[:, lst_col]
        return new_df.copy()
    def add_map_circle(self, map_object):
        for i in range(len(self.lst_loc)):
            folium.Circle(
                location=self.lst_loc[i],
                radius=50,
                color='blue',
                fill='crimson',
            ).add_to(map_object)
    def add_map_marker(self,map_object):
        for i in range(len(self.lst_loc)):
            folium.Marker(
                location=self.lst_loc[i],
                icon=folium.Icon(color='blue',icon='star')
            ).add_to(map_object)

class Data2():
    def __init__(self):
        self.df2 = None
        self.lst_loc = None
    def make_lst_loc(self):
        lst = []
        df2_lat = self.df2['Latitude'].unique()
        df2_loc = self.df2['Longitude'].unique()
        for i in range(len(df2_lat)):
            lst.append( [df2_lat[i],df2_loc[i]] )
        #print(lst)
        self.lst_loc = lst
    def load(self):
        self.df2 = pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/refined_data_v4/new_data2_2019_nov_15_2020_may_25.csv')
        self.df2['Total_date'] = pd.to_datetime(self.df2['Total_date'])
        self.make_lst_loc()
    def load_idx_datetime(self):
        self.df2 = pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/refined_data_v4/new_data2_2019_nov_15_2020_may_25.csv')
        self.df2['Total_date'] = pd.to_datetime(self.df2['Total_date'])
        self.df2 = self.df2.set_index('Total_date') # 오케이 일단, 시계열로 분석할 수 있게 해놓았음 --> 이제 비쥬얼 툴로 화면에 뿌려보자
        self.make_lst_loc()
    def get_df2(self):
        return self.df2.copy()
    def describe_statistics(self):
        ds = self.df2.describe(include= 'all' )
        return ds.copy()
    #def visual_num_data_year(self):
    #    return
    def get_sel_columns(self,lst_col):
        new_df = self.df2.loc[:,lst_col]
        return new_df.copy()
    #def set_index_with_datetime(self): 정확한 이유는 모르겠지만 main에서 직접 string을 datetime 타입으로 바꿔져야겠군
    #    self.df2['Total_date'] = pd.to_datetime(self.df2['Total_date'])
    #    self.df2.set_index('Total_date',inplace=False)
    #    return
    def add_map_circle(self,map_object):
        for i in range(len(self.lst_loc)):
            folium.Circle(
                location=self.lst_loc[i],
                radius=50,
                color='green',
                fill='crimson',
            ).add_to(map_object)
    def add_map_marker(self,map_object):
        for i in range(len(self.lst_loc)):
            folium.Marker(
                location=self.lst_loc[i],
                icon=folium.Icon(color='green',icon='star')
            ).add_to(map_object)

class Data3():
    def __init__(self):
        self.df3 = None
        self.lst_loc = None
    def make_lst_loc(self):
        lst = []
        df3_lat = self.df3['Latitude'].unique()
        df3_loc = self.df3['Longitude'].unique()
        for i in range(len(df3_lat)):
            lst.append([df3_lat[i], df3_loc[i]])
        #print(lst)
        self.lst_loc = lst
    def load(self):
        self.df3 = pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/refined_data_v4/new_data5_2019_nov_15_2020_may_25.csv')
        self.df3['Total_date'] = pd.to_datetime(self.df3['Total_date'])
        self.make_lst_loc()
    def load_idx_datetime(self):
        self.df3 = pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/refined_data_v4/new_data5_2019_nov_15_2020_may_25.csv')
        self.df3['Total_date'] = pd.to_datetime(self.df3['Total_date'])
        self.df3 = self.df3.set_index('Total_date')
        self.make_lst_loc()
    def get_df3(self):
        return self.df3.copy()
    def describe_statistics(self):
        ds = self.df3.describe(include= 'all' )
        return ds.copy()
    def get_sel_columns(self,lst_col):
        new_df = self.df3.loc[:,lst_col]
        return new_df.copy()
    def add_map_circle(self,map_object):
        for i in range(len(self.lst_loc)):
            folium.Circle(
                location=self.lst_loc[i],
                radius=50,
                color='red',
                fill='crimson',
            ).add_to(map_object)

# 유동인구 기준으로 특이점 보기 위해, attr1: 주차 차량 수, attr2: 보행자 수, x축은 시계열
def show_count_information(df1, df2):
    '''
    df1 = df1.groupby('Total_date').size()
    df2 = df2.loc[:,['Hourly_Counts','Total_date']]
    df2 = df2.groupby(by=["Total_date"]).sum()
    df1 = df1.to_frame('The number of on-street parked cars')
    df1.to_csv('.new_df1.csv')
    df2.to_csv('.new_df2.csv')
    '''

    new_df1 = pd.read_csv('./new_df1_count_per_date.csv')
    new_df2 = pd.read_csv('./new_df2_count_per_date.csv')
    new_df2 = new_df2.rename( {'Hourly_Counts': 'The number of pedestrians'}, axis='columns')

    df3 = new_df1.join(new_df2.set_index('Total_date'), on='Total_date')
    #print(df3)
    df3['Total_date'] = pd.to_datetime(new_df1['Total_date'])
    df3= df3.set_index('Total_date')
    ax = df3.plot()
    plt.ylim(bottom=0)
    plt.ylim(top=1000000)
    plt.xlabel("Time")
    plt.ylabel("Count(Million)")
    plt.show()

from datetime import date
import matplotlib.dates as mdates

def show_count_pedestrians_on_days(str_day):

    df_count_per_date_plus_days = open('./new_df2_count_per_date_plus_days.csv', 'r', encoding='utf-8')
    rdr_df_count_per_date_plus_days = csv.DictReader(df_count_per_date_plus_days)

    with open('./new_df2_count_per_date_{0}.csv'.format(str_day),'w', encoding='utf-8', newline='') as new_csv:
        fieldnames = ['Total_date','The number of pedestrians']
        writer = csv.DictWriter(new_csv, fieldnames=fieldnames)
        writer.writeheader()

        for i, line in enumerate(rdr_df_count_per_date_plus_days):
            if line['Day'] == str_day:
                writer.writerow({ 'Total_date': line['Total_date'], 'The number of pedestrians': line['Hourly_Counts']
                             })

    df = pd.read_csv('./new_df2_count_per_date_{0}.csv'.format(str_day))
    df['Total_date'] = pd.to_datetime(df['Total_date'])
    df = df.set_index('Total_date')
    #ax = df.plot.bar()
    ax = df.set_index(df.index.strftime('%Y-%m-%d')).plot.bar()
    #plt.rcParams["date.autoformatter.day"] = "%Y-%m-%d"
    #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xlabel("Date")
    plt.ylabel("Count")
    plt.show()

def show_correlation(df1, df2):
    new_df1 = pd.read_csv('./new_df1_count_per_date.csv')
    new_df2 = pd.read_csv('./new_df2_count_per_date.csv')
    col1 = new_df1['The number of on-street parked cars']
    col2 = new_df2['Hourly_Counts']
    res = col1.corr(col2)
    print(res)

    plt.scatter(col1,col2)
    plt.xlabel("The number of on-street parked cars")
    plt.ylabel("The number of pedestrians(Million)")
    plt.show()

def show_pValue():
    df1_df2 = pd.read_csv('./new_df1_df2_count_per_date.csv')
    print(df1_df2)
    def calculate_pvalues(df):
        df = df._get_numeric_data()
        dfcols = pd.DataFrame(columns=df.columns)
        pvalues = dfcols.transpose().join(dfcols, how='outer')
        for r in df.columns:
            for c in df.columns:
                if c == r:
                    df_corr = df[[r]].dropna()
                else:
                    df_corr = df[[r, c]].dropna()
                pvalues[r][c] = pearsonr(df_corr[r], df_corr[c])[1]
        return pvalues
    p_values = calculate_pvalues(df1_df2)
    print(p_values)

def show_fiveSummary_timezone():
    def show_outlier(data):
        mask1 = (data['00:00-05:59'] > 40000)
        outlier_data = data.loc[mask1]
        print(outlier_data)
    data = pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/refined_data_v4/filled_for_five_summary.csv')
    data = data.set_index('Total_date')

    #desc = data.describe(include='all')
    #print(desc)

    show_outlier(data)

    data.plot.box()
    plt.title("Box plot of the number of pedestrian on each Time Zone")
    plt.xlabel("Time Zone")
    plt.ylabel("The number of pedestrian")
    plt.show()

def show_fiveSummary_days():
    pd_fiveSummary_days = pd.read_csv('./for_five_summary_days.csv')
    pd_fiveSummary_days = pd_fiveSummary_days.set_index('Total_date')

    pd_fiveSummary_days.plot.box()
    plt.title("Box plot of the number of pedestrian on each day")
    plt.xlabel("Time Zone")
    plt.ylabel("The number of pedestrian(Million)")
    plt.show()

if __name__ == "__main__":
    show_fiveSummary_days()
    #show_fiveSummary_timezone()
    #show_count_pedestrians_on_days()
    #data = Data1()
    #data.load_idx_datetime()
    #data.load()
    #df1 = data.get_df1()

    '''
    map_object = folium.Map(
        location=[-37.8136, 144.963], # melbourne location
        zoom_start=14
    )
    '''

    #data.add_map_circle(map_object)
    #data.add_map_marker(map_object)

    #data2 = Data2()
    #data2.load_idx_datetime()
    #data2.load()
    #df2 = data2.get_df2()
    #data2.add_map_circle(map_object)
    #data2.add_map_marker(map_object)

    #map_object.save('map_data_1_2_marked.html')

    #show_count_information(df1,df2)
    #show_correlation(df1,df2)
    #show_count_pedestrians_on_days('Sunday')
    #show_pValue()


    '''
    data3 = Data3()
    data3.load_idx_datetime()
    data3.add_map_circle(map_object)

    df4 = pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/Super_Sunday_Bike_Count.csv')
    lst = []
    df4_lat = df4['latitude'].unique()
    df4_loc = df4['longitude'].unique()
    for i in range(len(df4_lat)):
        lst.append([df4_lat[i], df4_loc[i]])
    for i in range(len(lst)):
        folium.Circle(
            location=lst[i],
            radius=50,
            color='black',
            fill='crimson',
        ).add_to(map_object)
    map_object.save('map_data_1_2_3_4.html')
    '''

    '''
    np.random.seed(0)
    df1 = pd.DataFrame(np.random.randn(100, 3),
                       index=pd.date_range('1/1/2018', periods=100),
                       columns=['A', 'B', 'C']).cumsum()
    print(df1)
    df1.plot()
    plt.title("Pandas")
    plt.xlabel("time")
    plt.ylabel("Data")
    plt.show()
    '''
