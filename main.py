'''
필요한 칼럼들
data1: deviceid, durationseconds, streetmarker, area, streetid, streetname,
betweenstreet1id, betweenstreet1, betweenstreet2id, betweenstreet2, sideofstreet(-> 2019년 이후 sideofstreetcode, sidename
in_violation(overstay여부를 나타냄), vehicle_present(이건 뭔지 질문올렸음)

1 시공간과 사회적 맥락을 이해하는 데 도움이 안될 것 같은 칼럼들은 다 날려서 테이블당 크기를 줄이자
2 그다음 각 데이터들마다 내가 칼럼을 일일이 파싱하는 작업을 먼저하도록 하자
3 SQL처럼 뭔가 쿼리로 검색하고 싶을 경우 종종 있을테니 관련된 기능도 구현해놓자
'''
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import numpy as np
import pandas as pd
#from . import microclimate_sensor_types
import numpy as np
import multiprocessing as mp
from multiprocessing import Pool
import os



num_cores = mp.cpu_count()
print(num_cores) # 8
'''
def parallel_dataframe(df, func):
    df_split = np.array_split(df, num_cores)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df
'''

#pd.read_csv('C:/Users/Dae-Young Park/sensor_dataset/new_data2_2019_2020.csv',delimiter=',')

import matplotlib.pyplot as plt
'''
data1 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/On-street_Car_Parking_Sensor_Data_-_2014.csv',delimiter=',')
data1.shape
data1.columns
'''

'''
import csv
data1 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/On-street_Car_Parking_Sensor_Data_-_2014.csv', 'r', encoding='utf-8')
rdr_dt1 = csv.DictReader(data1)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data/new_data1_2014.csv', 'w',encoding='utf-8',newline='') as new_data1:
    fieldnames = ['ArrivalTime', 'DurationSeconds', 'StreetId', 'StreetName', 'BetweenStreet1', 'BetweenStreet2',
                  'In Violation', 'Vehicle Present']
    writer = csv.DictWriter(new_data1, fieldnames=fieldnames)
    writer.writeheader()

    for i,line in enumerate(rdr_dt1):
        if i == 0:
            continue
        writer.writerow({'ArrivalTime': line['ArrivalTime'], 'DurationSeconds': line['DurationSeconds'],
                         'StreetId': line['StreetId'], 'StreetName': line['StreetName'], 'BetweenStreet1': line['BetweenStreet1'],
                         'BetweenStreet2': line['BetweenStreet2'], 'In Violation': line['In Violation'], 'Vehicle Present': line['Vehicle Present']
                         })

data1.close()
'''

'''
import csv
data1 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/Pedestrian_Counting_System___2009_to_Present__counts_per_hour_.csv', 'r', encoding='utf-8')
rdr_dt1 = csv.DictReader(data1)

data2 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/Pedestrian_Counting_System_-_Sensor_Locations.csv', 'r', encoding='utf-8')
rdr_dt2 = csv.DictReader(data2)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data/reduced_data2_2014_2015.csv', 'w',encoding='utf-8',newline='') as new_data2_2014_2015:
    fieldnames = ['Date_Time', 'Year', 'Month', 'Day', 'Mdate', 'Time_hour', 'Sensor_ID','Hourly_Counts']
    writer = csv.DictWriter(new_data2_2014_2015, fieldnames=fieldnames)
    writer.writeheader()

    for i,line in enumerate(rdr_dt1):

        writer.writerow({'Date_Time': line['Date_Time'], 'Year': line['Year'],
                         'Month': line['Month'], 'Day': line['Day'], 'Mdate': line['Mdate'] ,'Time_hour': line['Time'], 'Sensor_ID': line['Sensor_ID'],
                         'Hourly_Counts': line['Hourly_Counts']
                         })

data1.close()
data2.close()
'''


'''
import csv
#data1 = open('C:/Users/Dae-Young Park/sensor_analysis/refined_data/reduced_data2.csv', 'r', encoding='utf-8')
#rdr_dt1 = csv.DictReader(data1)
data1 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data/reduced_data2.csv')
data2 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data/reduced_data2_location.csv')

data3 = data1.join(data2.set_index('Sensor_ID'), on='Sensor_ID') # 조인하면 인덱스 번호가 새로 생기는데 난 이거 필요없으니 없애는 파라미터 찾아보기

data3.to_csv('./new_data2.csv')
'''



'''
import csv
data1 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/Traffic_Count_Vehicle_Classification_2014-2017.csv', 'r', encoding='utf-8')
rdr_dt1 = csv.DictReader(data1)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data/new_data3_2014_2017.csv', 'w',encoding='utf-8',newline='') as new_data:
    # 14,15년도는 모토사이클이랑 바이크를 survey하지 않았다고 하니, 두 칼럼은 일단 빼자
    #fieldnames = ['date', 'time','road_name', 'location', 'suburb', 'num_vehicle','num_motorcycle','num_bike','average_speed','85th_percentile_speed']
    fieldnames = ['date', 'time', 'road_name', 'location', 'suburb', 'num_vehicle',
                  'average_speed', '85th_percentile_speed']
    writer = csv.DictWriter(new_data, fieldnames=fieldnames)
    writer.writeheader()

    for i,line in enumerate(rdr_dt1):
        if i == 0:
            continue

        sum_vehicle = 0
        for j in range(1,14):
            v = line["vehicle_class_{num}".format(num=j)]
            if v == '':
                v = '0'
            sum_vehicle += int(v)

        str_sum_vehicle = str(sum_vehicle)
        writer.writerow({'date': line['date'], 'time': line['time'],
                         'road_name': line['road_name'], 'location': line['location'], 'suburb': line['suburb'], 'num_vehicle': sum_vehicle,
                          'average_speed': line['average_speed'], '85th_percentile_speed': line['85th_percentile_speed']
                         })

data1.close()
'''



'''
import csv
data1 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/Melbourne_Bike_Share_Station_Readings_2011-2017.csv', 'r', encoding='utf-8')
rdr_dt1 = csv.DictReader(data1)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data/new_data4_2011_2017.csv', 'w',encoding='utf-8',newline='') as new_data:
    fieldnames = ['NAME', 'NBBIKES', 'NBEMPTYDOCKS', 'Year','Month','Day','Mdate','Time_hour','Time_minute', 'latitude', 'longitude']
    writer = csv.DictWriter(new_data, fieldnames=fieldnames)
    writer.writeheader()

    for i,line in enumerate(rdr_dt1):

        total_date = line['RUNDATE']
        year = total_date[0:4]
        month = getMonth( total_date[4:6] )
        mdate = total_date[6:8]
        day = getDayOfTheWeek(int(year),int(total_date[4:6]),int(mdate))
        time_hour = total_date[8:10]
        time_minute = total_date[10:12]

        writer.writerow({'NAME': line['NAME'], 'NBBIKES': line['NBBIKES'],
                         'NBEMPTYDOCKS': line['NBEMPTYDOCKS'], 'Year': year, 'Month': month, 'Day': day,
                          'Mdate': mdate, 'Time_hour': time_hour, 'Time_minute': time_minute, 'latitude': line['LAT'],
                         'longitude': line['LONG']
                         })

data1.close()
'''

'''
from datetime import date
def getDayOfTheWeek(year, month, day):
    dayOfTheWeek = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return dayOfTheWeek[date(year, month, day).weekday()]
def getMonth(num):
    return { '01':'January', '02':'February', '03':'March', '04': 'April',
             '05':'May','06':'June','07':'July', '08':'August','09': 'September','10': 'October','11': 'November','12':'December'}[num]
def setTimeonAMPM(num,AM_PM):
    if AM_PM == 'AM':
        if num == '12':
            return '00'
        return num
    elif AM_PM == 'PM':
        return { '01':'13', '02':'14', '03':'15', '04': '16',
             '05':'17','06':'18','07':'19', '08':'20','09': '21','10': '22','11': '23','12':'12'}[num]

import csv
data1 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/Sensor_readings__with_temperature__light__humidity_every_5_minutes_at_8_locations__trial__2014_to_2015_.csv', 'r', encoding='utf-8')
rdr_dt1 = csv.DictReader(data1)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data/new_data5_2014_2015.csv', 'w',encoding='utf-8',newline='') as new_data:
    fieldnames = ['Year','Month','Day','Mdate','Time_hour','Time_minute',
                  'temp_max','temp_min','temp_avg',
                  'light_max','light_min','light_avg',
                  'humidity_min','humidity_max','humidity_avg',
                  'location', 'latitude', 'longitude']
    writer = csv.DictWriter(new_data, fieldnames=fieldnames)
    writer.writeheader()

    for i,line in enumerate(rdr_dt1):

        total_date = line['timestamp']

        month = getMonth( total_date[0:2] )
        mdate = total_date[3:5]
        year = total_date[6:10]

        day = getDayOfTheWeek(int(year),int(total_date[0:2]),int(mdate))
        time_hour = setTimeonAMPM(total_date[11:13], total_date[20:22])
        time_minute = total_date[14:16]

        writer.writerow({'Year': year, 'Month': month, 'Day': day,
                          'Mdate': mdate, 'Time_hour': time_hour, 'Time_minute': time_minute,
                         'temp_max': line['temp_max'], 'temp_min': line['temp_min'], 'temp_avg': line['temp_avg'],
                        'light_max': line['light_max'], 'light_min': line['light_min'], 'light_avg': line['light_avg'],
                        'humidity_min': line['humidity_min'], 'humidity_max': line['humidity_max'], 'humidity_avg': line['humidity_avg'],
                         'location': line['location'],'latitude': line['latitude'], 'longitude': line['longitude']
                         })

data1.close()
'''

# 여기서부터 refined_data_v2 를 만들기 위한 코드
'''
from datetime import date
def getDayOfTheWeek(year, month, day):
    dayOfTheWeek = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return dayOfTheWeek[date(year, month, day).weekday()]
def getMonth(num):
    return { '01':'January', '02':'February', '03':'March', '04': 'April',
             '05':'May','06':'June','07':'July', '08':'August','09': 'September','10': 'October','11': 'November','12':'December'}[num]
def setTimeonAMPM(num,AM_PM):
    if AM_PM == 'AM':
        if num == '12':
            return '00'
        return num
    elif AM_PM == 'PM':
        return { '01':'13', '02':'14', '03':'15', '04': '16',
             '05':'17','06':'18','07':'19', '08':'20','09': '21','10': '22','11': '23','12':'12'}[num]

def changeFormat(num):
    if num == '0':
        return '00'
    elif num == '1':
        return '01'
    elif num == '2':
        return '02'
    elif num == '3':
        return '03'
    elif num == '4':
        return '04'
    elif num == '5':
        return '05'
    elif num == '6':
        return '06'
    elif num == '7':
        return '07'
    elif num == '8':
        return '08'
    elif num == '9':
        return '09'
    else: return num

import csv
data1 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data/new_data3_2014_2017.csv', 'r', encoding='utf-8')
rdr_dt1 = csv.DictReader(data1)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v2/new_data3_2014_2017.csv', 'w',encoding='utf-8',newline='') as new_data:
    fieldnames = ['Year','Month','Day','Mdate','Time_hour','Time_minute', 'road_name', 'location', 'suburb', 'num_vehicle',
                  'average_speed', '85th_percentile_speed']
    writer = csv.DictWriter(new_data, fieldnames=fieldnames)
    writer.writeheader()

    for i,line in enumerate(rdr_dt1):

        total_date = str(line['date'])
        total_date = total_date.split('/')
        total_time = str(line['time'])
        total_time = total_time.split(':')

        mdate = changeFormat(total_date[0])
        print(mdate)
        month = getMonth( total_date[1] )
        year = total_date[2]

        day = getDayOfTheWeek(int(year),int(total_date[1]),int(mdate))
        time_hour = changeFormat(total_time[0])
        time_minute = total_time[1]

        writer.writerow({'Year': year, 'Month': month, 'Day': day,
                          'Mdate': mdate, 'Time_hour': time_hour, 'Time_minute': time_minute,
                         'road_name': line['road_name'], 'location': line['location'], 'suburb': line['suburb'],
                        'num_vehicle': line['num_vehicle'], 'average_speed': line['average_speed'],
                         '85th_percentile_speed': line['85th_percentile_speed']
                         })

data1.close()
'''
'''
df1 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v2/new_data3_2014_2017.csv')

new_df = df1.loc[ (df1['Year'] == 2015) ]
#new_df = df1.loc[ lambda df: (df1['Year'] == '2015') ]
print(new_df)

new_df.to_csv('./new_data3_2015.csv')
'''

'''
from datetime import date
def getDayOfTheWeek(year, month, day):
    dayOfTheWeek = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return dayOfTheWeek[date(year, month, day).weekday()]
def getMonth(num):
    return { '01':'January', '02':'February', '03':'March', '04': 'April',
             '05':'May','06':'June','07':'July', '08':'August','09': 'September','10': 'October','11': 'November','12':'December'}[num]
def setTimeonAMPM(num,AM_PM):
    if AM_PM == 'AM':
        if num == '12':
            return '00'
        return num
    elif AM_PM == 'PM':
        return { '01':'13', '02':'14', '03':'15', '04': '16',
             '05':'17','06':'18','07':'19', '08':'20','09': '21','10': '22','11': '23','12':'12'}[num]

import csv
data1 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data/new_data1_2015.csv', 'r', encoding='utf-8')
rdr_dt1 = csv.DictReader(data1)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v2/new_data1_2015.csv', 'w',encoding='utf-8',newline='') as new_data1:
    fieldnames = ['Year', 'Month', 'Day', 'Mdate', 'Time_hour','Time_minute','DurationMinutes', 'StreetId',
                  'StreetName', 'BetweenStreet1', 'BetweenStreet2','In Violation', 'Vehicle Present']
    writer = csv.DictWriter(new_data1, fieldnames=fieldnames)
    writer.writeheader()

    for i,line in enumerate(rdr_dt1):

        total_time = line['ArrivalTime']
        total_time = total_time.split(' ')
        month_mdate_year = total_time[0].split('/')
        hour_minute = total_time[1].split(':')
        AM_PM = total_time[2]

        month = getMonth(month_mdate_year[0])
        mdate = month_mdate_year[1]
        year = month_mdate_year[2]
        day = getDayOfTheWeek(int(year), int(month_mdate_year[0]), int(mdate))
        time_hour = setTimeonAMPM(hour_minute[0],AM_PM)
        time_minute = hour_minute[1]

        duration_min = str( int(line['DurationSeconds']) / 60.0 )

        writer.writerow({'Year': year, 'Month': month, 'Day': day, 'Mdate': mdate, 'Time_hour': time_hour,'Time_minute': time_minute,
            'DurationMinutes': duration_min,
                         'StreetId': line['StreetId'], 'StreetName': line['StreetName'], 'BetweenStreet1': line['BetweenStreet1'],
                         'BetweenStreet2': line['BetweenStreet2'], 'In Violation': line['In Violation'], 'Vehicle Present': line['Vehicle Present']
                         })

data1.close()
'''
'''
df1 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/On-street_Car_Parking_Sensor_Data_-_2015.csv')

new_df = df1.loc[ (df1['Year'] == 2015) ]
#new_df = df1.loc[ lambda df: (df1['Year'] == '2015') ]
print(new_df)

new_df.to_csv('./new_data3_2015.csv')
'''

# 여기서부터 버전3 만들기 ㄱㄱ
'''
df1 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v2/new_data2.csv')

new_df1 = df1.loc[ (df1['Year'] == 2019) | (df1['Year'] == 2020) ]
#new_df = df1.loc[ lambda df: (df1['Year'] == '2015') ]
print(new_df1)

new_df1.to_csv('./new_data2_2019_2020.csv')
'''


'''
import csv
data1 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/new_data1_2019_2020_with_loc.csv', 'r', encoding='utf-8')
rdr_dt1 = csv.DictReader(data1)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/new_refined_data1_2019_2020_with_loc.csv', 'w',encoding='utf-8',newline='') as new_data1:
    fieldnames = ['Year', 'Month', 'Day', 'Mdate', 'Time_hour','Time_minute','DurationMinutes', 'StreetId',
                  'StreetName', 'bay_id','In Violation', 'Vehicle Present','latitude','longitude']
    writer = csv.DictWriter(new_data1, fieldnames=fieldnames)
    writer.writeheader()

    for i,line in enumerate(rdr_dt1):

        if line['latitude'] == '':
            continue
        
        total_time = line['ArrivalTime']
        total_time = total_time.split(' ')
        month_mdate_year = total_time[0].split('/')
        hour_minute = total_time[1].split(':')
        AM_PM = total_time[2]

        month = getMonth(month_mdate_year[0])
        mdate = month_mdate_year[1]
        year = month_mdate_year[2]
        day = getDayOfTheWeek(int(year), int(month_mdate_year[0]), int(mdate))
        time_hour = setTimeonAMPM(hour_minute[0],AM_PM)
        time_minute = hour_minute[1]
        

        writer.writerow({'Year': line['Year'], 'Month': line['Month'], 'Day': line['Day'],
                         'Mdate': line['Mdate'], 'Time_hour': line['Time_hour'],'Time_minute': line['Time_minute'],
            'DurationMinutes': line['DurationMinutes'],
                         'StreetId': line['StreetId'], 'StreetName': line['StreetName'], 'bay_id': line['bay_id'],
                         'In Violation': line['In Violation'], 'Vehicle Present': line['Vehicle Present'],
                         'latitude': line['latitude'], 'longitude': line['longitude']
                         })

data_pd = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/new_refined_data1_2019_2020_with_loc.csv')
print(data_pd)

data1.close()
'''

'''
data2 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/On-street_Parking_Bay_Sensors.csv', 'r', encoding='utf-8')
rdr_dt2 = csv.DictReader(data2)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/new_data1_bay_id_2019.csv', 'w',encoding='utf-8',newline='') as new_data1:
    fieldnames = ['bay_id', 'latitude', 'longitude']
    writer = csv.DictWriter(new_data1, fieldnames=fieldnames)
    writer.writeheader()

    for i,line in enumerate(rdr_dt2):

        writer.writerow({'bay_id': line['bay_id'], 'latitude': line['lat'], 'longitude': line['lon']
                         })

data1.close()
data2.close()
'''
'''
data1 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/new_data1_2019_2020.csv')
data2 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/new_data1_bay_id.csv')

data3 = data1.join(data2.set_index('bay_id'), on='bay_id')
#data3 = pd.concat([data1,data2])

data3.to_csv('./new_data1_2019_2020_with_loc.csv')

#print_data1 = parallel_dataframe(data1,print)

print(data3)
'''

from datetime import date
def getDayOfTheWeek(year, month, day):
    dayOfTheWeek = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return dayOfTheWeek[date(year, month, day).weekday()]
def getMonth(num):
    return { '01':'January', '02':'February', '03':'March', '04': 'April',
             '05':'May','06':'June','07':'July', '08':'August','09': 'September','10': 'October','11': 'November','12':'December'}[num]
def setTimeonAMPM(num,AM_PM):
    if AM_PM == 'AM':
        if num == '12':
            return '00'
        return num
    elif AM_PM == 'PM':
        return { '01':'13', '02':'14', '03':'15', '04': '16',
             '05':'17','06':'18','07':'19', '08':'20','09': '21','10': '22','11': '23','12':'12'}[num]


import csv

'''
data1 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/Microclimate_Sensor_Readings.csv', 'r',
             encoding='utf-8')
rdr_dt1 = csv.DictReader(data1)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/new_data5_2019_2020.csv',
          'w', encoding='utf-8', newline='') as new_data1:
    fieldnames = ['Year', 'Month', 'Day', 'Mdate', 'Time_hour', 'Time_minute', 'site_id',
                  'sensor_id', 'value', 'type', 'units']
    writer = csv.DictWriter(new_data1, fieldnames=fieldnames)
    writer.writeheader()

    for i, line in enumerate(rdr_dt1):

        total_time = line['local_time']
        total_time = total_time.split(' ')
        month_mdate_year = total_time[0].split('/')
        hour_minute = total_time[1].split(':')
        AM_PM = total_time[2]

        month = getMonth(month_mdate_year[1])
        mdate = month_mdate_year[2]
        year = month_mdate_year[0]
        day = getDayOfTheWeek(int(year), int(month_mdate_year[1]), int(mdate))
        time_hour = setTimeonAMPM(hour_minute[0], AM_PM)
        time_minute = hour_minute[1]

        writer.writerow({'Year': year, 'Month': month, 'Day': day,
                         'Mdate': mdate, 'Time_hour': time_hour, 'Time_minute': time_minute,
                         'site_id': line['site_id'],
                         'sensor_id': line['sensor_id'], 'value': line['value'], 'type': line['type'],
                         'units': line['units']
                         })


data2 = open('C:/Users/Dae-Young Park/sensor_analysis/dataset/Microclimate_Sensor_Locations.csv', 'r',
             encoding='utf-8')
rdr_dt2 = csv.DictReader(data2)

with open('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/data5_with_loc.csv',
          'w', encoding='utf-8', newline='') as new_data1:
    fieldnames = ['site_id', 'description', 'latitude', 'longitude']
    writer = csv.DictWriter(new_data1, fieldnames=fieldnames)
    writer.writeheader()

    for i, line in enumerate(rdr_dt2):
        writer.writerow({'site_id': line['site_id'], 'description': line['description'], 'latitude': line['latitude'],
                         'longitude': line['longitude']
                         })
'''
'''
data1 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/new_data5_2019_2020.csv')
data2 = pd.read_csv('C:/Users/Dae-Young Park/sensor_analysis/dataset/refined_data_v3/new_data5_with_loc.csv')

data3 = data1.join(data2.set_index('site_id'), on='site_id')
#data3 = pd.concat([data1,data2])

data3.to_csv('./new_data5_2019_2020_with_loc.csv')

#print_data1 = parallel_dataframe(data1,print)

print(data3)
'''


