from pathlib import Path
import requests
import pandas as pd
import time
import ast
import os


def data_extr_meteo(stations : list, time_start: str, time_end: str): #function takes station from a list of stations and extracts data from meteo.lt API about weather within chosen time interval
    """
    Function takes station from a list of chosen stations and extracts data from meteo.lt API about weather within chosen time interval. For each station, collected data is saved to separate CSV file 'stationname.csv'.
    """
    try:
        time_range = pd.date_range(start=time_start, end=time_end)
        for station in stations:
            time.sleep(70)  
            a = 0
            for j in time_range:
                x = requests.get(f'https://api.meteo.lt/v1/stations/{station}/observations/{j.strftime('%Y-%m-%d')}')
                with open(f'{station}.csv', 'a') as file:
                    file.write(f'{x.json()['observations']}\n')
                a = a + 1
                if a > 170:
                    time.sleep(70) #api.meteo.lt restricts queries to 180 per minute, thats why file.sleep(70) function is required every 170 querries
                    a = 0
    except Exception as e:
        with open(f'{station}.csv', 'a') as file:
            os.remove(file.name)
        print(f'Error: {e}')    


def meteo_file_to_pd(file_name : Path):
    """
    Function takes path to a file, reads file data, converts data to Pandas dataframe, cleans data prepares for further use.
    """
    lst = []
    pd_lst = []
    with open(file_name, 'r') as open_file:    
        for j in open_file:
            list_line = ast.literal_eval(j)
            lst.append(list_line)
    for h in lst:
        pd_lst = pd_lst + h 
    pd_station = pd.DataFrame(pd_lst).sort_values(by='observationTimeUtc')
    pd_station = pd_station.drop_duplicates()
    pd_station = pd_station[['observationTimeUtc','airTemperature','cloudCover']]
    pd_station = pd_station.bfill()
    pd_station = pd_station.rename(columns={'observationTimeUtc' : 'Time(Meteo)'})
    pd_station['airTemperature'] = round(pd_station['airTemperature'])
    pd_station['cloudCover'] = round(pd_station['cloudCover'], -1)
    pd_station = pd_station.reset_index(drop=True)
    return pd_station
    
   

def litgrid_data_to_pd(file_name: Path):
    """
    Function takes path to a file, reads file data, converts data to Pandas dataframe, prepares for further analysis.
    """
    elektr_duom = pd.read_csv(f'{file_name}')
    elektr_duom = elektr_duom.rename(columns={'Unnamed: 0' : 'Time'})
    elektr_duom = elektr_duom.rename(columns={'Prognozuojamas nacionalinis elektros energijos suvartojimas' : 'Predicted electricity consumption by Ignitis'})
    elektr_duom = elektr_duom.rename(columns={'Faktinis nacionalinis Elektros energijos vartojimas' : 'Actual electricity consumption'})
    elektr_duom = elektr_duom.drop(columns='Planuojamas nacionalinis elektros energijos suvartojimas')
    elektr_duom['Time'] = pd.to_datetime(elektr_duom['Time'])
    elektr_duom['Month'] = elektr_duom['Time'].dt.month
    elektr_duom['Week'] = elektr_duom['Time'].dt.weekday
    elektr_duom['Hour'] = elektr_duom['Time'].dt.hour
    elektr_duom = elektr_duom.sort_values(by='Time')
    return elektr_duom

