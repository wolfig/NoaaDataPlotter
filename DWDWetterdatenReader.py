import datetime
import os
from urllib import parse
import polars as pl
from matplotlib import pyplot as plt
import requests
import seaborn as sns

NOAA_BASE_URL = 'https://www.ncei.noaa.gov/access/services/data/v1?'
temp_file_path = os.path.join('temp', 'tmp.csv')

def fahrenheit_to_celsius(data_row: dict, field_name):
    try:
        if data_row[field_name] is not None:
            value = data_row[field_name]
            if isinstance(data_row[field_name], str):
                value = float(data_row[field_name])

            return (value - 32.0) * 5.0 / 9.0
        else:
            return None
    except Exception as err:
        print(data_row)
        return None

def get_noaa_data(station_code):
    request_url = NOAA_BASE_URL + parse.urlencode({'dataset': 'daily-summaries',
                                                   'stations': station_code,
                                                   'startDate': '1800-01-01',
                                                   'endDate': datetime.date.today(),
                                                   'format': 'csv',
                                                   'includeStationName':'true',
                                                   'units': 'metric'})

    print('[INFO] sending request', request_url)

    response = requests.get(request_url)
    response_string = ""
    if response.status_code == 200:
         response_string = response.content.decode().replace(' ', '')
    else:
        print('[WARNING] Response code:', response.status_code)

    response.close()

    with open(temp_file_path, 'w') as file:
        file.write(response_string)

    return temp_file_path


if __name__ == '__main__':

    station_id = 'AYW00090001'
    observable_column = 'TMIN_RAW'
    temperature_unit = 'C'
    date_format = '%Y-%m-%d'
    sns.set_context('paper')

    pl.Config.set_tbl_cols(200)
    pl.Config.set_tbl_width_chars(2048)
    pl.Config.set_tbl_rows(5)

    data_fetched = pl.read_csv(get_noaa_data(station_id))
    station_name = data_fetched['NAME'][0]
    print('[INFO] Station name:', station_name)

    columns_to_drop = data_fetched.columns
    columns_to_drop.remove('STATION')
    columns_to_drop.remove('NAME')
    columns_to_drop.remove('DATE')
    columns_to_drop.remove('TMAX')
    columns_to_drop.remove('TMIN')
    columns_to_drop.remove('TAVG')
    columns_to_drop.remove('PRCP')

    data_fetched = data_fetched.drop(columns_to_drop)
    print("[INFO] NOAA data:\n", data_fetched)

    rawdata = data_fetched

    rawdata = rawdata.with_columns(pl.col('DATE').cast(pl.String).str.to_date(format=date_format).alias('date'))
    rawdata = rawdata.with_columns(pl.col('TAVG').cast(pl.Float64))
    rawdata = rawdata.with_columns(pl.col('TMAX').cast(pl.Float64))
    rawdata = rawdata.with_columns(pl.col('TMIN').cast(pl.Float64))
    rawdata = rawdata.with_columns(pl.col('date').dt.year().alias('year'))
    rawdata = rawdata.with_columns(pl.col('date').dt.month().alias('month'))
    rawdata = rawdata.with_columns(pl.col('date').dt.day().alias('day'))
    print(rawdata.dtypes)
    print(rawdata)

    if temperature_unit == 'F':
        rawdata = rawdata.with_columns(pl.struct(pl.all()).map_elements(lambda row: fahrenheit_to_celsius(row, 'TAVG'), return_dtype=pl.Float64).alias('TAVG_C'))
        rawdata = rawdata.with_columns(pl.struct(pl.all()).map_elements(lambda row: fahrenheit_to_celsius(row, 'TMAX'),
                                                                        return_dtype=pl.Float64).alias('TMAX_C'))
        rawdata = rawdata.with_columns(pl.struct(pl.all()).map_elements(lambda row: fahrenheit_to_celsius(row, 'TMIN'),
                                                                        return_dtype=pl.Float64).alias('TMIN_C'))

    if observable_column in ['TMAX', 'TMIN', 'TAVG', 'TMAX_mean', 'TMIN_mean', 'TAVG_mean']:
        data_grouped = rawdata.group_by('year')
        if temperature_unit == 'F':
            agg_data = data_grouped.agg(pl.col('TMAX_C').max().alias('TMAX'),
                                        pl.col('TAVG_C').max().alias('TAVG'),
                                        pl.col('TMIN_C').min().alias('TMIN'),
                                        pl.col('TMAX_C').mean().alias('TMAX_mean'),
                                        pl.col('TAVG_C').mean().alias('TAVG_mean'),
                                        pl.col('TMIN_C').mean().alias('TMIN_mean'),
                                        pl.col('PRCP').mean().alias('PRCP_mean'))
        else:
            agg_data = data_grouped.agg(pl.col('TMAX').max(),
                                        pl.col('TAVG').max(),
                                        pl.col('TMIN').min(),
                                        pl.col('TMAX').mean().alias('TMAX_mean'),
                                        pl.col('TAVG').mean().alias('TAVG_mean'),
                                        pl.col('TMIN').mean().alias('TMIN_mean'),
                                        pl.col('PRCP').mean().alias('PRCP_mean'))

        agg_data = agg_data.sort("year")
        columns_to_drop = agg_data.columns
        columns_to_drop.remove(observable_column)
        columns_to_drop.remove('year')
        agg_data = agg_data.drop(columns_to_drop)
        agg_data = agg_data.drop_nans()
        agg_data = agg_data.drop_nulls()
        print(agg_data)

    plt.figure(figsize=(12,7))

    observable_text = '?'
    if observable_column == 'TMAX':
        observable_text = 'Höchste Temperatur im Jahr'
    elif observable_column == 'TMIN':
        observable_text = 'Niedrigste Temperatur im Jahr'
    elif observable_column == 'TAVG':
        observable_text = 'Höchste mittlere Temperatur im Jahr'
    elif observable_column == 'TMAX_mean':
        observable_text = 'Mittelwerte der höchsten Tagestemperaturen pro Jahr'
    elif observable_column == 'TMIN_mean':
        observable_text = 'Mittelwerte der niedrigsten Tagestemperaturen pro Jahr'
    elif observable_column == 'TAVG_mean':
        observable_text = 'Mittelwerte der mittleren Tagesemperatur pro Jahr'
    elif observable_column.endswith('TMAX_RAW'):
        observable_text = 'Tages-Maximaltemperaturen'
    elif observable_column.endswith('TMIN_RAW'):
        observable_text = 'Tages-Minimaltemperaturen'
    elif observable_column.endswith('TAVG_RAW'):
        observable_text = 'Tages-Durchschnittstemperaturen'

    plt.title(f'{station_name}: {observable_text}')
    plt.ylabel('Temperatur [°C]')
    if observable_column in ['TMAX', 'TMIN', 'TAVG', 'TMAX_mean', 'TMIN_mean', 'TAVG_mean']:
        plt.xlabel('Jahr')
        plt.scatter(x=agg_data['year'], y=agg_data[observable_column])
        rolling_data = agg_data.rolling('year', period='10i').agg([pl.mean(observable_column), pl.count(observable_column).alias('count')])
        print(rolling_data)
        plt.plot(rolling_data['year'][10:], rolling_data[observable_column][10:], color='red')
    elif observable_column in ['TMAX_RAW', 'TMIN_RAW', 'TAVG_RAW']:
        plt.xlabel('Jahr')
        plt.scatter(x=rawdata['date'], y=rawdata[observable_column.replace('_RAW', '')])

    plt.grid()
    plt.show()
