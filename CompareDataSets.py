from cProfile import label

import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_context('talk')

berkeley_monthly_nominal = {1: -0.87, 2: 0.28, 3: 3.67, 4: 7.43, 5: 11.83, 6: 15.24, 7: 16.73, 8: 16.35, 9: 13.36, 10: 8.60, 11: 3.69, 12: 0.42}

month_filter = 1

# Datensätze für den Vergleich
file1 = 'Daten/BerkeleyEarth/BerkeleyEarth_FrankfurtMain_50.63N-8.87E-TAVG-Trend.txt'
file2 = 'Daten/DWD/DWD_FrankfurtMonatlich_19350701_20241231_01420.txt'

# Berkeley-Earth
data1 = pl.read_csv(file1, has_header=True, separator=';', skip_rows=66)
# Stri g Datum in echtes Datum umwandeln
data1 = data1.with_columns(pl.col('Year-Month').str.to_date(format='%Y-%m').alias('date'))
data1 = data1.with_columns(pl.col('date').dt.year().alias('year'))
data1 = data1.with_columns(pl.col('date').dt.month().alias('month'))
# Berkeley-Earth gibt die monatlichen Daten als Anomalie zum Wert der Referenzperiode an ->
# Referenzwert wieder dazuaddieren, um Absolutwert zu bekommen
# monatlicher Referenzwert
data1 = data1.with_columns(data1.map_rows(lambda row: row[1] + berkeley_monthly_nominal[row[13]])).rename({'map':'monthly_abs'})
# Referenzwert über ganze Referenzperiode
data1 = data1.with_columns(data1.map_rows(lambda row: row[3] + 8.06)).rename({'map':'yearly_abs'})

# Deutscher Wetterdienst
data2 = pl.read_csv(file2, has_header=True, separator=';')
data2 = data2.with_columns(pl.col('MESS_DATUM_BEGINN').cast(pl.String).str.to_date(format='%Y%m%d').alias('date'))
data2 = data2.with_columns(pl.col('date').dt.year().alias('year'))
data2 = data2.with_columns(pl.col('date').dt.month().alias('month'))
data2 = data2.with_columns(pl.col('date').dt.day().alias('day'))
# Der Datensatz enthält Messwerte mit "-999" (fehlerhafte Messungen) -> diese rausfiltern
data2 = data2.filter((pl.col('TAVG') > -900))

# plot jährliche Mittelwerte aus Momaten
plt.figure(figsize=(14, 8))

data1_agg_year = data1.group_by_dynamic("date", every='1y').agg(pl.col('monthly_abs').mean())
data2_agg_year = data2.group_by_dynamic("date", every='1y').agg(pl.col('TAVG').mean())
print(data2_agg_year)
sns.scatterplot(data=data1_agg_year, x='date', y='monthly_abs', label='Berkeley-Earth')
sns.scatterplot(data=data2_agg_year, x='date', y='TAVG', label='Deut. Wetterdienst')

plt.xlabel('Jahr')
plt.ylabel('Temperatur [°C]')
plt.title('Frankfurt, Jahres-Mittelwerte')
plt.legend(loc="upper left")
plt.show()

# plot für jeden Monat
plt.figure(figsize=(14, 8))

for month in range(1, 13):
    data1_plot = data1.filter(pl.col('month') == month)
    data2_plot = data2.filter(pl.col('month') == month)

    reference_mean1 = data1_plot.filter((pl.col('year') >= 1961) & (pl.col('year') <= 2010))['monthly_abs'].mean()
    data1_plot = data1_plot.with_columns(data1_plot.map_rows(lambda row: row[15] - reference_mean1)).rename({'map':'difference'})

    reference_mean2 = data2_plot.filter((pl.col('year') >= 1961) & (pl.col('year') <= 2010))['TAVG'].mean()
    data2_plot = data2_plot.with_columns(data2_plot.map_rows(lambda row: row[5] - reference_mean2)).rename({'map':'difference'})

    data2_agg_month = data2_plot.group_by_dynamic("date", every='1mo').agg(pl.col('TAVG').mean())

    data1_rolling = data1_plot.rolling('year', period='10i').agg([pl.col('date').last(), pl.col('Anomaly_Month').mean()])
    data2_rolling = data2_plot.rolling('year', period='10i').agg([pl.col('date').last(), pl.col('difference').mean()])

    plt.figure(figsize=(14, 8))
    sns.lineplot(data=data1_plot, x='date', y='Anomaly_Year', alpha=0.5, drawstyle='steps', label='Berkeley-Earth, Monatsmittel')
    sns.lineplot(data=data2_plot, x='date', y='difference', alpha=0.5, drawstyle='steps', label='Deut. Wetterdienst, Monatsmittel')
    sns.lineplot(data=data1_rolling[10:], x='date', y='Anomaly_Month', label='Berkeley-Earth, rollend')
    sns.lineplot(data=data2_rolling[10:], x='date', y='difference', label='Deut. Wetterdienst, rollend')
    plt.xlabel('Jahr')
    plt.ylabel('Temperatur [°C]')
    plt.title('Frankfurt, monatlicher Temperatur-Mittelwert für Monat:' + str(month))
    plt.legend(loc="upper left")
    plt.grid()
    plt.show()
    plt.close()