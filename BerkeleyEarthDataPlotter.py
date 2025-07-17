import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_context('talk')

file = 'Daten/BerkeleyEarth/Paris_49.03N-2.45E-TAVG-Trend.txt'
observable_column = 'AnomalyMonth'

monthly_reference = {1:2.82,2:3.84,3:6.49,4:9.15,5:13.02,6:16.17,7:18.08,8:17.81,9:15.36,10:11.05,11:6.41,12:3.83}

data1 = pl.read_csv(file, has_header=True, separator=';', skip_rows=67)
# Stri g Datum in echtes Datum umwandeln
data1 = data1.with_columns(pl.col('Year-Month').str.to_date(format='%Y-%m').alias('date'))
data1 = data1.with_columns(pl.col('date').dt.year().alias('year'))
data1 = data1.with_columns(pl.col('date').dt.month().alias('month'))
# Berkeley-Earth gibt die monatlichen Daten als Anomalie zum Wert der Referenzperiode an ->
# Referenzwert wieder dazuaddieren, um Absolutwert zu bekommen
# monatlicher Referenzwert
data1 = data1.with_columns(data1.map_rows(lambda row: row[1] + monthly_reference[row[13]])).rename({'map':'monthly_abs'})
# Referenzwert über ganze Referenzperiode
data1 = data1.with_columns(data1.map_rows(lambda row: row[3] + 8.06)).rename({'map':'yearly_abs'})

plt.figure(figsize=(14, 8))

for month in range(1, 13):
    data1_plot = data1.filter(pl.col('month') == month)

    reference_mean1 = data1_plot.filter((pl.col('year') >= 1961) & (pl.col('year') <= 2010))['monthly_abs'].mean()
    data1_plot = data1_plot.with_columns(data1_plot.map_rows(lambda row: row[15] - reference_mean1)).rename({'map':'difference'})

    data1_rolling = data1_plot.rolling('year', period='10i').agg([pl.col('date').last(), pl.col(observable_column).mean()])

    plt.figure(figsize=(14, 8))
    sns.lineplot(data=data1_plot, x='date', y=observable_column, alpha=0.5, drawstyle='steps', label='Berkeley-Earth, Monatsmittel')
    sns.lineplot(data=data1_rolling[10:], x='date', y=observable_column, label='Berkeley-Earth, rollend')
    plt.xlabel('Jahr')
    plt.ylabel('Temperatur [°C]')
    plt.title('Frankfurt, monatlicher Temperatur-Mittelwert für Monat:' + str(month))
    plt.legend(loc="upper left")
    plt.grid()
    plt.show()
    plt.close()