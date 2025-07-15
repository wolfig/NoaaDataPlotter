import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns


berkeley_monthly_nominal = {1: -0.87, 2: 0.28, 3: 3.67, 4: 7.43, 5: 11.83, 6: 15.24, 7: 16.73, 8: 16.35, 9: 13.36, 10: 8.60, 11: 3.69, 12: 0.42}

file1 = 'Daten/BerkeleyEarth_FrankfurtMain_50.63N-8.87E-TAVG-Trend.txt'
file2 = 'Daten/DWD_FrankfurtMonatlich_19350701_20241231_01420.txt'

data1 = pl.read_csv(file1, has_header=True, separator=';', skip_rows=66)
data1 = data1.with_columns(pl.col('Year-Month').str.to_date(format='%Y-%m').alias('date'))
data1 = data1.with_columns(pl.col('date').dt.year().alias('year'))
data1 = data1.with_columns(pl.col('date').dt.month().alias('month'))
data1 = data1.with_columns(data1.map_rows(lambda row: row[1] + berkeley_monthly_nominal[row[13]]))
print(data1)

data2 = pl.read_csv(file2, has_header=True, separator=';')
data2 = data2.with_columns(pl.col('DATE').cast(pl.String).str.to_date(format='%Y%m%d').alias('date'))
data2 = data2.with_columns(pl.col('date').dt.year().alias('year'))
data2 = data2.with_columns(pl.col('date').dt.month().alias('month'))
data2 = data2.with_columns(pl.col('date').dt.day().alias('day'))
data2_agg = data2.group_by_dynamic("date", every='1mo').agg(pl.col('TAVG').mean())
print(data2_agg)

sns.lineplot(x=data1['date'], y=data1['map'])
sns.lineplot(x=data2_agg['date'], y=data2_agg['TAVG'])
plt.show()