import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns


file1 = 'Daten/BerkeleyEarth_FrankfurtMain_50.63N-8.87E-TAVG-Trend.txt'
file2 = 'Daten/FrankfurtMain_19350701_20241231.csv'

data1 = pl.read_csv(file1, has_header=True, separator=';', skip_rows=66)
data1 = data1.with_columns(pl.col('Year-Month').str.to_date(format='%Y-%m').alias('Year-Month_Date'))

sns.lineplot(x=data1['Year-Month_Date'], y=data1['Anomaly_Month'])
plt.show()