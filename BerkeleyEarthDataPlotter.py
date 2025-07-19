import calendar
import locale
import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns

def clean_data_file(file_name: str):
    file_raw = open(file_name, 'r', encoding='ISO-8859-1') # Berkeley-Earth uses non-UTF8 characters
    comment_counter = 0
    output_string = ''
    next_lines_contain_reference = False
    reference_temperatures = []

    is_comment = True

    for line_raw in file_raw.readlines():
        if line_raw.startswith('%'):
            comment_counter += 1
            if next_lines_contain_reference:
                reference_temperatures = extract_monthly_reference(line_raw)
                next_lines_contain_reference = False
            if line_raw.replace(' ', '').strip() == '%JanFebMarAprMayJunJulAugSepOctNovDec':
                next_lines_contain_reference = True

            output_string = ''.join([output_string, line_raw])
        else:
            if is_comment:
                output_string = ''.join([output_string, 'YearMonth;AnomalyMonth;AnomalyMonthErr;AnomalyYear;AnomalyYearErr;AnomalyFive;AnomalyFiveErr;AnomlyTen;AnomalyTenErr;AnomalyTwenty;AnomalyTwentyErr\n'])
                is_comment = False
            processed_line  = line_raw.replace('    ', ' ') \
                    .replace('   ', ' ') \
                    .replace('  ', ' ') \
                    .lstrip() \
                    .replace(' ', ';') \
                    .replace(';1;', '-01;') \
                    .replace(';2;', '-02;') \
                    .replace(';3;', '-03;') \
                    .replace(';4;', '-04;') \
                    .replace(';5;', '-05;') \
                    .replace(';6;', '-06;') \
                    .replace(';7;', '-07;') \
                    .replace(';8;', '-08;') \
                    .replace(';9;', '-09;') \
                    .replace(';10;', '-10;') \
                    .replace(';11;', '-11;') \
                    .replace(';12;', '-12;')

            output_string = ''.join([output_string, processed_line])

    file_raw.close()

    tempfile = open('tmp.csv', 'w')
    tempfile.write(output_string)
    tempfile.close()

    return comment_counter, reference_temperatures

def extract_monthly_reference(reference_line_raw: str) -> list[float]:
    reference_line_processed = reference_line_raw.replace('%', '').replace('    ', ' ').replace('   ', ' ').replace('  ', ' ').lstrip()
    reference_data = reference_line_processed.split()
    return list(map(float, reference_data))

def open_file_and_process(comment_count: int, reference_temperatures: list[float]) -> pl.DataFrame:
    data_frame = pl.read_csv('tmp.csv', has_header=True, separator=';', skip_rows=comment_count)
    # Stri g Datum in echtes Datum umwandeln
    data_frame = data_frame.with_columns(pl.col('YearMonth').str.to_date(format='%Y-%m').alias('date'))
    data_frame = data_frame.with_columns(pl.col('date').dt.year().alias('year'))
    data_frame = data_frame.with_columns(pl.col('date').dt.month().alias('month'))
    # Berkeley-Earth gibt die monatlichen Daten als Anomalie zum Wert der Referenzperiode an ->
    # Referenzwert wieder dazuaddieren, um Absolutwert zu bekommen
    # monatlicher Referenzwert
    data_frame = data_frame.with_columns(data_frame.map_rows(lambda row: row[1] + reference_temperatures[row[13] - 1])).rename({'map': 'MonthlyAbs'})
    # Referenzwert über ganze Referenzperiode
    data_frame = data_frame.with_columns(data_frame.map_rows(lambda row: row[3] + 8.06)).rename({'map': 'YearlyAbs'})
    return data_frame

def plot_monthly_data(data_frame: pl.DataFrame, city_name: str):
    plt.figure(figsize=(14, 8))
    for month in range(1, 13):
        data_for_plot = data_frame.filter(pl.col('month') == month)

        reference_mean1 = data_for_plot.filter((pl.col('year') >= 1961) & (pl.col('year') <= 2010))['MonthlyAbs'].mean()
        data_for_plot = data_for_plot.with_columns(data_for_plot.map_rows(lambda row: row[15] - reference_mean1)).rename({'map': 'difference'})

        data_rolling = data_for_plot.rolling('year', period='10i').agg([pl.col('date').last(), pl.col(observable_column).mean()])

        plt.figure(figsize=(14, 8))
        sns.lineplot(data=data_for_plot, x='date', y=observable_column, alpha=0.5, drawstyle='steps', label='Berkeley-Earth, Monatsmittel')
        sns.lineplot(data=data_rolling[10:], x='date', y=observable_column, label='Berkeley-Earth, rollend')
        plt.xlabel('Jahr')
        plt.ylabel('Temperatur [°C]')
        plt.title(city_name + ', monatlicher Temperatur-Mittelwert für Monat: ' + calendar.month_name[month])
        plt.legend(loc="upper left")
        plt.grid()
        plt.show()
        plt.close()


if __name__ == '__main__':

    sns.set_context('talk')
    file_name = 'Genf_45.81N-5.77E-TAVG-Trend.txt'
    observable_column = 'AnomalyMonth'

    file_path = 'Daten/BerkeleyEarth/' + file_name

    locale.setlocale(locale.LC_ALL, 'de_DE')
    comment_count, reference_temperatures = clean_data_file(file_path)

    data_frame = open_file_and_process(comment_count, reference_temperatures)
    city_name = file_name.split('_')[0]
    plot_monthly_data(data_frame, city_name)
