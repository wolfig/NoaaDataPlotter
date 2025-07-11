#About this project
This is a simple Python program allowing to fetch data from the NOAA weather data REST API and to plot the data in a chart.

#How to use?
1. Search for a weather station using for example the NOAA search web page: https://www.ncei.noaa.gov/cdo-web/search
2. Select the station from the search results and click on "details". the detail page will show you the available data for this station and the station ID
3. Copy-paste the station ID into the program code (top of "main" method)
4. Choose an option to plot:
   * TMAX_RAW: original time series of daily maximum temperatures provided in column TMAX
   * TMIN_RAW: original time series of daily minimum temperatures provided in column TMIN
   * TAVG_RAW: original time series of daily averaged temperatures provided in column TAVG
   * TMAX: maximum temperature of each year (via aggregation function)
   * TMIN: minimum temperature of each year (via aggregation function)
   * TAVG: maximum averaged temperature of each year (via aggregation function)
   * TMAX_mean: average of all daily maximum temperatures of one year
   * TMIN_mean: average of all daily minimum temperatures of one year
   * TAVG_mean: average of all daily average temperatures of one year
