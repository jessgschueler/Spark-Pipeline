from dataframe import df
from  pyspark.sql.functions import abs, round, avg, count, year
import matplotlib

#Add columns

#create column showing diff between Close and Open
df = df.withColumn('Daily_Change', round((df['Close']-df['Open']), 2))
#create column showing diff between High and Low
df = df.withColumn('HighLow_Diff', round((df['High']-df['Low']), 2))
#create bool column if volume > 100
df = df.withColumn("Volume_Over_100", (df['Volume'] > 100))
#create column for absolute value of daily change
df = df.withColumn('Absolute_Change', round(abs(df.Daily_Change), 2))

#Computations

#compute net sales column
df.withColumn('Net_Sales', ((df.Open + df.High + df.Low + df.Close)/4)*df.Volume)
#avg absolute_change
df.select(avg('Absolute_Change'))
#count of rows where volume < 100
df.select('Volume_Over_100').where(df.Volume_Over_100 == False).count()
#avg open value and max high
df.agg({'Open': 'avg', 'High': 'max'})

#write to parquet
df.write.parquet('data/coffee_agg.parquet')

#create year column
plot_df = df.withColumn('Year', year(df['Date']))
#create pandas datframe, averageing daily_change by year
pan_df = plot_df.groupBy(plot_df['Year']).agg(avg(plot_df.Daily_Change)).toPandas()
#order column by year
pan_df.sort_values('Year', ascending=True, inplace=True)
#create line and bar graph potting average daily change over the years
pan_df.set_index('Year').plot.line()
pan_df.set_index('Year').plot.bar()