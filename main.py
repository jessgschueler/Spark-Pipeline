from dataframe import df
from  pyspark.sql.functions import abs, round, avg, count

#create column showing diff between Close and Open
df = df.withColumn('Daily_Change', round((df['Close']-df['Open']), 2))
#create column showing diff between High and Low
df = df.withColumn('HighLow_Diff', round((df['High']-df['Low']), 2))
#create bool column if volume > 100
df = df.withColumn("Volume_Over_100", (df['Volume'] > 100))

#create column for absolute value of daily change
df = df.withColumn('Absolute_Change', round(abs(df.Daily_Change), 2))
#compute net sales column
df.withColumn('Net_Sales', ((df.Open + df.High + df.Low + df.Close)/4)*df.Volume)

#avg absolute_change
df.select(avg('Absolute_Change'))
#count of rows where volume < 100
df.select('Volume_Over_100').where(df.Volume_Over_100 == False).count()
