from dataframe import df

#create column showing diff between Close and Open
df = df.withColumn('Daily_Change', (df['Close']-df['Open']))
#create column showing diff between High and Low
df = df.withColumn('HighLow_Diff', (df['High']-df['Low']))
#create bool column if volume > 100
df = df.withColumn("Volume_Over_100", (df['Volume'] > 100))

