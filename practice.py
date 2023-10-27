import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml import Pipeline

# Create a Spark session
spark = SparkSession.builder \
    .appName("stock_data_processing") \
    .getOrCreate()

# Define the path to the stock data folder
path = "stock_data/"

# Initialize an empty list to store sections
sections_list = []

# Function to read the data and create sections
def process_file(file_path):
    df_list = []

    # Loop through the files in the folder
    for file in os.listdir(path):
        if file.endswith(".csv"):
            file_path = os.path.join(path, file)
            df = spark.read.option("header", "true").csv(file_path)
            df = df.withColumn("file_name", F.lit(file))  # Add a column with the filename
            df_list.append(df)

    # Concatenate all DataFrames into a single DataFrame
    df = df_list[0]  # Assuming at least one DataFrame exists
    for i in range(1, len(df_list)):
        df = df.union(df_list[i])

    window = Window.partitionBy('file_name').orderBy('Time')
    # df = spark.read.option("header", "true").csv(file_path)
    step_size = 50
    section_length = 100

    # df = df.withColumn("row_num", F.row_number().over(window))

    df = df.withColumn('next5_avg', F.avg('price').over(window.rowsBetween(1, 5)))
    df = df.withColumn('last5_avg', F.avg('price').over(window.rowsBetween(-4, -0)))

    df = df.withColumn('next10_avg', F.avg('price').over(window.rowsBetween(1, 10)))
    df = df.withColumn('last10_avg', F.avg('price').over(window.rowsBetween(-9, -0)))

    # Moving Averages (MA)
    df = df.withColumn('ma_5', F.avg('price').over(window.rowsBetween(-4, 0)))
    df = df.withColumn('ma_10', F.avg('price').over(window.rowsBetween(-9, 0)))
    
    # Bollinger Bands
    rolling_std = F.stddev('price').over(window.rowsBetween(-9, 0))
    df = df.withColumn('rolling_std', rolling_std)
    df = df.withColumn('upper_band', F.col('ma_10') + 2 * rolling_std)
    df = df.withColumn('lower_band', F.col('ma_10') - 2 * rolling_std)

    # RSI (Assuming you have the necessary data for calculation)
    rsi_period = 14  # Adjust the period as needed
    price_diff = F.col('price') - F.lag(F.col('price'), 1).over(window)
    gain = F.when(price_diff > 0, price_diff).otherwise(0)
    loss = F.when(price_diff < 0, -price_diff).otherwise(0)
    avg_gain = F.avg(gain).over(window.rowsBetween(-rsi_period + 1, 0))
    avg_loss = F.avg(loss).over(window.rowsBetween(-rsi_period + 1, 0))
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    df = df.withColumn('rsi', F.when(avg_loss == 0, 100).otherwise(rsi))

    # MACD (Assuming you have the necessary data for calculation)
    short_term_period = 12  # Adjust as needed
    long_term_period = 26  # Adjust as needed
    signal_period = 9  # Adjust as needed
    ema_short = F.avg('price').over(window.rowsBetween(-short_term_period + 1, 0))
    ema_long = F.avg('price').over(window.rowsBetween(-long_term_period + 1, 0))
    macd = ema_short - ema_long
    signal_line = F.avg(macd).over(window.rowsBetween(-signal_period + 1, 0))
    df = df.withColumn('macd', macd)
    df = df.withColumn('signal_line', signal_line)

    # Stochastic Oscillator (Assuming you have the necessary data for calculation)
    k_period = 14  # Adjust as needed
    d_period = 3  # Adjust as needed
    k_values = 100 * (df['price'] - F.min('price').over(window.rowsBetween(-k_period + 1, 0))) / (
            F.max('price').over(window.rowsBetween(-k_period + 1, 0)) - F.min('price').over(
        window.rowsBetween(-k_period + 1, 0)))
    d_values = F.avg(k_values).over(window.rowsBetween(-d_period + 1, 0))
    df = df.withColumn('stochastic_k', k_values)
    df = df.withColumn('stochastic_d', d_values)

    # CCI (Assuming you have the necessary data for calculation)
    cci_period = 20  # Adjust as needed
    typical_price = (df['price'] + df['price'] + df['price']) / 3
    mean_deviation = F.avg(F.abs(typical_price - F.avg(typical_price).over(window.rowsBetween(-cci_period + 1, 0)))
                        ).over(window.rowsBetween(-cci_period + 1, 0))
    cci = (typical_price - F.avg(typical_price).over(window.rowsBetween(-cci_period + 1, 0))) / (0.015 * mean_deviation)
    df = df.withColumn('cci', cci)

    #remove rows with nulls
    df = df.na.drop()

    indicator_columns = ['Price', 'ma_5', 'ma_10', 'rolling_std', 'upper_band', 'lower_band', 'rsi', 'macd', 'signal_line', 'stochastic_k', 'stochastic_d', 'cci']
    # Create a StandardScaler for normalization
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    # Assemble the features into a single vector
    assembler = VectorAssembler(inputCols=indicator_columns, outputCol="features")
    # Create a pipeline for the transformations
    pipeline = Pipeline(stages=[assembler, scaler])
    # Fit and transform the DataFrame
    df = pipeline.fit(df).transform(df)


    df = df.withColumn('section_id', F.dense_rank().over(window))
    temp_list = []
    for i in range(1, df.select(F.max("section_id")).first()[0]):
        section = df.filter(F.col("section_id") == i)
        temp_list.append(section)

    # temp_section_list = []
    for temp in temp_list:
        for i in range(1, temp.count()-section_length+2, step_size):
            section = temp.filter((F.col("row_num") >= i) & (
                F.col("row_num") < i + section_length))
            sections_list.append(section)
    # return temp_section_list






process_file(path)



# # Iterate through the files and process them
# for file in os.listdir(path):
#     if file.endswith(".csv"):
#         file_path = os.path.join(path, file)
#         sections_list.extend(process_file(file_path))

print(len(sections_list))
sections_list[-1].show(10000)



# Stop the Spark session
spark.stop()
