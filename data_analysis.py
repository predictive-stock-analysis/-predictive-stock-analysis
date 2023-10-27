import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml import Pipeline

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import train_test_split

# Create a Spark session
spark = SparkSession.builder \
    .appName("stock_data_processing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define the path to the stock data folder
path = "stock_data/"

# Initialize an empty list to store sections
sections_list = []

# Function to read the data and create sections
def process_file(file_path):
    df = spark.read.option("header", "true").csv(file_path)
    step_size = 25
    section_length = 150

    window = Window.orderBy(F.monotonically_increasing_id())
    df = df.withColumn("row_num", F.row_number().over(window))

    df = df.withColumn('next5_avg', F.avg('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(1, 5)))
    df = df.withColumn('last5_avg', F.avg('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-4, -0)))

    df = df.withColumn('next10_avg', F.avg('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(1, 10)))
    df = df.withColumn('last10_avg', F.avg('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-9, -0)))

    # Moving Averages (MA)
    df = df.withColumn('ma_5', F.avg('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-4, 0)))
    df = df.withColumn('ma_10', F.avg('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-9, 0)))
    
    # Bollinger Bands
    rolling_std = F.stddev('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-9, 0))
    df = df.withColumn('rolling_std', rolling_std)
    df = df.withColumn('upper_band', F.col('ma_10') + 2 * rolling_std)
    df = df.withColumn('lower_band', F.col('ma_10') - 2 * rolling_std)

    # RSI (Assuming you have the necessary data for calculation)
    rsi_period = 14  # Adjust the period as needed
    price_diff = F.col('price') - F.lag(F.col('price'), 1).over(Window.orderBy(F.monotonically_increasing_id()))
    gain = F.when(price_diff > 0, price_diff).otherwise(0)
    loss = F.when(price_diff < 0, -price_diff).otherwise(0)
    avg_gain = F.avg(gain).over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-rsi_period + 1, 0))
    avg_loss = F.avg(loss).over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-rsi_period + 1, 0))
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    df = df.withColumn('rsi', F.when(avg_loss == 0, 100).otherwise(rsi))

    # MACD (Assuming you have the necessary data for calculation)
    short_term_period = 12  # Adjust as needed
    long_term_period = 26  # Adjust as needed
    signal_period = 9  # Adjust as needed
    ema_short = F.avg('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-short_term_period + 1, 0))
    ema_long = F.avg('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-long_term_period + 1, 0))
    macd = ema_short - ema_long
    signal_line = F.avg(macd).over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-signal_period + 1, 0))
    df = df.withColumn('macd', macd)
    df = df.withColumn('signal_line', signal_line)

    # Stochastic Oscillator (Assuming you have the necessary data for calculation)
    k_period = 14  # Adjust as needed
    d_period = 3  # Adjust as needed
    k_values = 100 * (df['price'] - F.min('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-k_period + 1, 0))) / (
            F.max('price').over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-k_period + 1, 0)) - F.min('price').over(
        Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-k_period + 1, 0)))
    d_values = F.avg(k_values).over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-d_period + 1, 0))
    df = df.withColumn('stochastic_k', k_values)
    df = df.withColumn('stochastic_d', d_values)

    # CCI (Assuming you have the necessary data for calculation)
    cci_period = 20  # Adjust as needed
    typical_price = (df['price'] + df['price'] + df['price']) / 3
    mean_deviation = F.avg(F.abs(typical_price - F.avg(typical_price).over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-cci_period + 1, 0)))
                        ).over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-cci_period + 1, 0))
    cci = (typical_price - F.avg(typical_price).over(Window.orderBy(F.monotonically_increasing_id()).rowsBetween(-cci_period + 1, 0))) / (0.015 * mean_deviation)
    df = df.withColumn('cci', cci)

    #remove rows with nulls
    df = df.na.drop()
    df = df.withColumn("Price", df["Price"].cast("float"))

    # threshold_percentage = 0.1  # Adjust the threshold percentage as needed
    # # Create a column to store the class based on the threshold
    # df = df.withColumn('movement_class', 
    #                    F.when((F.col('next10_avg') - F.col('last10_avg')) >= threshold_percentage * F.col('last10_avg'), 'up')
    #                    .when((F.col('next10_avg') - F.col('last10_avg')) <= -threshold_percentage * F.col('last10_avg'), 'down')
    #                    .otherwise('no_change'))

    indicator_columns = ['Price', 'ma_5', 'ma_10', 'rolling_std', 'upper_band', 'lower_band', 'rsi', 'macd', 'signal_line', 'stochastic_k', 'stochastic_d', 'cci']
    # Create a StandardScaler for normalization
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    # Assemble the features into a single vector
    assembler = VectorAssembler(inputCols=indicator_columns, outputCol="features")
    # Create a pipeline for the transformations
    pipeline = Pipeline(stages=[assembler, scaler])
    # Fit and transform the DataFrame
    df = pipeline.fit(df).transform(df)

    # threshold_percentage = 0.001  # Adjust the threshold percentage as needed
    # Create a column to store the class based on the threshold
    df = df.withColumn('movement_class', 
                       F.when((F.col('next10_avg') - F.col('last10_avg')) > 0 * F.col('last10_avg'), 'up')
                    #    .when((F.col('next10_avg') - F.col('last10_avg')) <= -threshold_percentage * F.col('last10_avg'), 'down'))
                       .otherwise('down'))

    temp_section_list = []
    for i in range(1, df.count()-section_length+2, step_size):
        section = df.filter((F.col("row_num") >= i) & (
            F.col("row_num") < i + section_length))
        temp_section_list.append(section)
    return temp_section_list


# Iterate through the files and process them
for file in os.listdir(path):
    if file.endswith(".csv"):
        file_path = os.path.join(path, file)
        sections_list.extend(process_file(file_path))


# sections_list[54].show(10000)
print(len(sections_list))




class LSTMModel(nn.Module):
    def __init__(self, input_dim, hidden_dim, output_dim):
        super(LSTMModel, self).__init__()
        self.hidden_dim = hidden_dim
        self.lstm = nn.LSTM(input_dim, hidden_dim)
        self.linear = nn.Linear(hidden_dim, output_dim)
        self.sigmoid = nn.Sigmoid()

    def forward(self, inputs):
        lstm_out, _ = self.lstm(inputs.view(len(inputs), 1, -1))
        output = self.linear(lstm_out.view(len(inputs), -1))
        output = self.sigmoid(output)
        return output


indicator_columns = ['Price', 'ma_5', 'ma_10', 'rolling_std', 'upper_band',
                     'lower_band', 'rsi', 'macd', 'signal_line', 'stochastic_k', 'stochastic_d', 'cci']

# Define hyperparameters
input_dim = 10*len(indicator_columns)
hidden_dim = 200
output_dim = 1
learning_rate = 0.01
num_epochs = 10

# Initialize the LSTM model
model = LSTMModel(input_dim, hidden_dim, output_dim)

# Define loss function and optimizer
criterion = nn.BCELoss()
optimizer = optim.SGD(model.parameters(), lr=learning_rate)

# Split the data into training and testing sets
train_data, test_data = train_test_split(
    sections_list, test_size=0.2, random_state=42)

# Train the model
for epoch in range(num_epochs):
    train_len = len(train_data)
    ind = 0
    for section in train_data:
        ind+=1
        print(ind/train_len)
        section_data = section.select(
            'scaled_features', 'movement_class').collect()
        section_length = len(section_data)
        for i in range(9, section_length-1):
            inputs = torch.tensor(
                [row['scaled_features'] for row in section_data[i-9:i+1]], dtype=torch.float32)
            labels = torch.tensor(
                [[1 if section_data[i+1]['movement_class'] == 'up' else 0]], dtype=torch.float32)

            optimizer.zero_grad()
            outputs = model(inputs.unsqueeze(0))
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

        # if (epoch + 1) % 10 == 0:
        print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.4f}')


# Evaluate the model on the test data
with torch.no_grad():
    for section in test_data:
        section_data = section.select('scaled_features', 'movement_class').collect()
        section_length = len(section_data)
        correct_predictions = 0
        total_samples = 0
        for i in range(9, section_length):
            inputs = torch.tensor([row['scaled_features'] for row in section_data[i-9:i+1]], dtype=torch.float32)
            labels = torch.tensor([1 if section_data[i]['movement_class'] == 'up' else 0], dtype=torch.float32)

            outputs = model(inputs.unsqueeze(0))
            predicted = outputs.ge(0.5).view(-1)
            correct_predictions += (predicted == labels).sum().item()
            total_samples += labels.size(0)

        accuracy = correct_predictions / total_samples
        print(f'Accuracy: {accuracy:.4f}')


# Stop the Spark session
spark.stop()
