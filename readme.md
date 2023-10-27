This project focuses on processing and analyzing stock market data using Apache Spark and building a Long Short-Term Memory (LSTM) neural network model for predicting stock price movements. The project utilizes Spark's data processing capabilities to preprocess and engineer features from historical stock data. The engineered features include moving averages, Bollinger Bands, Relative Strength Index (RSI), Moving Average Convergence Divergence (MACD), Stochastic Oscillator, and Commodity Channel Index (CCI).

The LSTM model is developed using PyTorch to predict whether the stock price will move up or down based on the extracted features. The model takes into account the scaled features of the previous 10 rows to make predictions for the next data point. The project involves training and evaluating the LSTM model on both training and test data sections, assessing its accuracy and performance for stock movement prediction.

Key Steps:

1. Data Preprocessing: Spark is employed to preprocess and engineer various technical indicators and features from historical stock data. Sections of data are created to facilitate efficient processing.

2. Feature Engineering: Various technical indicators including moving averages, Bollinger Bands, RSI, MACD, Stochastic Oscillator, and CCI are calculated using the historical stock data.

3. Model Development: An LSTM neural network model is built using PyTorch to predict stock price movements. The model takes scaled features of the last 10 rows as input and predicts the movement class as "up" or "down" for the current row.

4. Training and Evaluation: The LSTM model is trained on the training data sections and evaluated for accuracy and performance on the test data sections. The model's performance is monitored to assess its effectiveness in predicting stock price movements.

Overall, the project combines the power of Apache Spark for efficient data processing and PyTorch for developing a robust LSTM model, enabling accurate predictions for stock price movements.




