import math
import matplotlib.pyplot as plt
import pathlib
import os
import keras
import pandas as pd
import numpy as np
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Dropout
from keras.layers import *
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.utils import shuffle
from keras.callbacks import EarlyStopping
from keras.models import load_model

training_data_folder = "training_data"
model_folder = "models"
fund_data_folder = "fundamental_info"


def load_csv(file):
    df = pd.read_csv(file)
    return df

#   Separate month1 from month2
def partition_df(df, pred_count):
    partition = len(df) - pred_count
    training_set = df.iloc[:partition, 3:4].values
    return [partition, training_set]


def scale(training_set):
    # Feature Scaling
    sc = MinMaxScaler(feature_range=(0, 1))
    training_set_scaled = sc.fit_transform(training_set)
    return [training_set_scaled, sc]

#   Format data to feed the LSTM
def create_train_data(steps, training_set):
    X_train = []
    y_train = []
    for i in range(steps, len(training_set)):
        X_train.append(training_set[i - steps:i, 0])
        y_train.append(training_set[i, 0])
    X_train, y_train = np.array(X_train), np.array(y_train)
    X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))
    return [X_train, y_train]

#   Build LSTM and train it
def build_model(xtrain, ytrain):
    model = Sequential()
    # Adding the first LSTM layer and some Dropout regularisation
    model.add(LSTM(units=50, return_sequences=True, input_shape=(xtrain.shape[1], 1)))
    model.add(Dropout(0.2))
    # Adding a second LSTM layer and some Dropout regularisation
    model.add(LSTM(units=50, return_sequences=True))
    model.add(Dropout(0.2))
    # Adding a third LSTM layer and some Dropout regularisation
    model.add(LSTM(units=50, return_sequences=True))
    model.add(Dropout(0.2))
    # Adding a fourth LSTM layer and some Dropout regularisation
    model.add(LSTM(units=50))
    model.add(Dropout(0.2))
    # Adding the output layer
    model.add(Dense(units=1))
    # Compiling the RNN
    model.compile(optimizer='adam', loss='mean_squared_error')
    # Fitting the RNN to the Training set
    model.fit(xtrain, ytrain, epochs=100, batch_size=60, validation_split=0.1, verbose=0)
    return model

def retrain_model(model, xtrain, ytrain):
    model.fit(xtrain, ytrain, epochs=100, batch_size=60, validation_split=0.1, verbose=0)
    return model

#   For month2, include previous day's output as input for the next day
def predict_30_days(df, sc, model, steps, pred_count):
    partition = len(df) - pred_count
    x_test = df.iloc[partition - steps: partition, 3:4].values
    y_test = df.iloc[partition:len(df), 3:4]
    x_test = x_test.reshape(-1, 1)
    x_test = sc.transform(x_test)

    pred = []

    for i in range(0, pred_count):
        inputs = []
        inputs.append(x_test[:, :])
        inputs = np.array(inputs)
        inputs = np.reshape(inputs, (inputs.shape[0], inputs.shape[1], 1))
        prev = model.predict(inputs)[0]
        pred.append(prev[0])
        x_test = np.append(x_test[1:], prev)
        x_test = x_test.reshape(-1, 1)

    pred = np.array(pred)
    pred = pred.reshape(-1, 1)
    pred = sc.inverse_transform(pred)
    return [y_test, pred]

#   We're gonna trade only top 200 traded stocks
def get_top_stocks(write_dir):
    top = []
    src_dir = os.path.join(write_dir, fund_data_folder)
    for file in pathlib.Path(src_dir).iterdir():
        df = pd.read_csv(file, sep=",")
        top_100 = df['Ticker'][:200]
        top = list(top_100)
        break
    return top

#   Iterate all the stock files
#   Train the LSTM on all these infos
#   Predict the output for month2 by making output as input
def build_lstm(year, month1, month2, write_dir, pred_count):
    steps = 30
    model_file_name = 'lstm_30_model.h5'
    src_dir = os.path.join(write_dir, training_data_folder)
    model_file = os.path.join(os.path.join(write_dir, model_folder), model_file_name)

    top = get_top_stocks(write_dir)

    for file in pathlib.Path(src_dir).iterdir():
        symb = file.stem
        if not (symb in top):
            continue
        df = load_csv(file)
        partition, train_set = partition_df(df, pred_count)
        train_set_scaled, sc = scale(train_set)
        xtrain, ytrain = create_train_data(steps, train_set_scaled)
        if os.path.exists(model_file):
            model = load_model(model_file)
            model = retrain_model(model, xtrain, ytrain)
        else:
            model = build_model(xtrain, ytrain)
        model.save(model_file)
        y_test, pred = predict_30_days(df, sc, model, steps, pred_count)
        # plot_graph(y_test, pred, symb)
        lstm_out = [np.nan for i in range(0, partition)] + list(pred.flatten())
        df['LSTM_Y'] = lstm_out
        df.to_csv(file, mode='w', encoding='utf-8', index=False)
