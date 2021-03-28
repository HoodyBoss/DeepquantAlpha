"""
A code template for simple Deep Neural Network (DNN) model.
You can perform training, evaluating, testing, predicting.
"""

import numpy as np
import pandas as pd
from keras.utils import np_utils
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Dropout
from keras.regularizers import l2
from keras.wrappers.scikit_learn import KerasClassifier
from keras.optimizers import Adam
from keras.optimizers import SGD
from keras.optimizers import Adadelta
from keras.constraints import maxnorm
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import make_scorer
from sklearn.metrics import accuracy_score

from keras.models import model_from_json

from keras.layers import merge
from keras.layers.core import Lambda
from keras.models import Model
import datetime as dt
import keras
import tensorflow as tf
# Memory Management
from keras.backend.tensorflow_backend import set_session

import matplotlib.pyplot as plt
import matplotlib


class DNN_Train():

    def __init__(self, dataset_files, feat_start_colname, class_num, drop_cols=None):
        config = tf.ConfigProto()
        config.gpu_options.allow_growth = True
        config.gpu_options.allocator_type = 'BFC'
        # NOT USE THIS LINE config.gpu_options.per_process_gpu_memory_fraction = 0.3
        set_session(tf.Session(config=config))

        self.class_num = class_num

        # load dataset from file(s)
        self.df, self.dataset = self.load_dataset(dataset_files, drop_cols)

        self.feat_start_col_idx = self.df.columns.get_loc(feat_start_colname)
        self.feat_end_col_idx = self.df.shape[1] - 1
        self.label_col_idx = 0

        self.datetime_same_col = False
        # find datetime column
        if 'DATETIME' in self.df.columns or 'datetime' in self.df.columns:
            self.datetime_same_col = True

        self.has_volume_col = False
        self.last_price_col = -1
        if 'VOLUME' in self.df.columns or 'volume' in self.df.columns:
            self.has_volume_col = True
            self.last_price_col = 6 if self.datetime_same_col == True else 7
        else:
            self.last_price_col = 5 if self.datetime_same_col == True else 6

        self.start_row = 0  # row 0 is header
        self.last_row = self.dataset.shape[0]
        print('start row of dataset is {}'.format(self.last_row))
        print('last row of dataset is {}'.format(self.last_row))

        self.column_size = self.dataset.shape[1]
        self.input_size = (self.feat_end_col_idx - self.feat_start_col_idx) + 1

        # split to features and label datasets
        self.feat_dataset = self.dataset[self.start_row:self.last_row, self.feat_start_col_idx:self.feat_end_col_idx + 1].astype(float)
        self.label_col = self.dataset[self.start_row:self.last_row, self.label_col_idx]
        print('shape of dataset is {}'.format(self.feat_dataset.shape))
        print('shape of label is {}'.format(self.label_col.shape))


    def load_dataset(self, dataset_files, drop_cols=None):
        # load training datasets
        if len(dataset_files) > 0:
            df = pd.read_csv(dataset_files[0])

        if len(dataset_files) > 1:
            for i in range(1, len(dataset_files)):
                df_next = pd.read_csv(dataset_files[i])
                df = df.append(df_next, ignore_index=True)

        df = self.drop_column(df, drop_cols)
        dataset = df.to_numpy()
        print('shape of dataset is {}'.format(dataset.shape))
        return  df, dataset


    def drop_column(self, df, drop_cols):
        if drop_cols is not None and len(drop_cols) > 0:
            original_col = df.columns
            drop_col = list()
            for i in range(0, len(original_col)):
                if original_col[i] not in drop_cols:
                    drop_col.append(original_col[i])
            df = df.drop(drop_col, axis=1)
        return df


    # Calculate accuracy
    def get_accuracy(self, testSet, predictions):
        correct = 0
        for i in range(len(testSet)):
            if testSet[i] == predictions[i]:
                correct += 1
        return (correct / float(len(testSet))) * 100.0


    # ******************************************************************************************************************************
    # ==============================================================================================================================
    # Create model
    def create_model(self):
        model = Sequential()
        """
        model.add(Dense(600, input_dim=intput_size, activation='relu', W_regularizer=l2(0.00001), kernel_constraint=maxnorm(3)))
        model.add(Dropout(0.3))
        model.add(Dense(600, activation='relu', W_regularizer=l2(0.00001), kernel_constraint=maxnorm(3)))
        model.add(Dropout(0.3))
        model.add(Dense(600, activation='relu', W_regularizer=l2(0.00001), kernel_constraint=maxnorm(3)))
        model.add(Dropout(0.3))
        model.add(Dense(3, activation='softmax', W_regularizer=l2(0.00001)))
        """
        model.add(Dense(200, input_dim=self.input_size, activation='relu', kernel_regularizer=l2(0.00001)))
        model.add(Dense(200, activation='relu', kernel_regularizer=l2(0.00001)))
        model.add(Dense(200, activation='relu', kernel_regularizer=l2(0.00001)))
        model.add(Dense(3, activation='softmax', kernel_regularizer=l2(0.00001)))

        optimizer = Adam(lr=0.0001, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
        # optimizer = SGD(lr=0.001, momentum=0.9, decay=0.01, nesterov=True)
        # optimizer = Adadelta(lr=2.0, rho=0.99, epsilon=1e-08, decay=0.0)

        model.compile(loss='categorical_crossentropy', optimizer=optimizer, metrics=['accuracy'])
        return model


    def save_model(self, model, filename):
        # save model by serializing to JSON
        model_json = model.to_json()
        with open(filename + '.json', 'w') as json_file:
            json_file.write(model_json)
        # save weights by serializing to HDF5
        model.save_weights(filename + '.h5')
        print("Saved model and weights to disk successfully")


    # ******************************************************************************************************************************
    # ==============================================================================================================================
    def train(self, model_file, show_graph=False):
        encoder = LabelEncoder()
        encoder.fit(self.label_col)
        encoded_labels = encoder.transform(self.label_col)
        one_hot_labels = np_utils.to_categorical(encoded_labels, num_classes=self.class_num)

        model = self.create_model()

        seed = 999
        np.random.seed(seed)

        batch_size = 100
        epochs = 2

        # Fit the model
        history = model.fit(self.feat_dataset, one_hot_labels, epochs=epochs, batch_size=batch_size, verbose=1)
        # evaluate the model
        scores = model.evaluate(self.feat_dataset, one_hot_labels, verbose=0)
        print("\n%s: %.2f%%" % (model.metrics_names[1], scores[1] * 100))

        # save model by serializing to JSON and save weights to HDFS
        self.save_model(model, model_file)

        if show_graph == True:
            self.show_graph(history)


    def train_gridsearch(self, param_dict, model_file):
        seed = 999
        np.random.seed(seed)

        batch_size = param_dict['batch_size']
        epochs = param_dict['epochs']
        param_grid = dict(batch_size=batch_size, epochs=epochs)

        classifier = KerasClassifier(build_fn=self.create_model, verbose=0)
        grid = GridSearchCV(estimator=classifier, param_grid=param_grid \
                            , scoring='accuracy', n_jobs=-1, cv=3, return_train_score=True)
        grid_result = grid.fit(self.feat_dataset, self.label_col)

        # summarize results
        print("Best: {} using {}".format(grid_result.best_score_, grid_result.best_params_))
        means = grid_result.cv_results_['mean_test_score']
        stds = grid_result.cv_results_['std_test_score']
        params = grid_result.cv_results_['params']
        for mean, stdev, param in zip(means, stds, params):
            print("{} ({}) with: {}".format(mean, stdev, param))

        best_model = grid.best_estimator_.model
        # save model by serializing to JSON and save weights to HDFS
        self.save_model(best_model, model_file)


    def train_splitvalidate(self, model_file, show_graph):
        encoder = LabelEncoder()
        encoder.fit(self.label_col)
        encoded_labels = encoder.transform(self.label_col)
        one_hot_labels = np_utils.to_categorical(encoded_labels, num_classes=self.class_num)

        model = self.create_model()

        seed = 999
        np.random.seed(seed)

        batch_size = 100
        epochs = 2

        history = model.fit(self.feat_dataset, one_hot_labels, validation_split=0.33, epochs=epochs, batch_size=batch_size, verbose=1)
        scores = model.evaluate(self.feat_dataset, one_hot_labels, batch_size=batch_size)
        print("\n%s: %.2f%%" % (model.metrics_names[1], scores[1] * 100))

        # save model by serializing to JSON and save weights to HDFS
        self.save_model(model, model_file)

        if show_graph == True:
            self.show_graph(history)


    def eval_kfold(self):
        seed = 999
        np.random.seed(seed)

        batch_size = 100
        epochs = 1
        fold_num = 3

        classifier = KerasClassifier(build_fn=self.create_model, epochs=epochs, batch_size=batch_size, verbose=1)
        # evaluate using K-fold cross validation
        kfold = StratifiedKFold(n_splits=fold_num, shuffle=True, random_state=seed)
        results = cross_val_score(classifier, self.feat_dataset, self.label_col, cv=kfold)
        print('\nAccuracy: %.2f%%' % (results.mean() * 100))


    def eval_kfold_using_for(self):
        encoder = LabelEncoder()
        encoder.fit(self.label_col)
        encoded_labels = encoder.transform(self.label_col)
        one_hot_labels = np_utils.to_categorical(encoded_labels, num_classes=self.class_num)

        seed = 999
        np.random.seed(seed)

        batch_size = 1000
        epochs = 10
        fold_num = 10

        # train model
        print('Start training', dt.datetime.now())
        kfold = StratifiedKFold(n_splits=fold_num, shuffle=True, random_state=seed)
        cvscores = []

        for train, test in kfold.split(self.feat_dataset, self.label_col):
            # create model
            model = self.create_model()
            # Fit the model
            model.fit(self.feat_dataset[train], one_hot_labels[train], epochs=epochs, batch_size=batch_size, verbose=1)
            # evaluate the model
            scores = model.evaluate(self.feat_dataset[test], one_hot_labels[test], verbose=1)
            print("%s: %.2f%%" % (model.metrics_names[1], scores[1] * 100))
            cvscores.append(scores[1] * 100)

        print("%.2f%% (+/- %.2f%%)" % (np.mean(cvscores), np.std(cvscores)))
        print('\n')
        print('Finished training', dt.datetime.now())


    # ******************************************************************************************************************************
    # ==============================================================================================================================
    def test(self, model_file):
        encoder = LabelEncoder()
        encoder.fit(self.label_col)
        encoded_labels = encoder.transform(self.label_col)
        one_hot_labels = np_utils.to_categorical(encoded_labels, num_classes=3)

        # load model from file
        model = self.load_model(model_file)

        # compile loaded model
        optimizer = Adam(lr=0.0001, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
        model.compile(loss='categorical_crossentropy', optimizer=optimizer, metrics=['accuracy'])

        # evaluate the model
        scores = model.evaluate(self.feat_dataset, one_hot_labels, verbose=1)
        print("%s: %.2f%%" % (model.metrics_names[1], scores[1] * 100))
        print('Finished testing', dt.datetime.now())


    def predict(self, model_file, classes, has_label_col=False):
        encoder = LabelEncoder()
        encoder.fit(classes)

        # load model from file
        model = self.load_model(model_file)

        # compile loaded model
        optimizer = Adam(lr=0.0001, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
        model.compile(loss='categorical_crossentropy', optimizer=optimizer, metrics=['accuracy'])

        # Test
        print('Start predicting', dt.datetime.now())
        predictions = model.predict(self.feat_dataset)
        # calculate predictions
        predicted_labels = encoder.inverse_transform(np.argmax(predictions, 1))

        # get price columns (datetime, open, high, low, close, volume)
        start_col = 0 if has_label_col == False else 1
        df_price = self.df.iloc[0:self.last_row, start_col:self.last_price_col]

        # Adjust time format by append '0' to the left side
        if self.datetime_same_col == False:
            df_price.iloc[:, 2] = df_price.iloc[:, 2].astype(str).str.zfill(6)

        print("shape of df_price['DATETIME'] is {}".format(df_price['DATETIME'].shape))
        print("shape of predicted_labels is {}".format(predicted_labels.shape))
        print(df_price['DATETIME'][0])
        print(predicted_labels[0])

        df_price['SIGNAL'] = predicted_labels
        predicted_file = model_file + '_Predicted.csv'
        df_price.to_csv(predicted_file, index=False)
        print('Finished testing', dt.datetime.now())


    # ******************************************************************************************************************************
    # ==============================================================================================================================
    def show_graph(self, history):
        matplotlib.use("macOSX")

        # list all data in history
        print(history.history.keys())
        # summarize history for accuracy
        plt.plot(history.history['acc'])
        plt.title('model accuracy')
        plt.ylabel('accuracy')
        plt.xlabel('epoch')
        plt.legend(['train', 'test'], loc='upper left')
        plt.show()
        # summarize history for loss
        plt.plot(history.history['loss'])
        plt.title('model loss')
        plt.ylabel('loss')
        plt.xlabel('epoch')
        plt.legend(['train', 'test'], loc='upper left')
        plt.show()


    # ******************************************************************************************************************************
    # ==============================================================================================================================
    def load_model(self, model_file):
        # load json and create model
        json_file = open(model_file + '.json', 'r')
        loaded_model_json = json_file.read()
        json_file.close()
        model = model_from_json(loaded_model_json)
        # load weights into new model
        model.load_weights(model_file + '.h5')
        print('Loaded model from disk successfully')
        return model


    def load_model_keras(self, model_file):
        model = keras.models.load_model(model_file + '.model')
        return model


# ******************************************************************************************************************************
# ==============================================================================================================================
# Set and Run

feat_start_colname = 'CHANNEL_COLOR_CODE'
drop_cols   = [] # list of columns to be deleted
class_num   = 3
file_path   = '/Users/minimalist/Google Drive/Model/FOREX/barracuda/mark_label/'
model_path  = '/Users/minimalist/Google Drive/Model/FOREX/barracuda/mark_label/mlmodels/'
model_file  = model_path + 'gold_barracuda_sig' # output filename (model file)
model_num   = 1

dataset_files = [file_path + 'XAUUSD_M5_Sig_Train.csv']

# Train
# ถ้าจะ train model ให้เอาคอมเม้นต์ (#) หน้าบรรทัดที่ต้องการออก
# แล้วถ้าต้องการ test และ predict ให้เอาคอมเม้นต์ (#) หน้าบรรทัดในส่วน Test และ Predict ออกให้หมดทุกบรรทัด
print("=========================================================================================")
dnn_train = DNN_Train(dataset_files, feat_start_colname, class_num, drop_cols)
#dnn_train.train(model_file + str(model_num), show_graph=False)
#dnn_train.train_splitvalidate(model_file + str(model_num), show_graph=True)
dnn_train.train_gridsearch({'batch_size':[100], 'epochs':[2, 3]}, model_file + str(model_num))


# Evaluate model
# ถ้าจะ evaluate model ให้เอาคอมเม้นต์ (#) หน้าบรรทัดที่ต้องการออก
# แล้วใส่คอมเม้นต์ (#) ปิดทุกบรรทัดในส่วน Test และ Predict
print("=========================================================================================")
#dnn_train = DNN_Train(dataset_files, feat_start_colname, class_num, drop_cols)
#dnn_train.eval_kfold()
#dnn_train.eval_kfold_using_for()


# Test
print("=========================================================================================")
testdata_files = [file_path + 'XAUUSD_M5_Sig_Test.csv']
dnn_test = DNN_Train(testdata_files, feat_start_colname, class_num, drop_cols)
dnn_test.test(model_file + str(model_num))


# Predict
print("=========================================================================================")
testdata_files = [file_path + 'XAUUSD_M5_Sig_Test.csv']
dnn_predict = DNN_Train(testdata_files, feat_start_colname, class_num, drop_cols)
classes = [0, 1, 2]
dnn_predict.predict(model_file + str(model_num), classes, has_label_col=True)

