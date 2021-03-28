from flask import current_app
from flask_redis import FlaskRedis
from redis import Redis
import pymongo
import pandas as pd
import numpy as np
try:
   from six.moves import cPickle as pickle
except:
   import pickle

from deepquant.common.dataset_dao import DataSetDAO


# This is a wrapper class, encapsulating session using Redis
class DataSetContext :

    __db_host = None
    __db_port = 0
    __db_name = None

    # A constructor of DataSetContext
    def __init__(self, name, robot_config=None):
        self.__name = name
        self.__robot_config = robot_config

        if robot_config is not None:
            self.__db_host = self.__robot_config['db_host']
            self.__db_port = self.__robot_config['db_port']
            self.__db_name = self.__robot_config['db_name']

        try:
            self.__cache = FlaskRedis(current_app)
        except:
            self.__cache = Redis(host=self.__robot_config['cache_host'], port=self.__robot_config['cache_port'])

    def get_dataset_last_row(self):
        """
        Return last row  of dataset containing all columns

        Returns:
            Dataframe
        """
        df = self.get_dataset()
#         row_num = len(df)
        return df[-1:]

    def get_indicator(self):
        """
        Get only indicator

        Returns:
            DataFrame
        """

        df = self.get_dataset()
        return df['indi'].apply(pd.Series)

    def get_indicator_last_row(self):
        """
        Get last row of indicator

        Return:
            DataFrame
        """
        indi = self.get_indicator()
        return indi[-1:]

    def get_feature(self):
        """
        Get only feature data

        Returns:
            DataFrame
        """
        df = self.get_dataset()
        return df['feat'].apply(pd.Series)


    def get_feature_last_row(self):
        """
        Get last row of feature

        Return:
            DataFrame
        """
        f = self.get_feature()
        return f[-1:]

    def get_dataset(self):
        """
        Return all rows of dataset
        """
        dataset = self.__cache.get(self.__name)
        if dataset is None:
            dataset_dao = DataSetDAO()
            dataset_dao.connect(self.__db_host, self.__db_port, self.__db_name)
            lastdata = dataset_dao.load(self.__robot_config['db_collection_name'], \
                                         self.__robot_config['dataset_session_maxrow'], \
                                        pymongo.DESCENDING)
            lastdata_df = pd.DataFrame(list(lastdata))
            if 'date' in lastdata_df.columns:
                lastdata_df.set_index('date', inplace=True)
            self.set_dataset(lastdata_df)
            dataset = self.__cache.get(self.__name)

        return pickle.loads(dataset)

    def set_dataset(self, dataset):
        """
        """
        self.__cache.set(self.__name, pickle.dumps(dataset, pickle.HIGHEST_PROTOCOL))

    def drop_first_row(self):
        """
        ลบ row แรกของ dataset
        """
        df = self.get_dataset()
        df = df.drop(df.index[[0]])
        self.set_dataset(df)

    def append_row(self, new_row_df):
        """
        เพิ่ม row ใหม่เข้าไปใน dataset
        """
        df = self.get_dataset()
        df = df.append(new_row_df)
        #df.index = pd.to_datetime(df.index, utc=True)
        #df.index = df.index.tz_convert(new_row_df.index.tz)

        self.set_dataset(df)