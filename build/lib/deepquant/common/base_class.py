from abc import ABC, abstractmethod

# An abstract class for handling the commission
class BaseCommission(ABC):

    # An abstract method to return commission rate corresponding to specified 'pos_size'
    # pos_size - position size, data type is float
    @abstractmethod
    def get_comm_rate(self, pos_size):
        pass


class CacheAdapter(ABC):
    # Return last row  of dataset containing all columns
    # Output is dataframe
    @abstractmethod
    def get_dataset_last_row(self):
        pass

    # Return last row  of dataset containing all columns
    # Output is dataframe
    @abstractmethod
    def get_dataset_last_row(self):
        pass

    # Return last row of only feature columns
    # Output is numpy array
    @abstractmethod
    def get_feature_last_row(self):
        pass

    # Return all rows of dataset
    @abstractmethod
    def get_dataset(self):
        pass

    # ลบ row แรกของ dataset
    @abstractmethod
    def drop_first_row(self):
        pass

    # เพิ่ม row ใหม่เข้าไปใน dataset
    @abstractmethod
    def append_row(self, new_row_df):
        pass