from deepquant.common.base_class import CacheAdapter

class RedisAdapter(CacheAdapter):

    # A constructor of DataSetContext
    # Param: dataset_colsizes - dictionary -> {'price_columns':xx, 'indi_columns':xx, 'feature_columns':xx, 'analysis_log_columns':xx}
    # Param: dataset - type is pandas dataframe
    def __init__(self, dataset_colsizes, dataset):
        self.dataset_colsizes = dataset_colsizes
        self.dataset = dataset

    # Return last row  of dataset containing all columns
    # Output is dataframe
    def get_dataset_last_row(self):
        row_num = len(self.dataset)
        return self.dataset.iloc[row_num - 1:]

    # Return last row of only feature columns
    # Output is numpy array
    def get_feature_last_row(self):
        # Get column size
        price_col_size = len(self.dataset_colsizes['price_columns'])
        indi_col_size = len(self.dataset_colsizes['indi_columns'])
        feature_col_size = len(self.dataset_colsizes['feature_columns'])

        # Calculate start and last columns of feature
        start_col = price_col_size + indi_col_size + 1
        last_col = price_col_size + indi_col_size + feature_col_size

        # Get total row number
        row_num = len(self.dataset)

        # Get feature columns
        dataset_df = self.dataset.iloc[row_num - 1:]
        dataset_arr = dataset_df.values
        features = dataset_arr[:, start_col:last_col].astype(float)

        return features

    # Return all rows of dataset
    def get_dataset(self):
        return self.dataset

    # ลบ row แรกของ dataset
    def drop_first_row(self):
        self.dataset = self.dataset.drop(self.dataset.index[[0]])

    # เพิ่ม row ใหม่เข้าไปใน dataset
    def append_row(self, new_row_df):
        self.drop_first_row()
        self.dataset.append(new_row_df, ignore_index=True)