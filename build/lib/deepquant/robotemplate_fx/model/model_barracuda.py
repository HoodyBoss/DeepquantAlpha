import pandas as pd
import numpy as np
import deepquant.common.error as error
import deepquant.common.state_machine as state_machine
from sklearn.preprocessing import LabelEncoder


class PredictiveModel:
    sm = state_machine.StateMachine()

    def __int__(self, model_name, ml_model):
        self.model_name = model_name
        self.ml_model = ml_model

    def predict(self, dataset, feat_first_col=None, feat_last_col=None, bar='all'):
        """
        Predict signal
        :param dataset: a DataFrame
        :param feat_first_col: a first feature column number in DataFrame
        :param feat_last_col: a last feature column number in DataFrame
        :param bar: 'all' = all rows (bars), 'last' = only last row (bar)
        :return:
        """
        try:
            first_col = feat_first_col if feat_first_col is not None else 7
            last_col = feat_last_col if feat_last_col is not None else len(dataset.columns)

            # This is an example code
            # calculate predictions, data type of predicted_labels is numpy array

            # prepare one hot encoding
            labels = np.array([1.0, 2.0, 3.0])
            encoder = LabelEncoder()
            encoder.fit(labels)

            # -1 means predict only last row (current bar data)
            if bar == 'all':
                features = dataset[:, first_col:last_col].astype(float)
            elif bar == 'last':
                features = dataset[-1, first_col:last_col].astype(float)

            # Predict and inverse one hot encoding to label
            predictions = self.ml_model.predict(features)
            predicted_signals = encoder.inverse_transform(np.argmax(predictions, 1))
            signals = int(predicted_signals[:]) if bar == 'all' else int(predicted_signals[-1])

        except Exception as e:
            raise error.PredictiveModelError('{}: Predict in predictive model error: {}'.format(self.model_name, e))

        return signals
