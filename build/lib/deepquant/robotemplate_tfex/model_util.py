class BaseModelUtil:

    _price_columns = None
    _indi_columns = None
    _feature_columns = None

    _analysis_log_columns = None

    def __int__(self):
        self._price_columns = ['date', 'time', 'open', 'high', 'low', 'close']
        self._indi_columns = []
        self._feature_columns = []

        self._analysis_log_columns = ['trade_signal', 'trade_action', 'pos_size', 'stop_loss', 'take_profit']

    def get_price_columns(self):
        return self._price_columns

    def get_indi_columns(self):
        return self._indi_columns

    def get_feature_columns(self):
        return self._feature_columns

    def get_analysis_log_columns(self):
        return self._analysis_log_columns

    def has_indi(self):
        if len(self._indi_columns) > 0:
            return True
        else:
            return False

    def has_feature(self):
        if len(self._feature_columns) > 0:
            return True
        else:
            return False

    # Load Deep Neural Network model and its weights from disk and return loaded model
    def load_dnn_model(self, model_filename, weight_filename):
        loaded_model = None
        return loaded_model

    # Load XGBoost model and its weights from disk and return loaded model
    def load_xgboost_model(self, model_filename):
        loaded_model = None
        return loaded_model

    # ==============================================================================
    # Extract indicators
    #
    # BEGIN: Extract OHLC by calculating the indicators
    # ==============================================================================
    def extract_indi(self, price_df):
        latest_indi_df = None
        return latest_indi_df
    # ==============================================================================
    # END: Extract OHLC by calculating the indicators
    # ==============================================================================


    # ==============================================================================
    # Extract features
    #
    # BEGIN: Extract features from price & indi file
    # ==============================================================================
    def extract_feature(self, price_indi_df, min_price, max_price, min_macd, max_macd, max_skip_row, max_backward_row):
        latest_feature_df = None
        return latest_feature_df
    # =============================================================================
    # END: Extract features from price & indi file
    # =============================================================================
