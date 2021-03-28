import deepquant.common.error as error
import pickle
from keras.models import model_from_json


# Load stored model in json format and then create and return DNN model
def load_dnn_model(model_filename, weight_filename):
    loaded_model = None
    try:
        json_file = open(model_filename, 'r')
        loaded_model_json = json_file.read()
        json_file.close()

        # Load model
        loaded_model = model_from_json(loaded_model_json)
        # Load weights
        loaded_model.load_weights(weight_filename)
    except Exception as e:
        raise error.PredictiveModelError('Load DNN model error: {}'.format(e))

    return loaded_model

def load_xgboost_model(model_filename):
    loaded_model = None
    try:
        loaded_model = pickle.load(open(model_filename + '.dat', 'rb'))
    except Exception as e:
        raise error.PredictiveModelError('Load XGBoost model error: {}'.format(e))
    return loaded_model
