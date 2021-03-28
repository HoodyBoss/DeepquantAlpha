import time
import logging
from importlib import import_module

import deepquant.common.error as error

# create logger
logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def get_dataset(bar_time, dataset_location, broker_id, model_name, symbol_name, timeframe, dataset_router_modulename=None):
    dataset = None

    try:
        if dataset_location.lower() == 'local':
            # Load dataset router module dynamically at runtime (Runtime binding)
            dataset_router = import_module(dataset_router_modulename)

            # Get dataset, dataset type is pandas DataFrame
            is_new_dataset = False
            wait_time = 0.5  # wait 0.5 second
            max_retry = 60  # 30 second
            retry_num = 0
            while is_new_dataset == False and retry_num < max_retry:
                dataset = dataset_router.get_dataset(broker_id, model_name, symbol_name, timeframe)
                # Validate bar time of last row (last bar)
                if dataset.iloc[-1, 0] == bar_time:
                    is_new_dataset = True
                else:
                    time.sleep(wait_time)
                    retry_num += 1

            if retry_num == max_retry:
                raise error.PredictiveModelError('Has no new dataset for {} - {} - {},  retry exceed max retry: {}'.format(\
                    model_name, symbol_name, timeframe, retry_num))

    except error.PredictiveModelError as pe:
        raise error.PredictiveModelError(pe)
    except Exception as e:
        raise Exception('Unexpected error in getting dataset by dataset gateway: {}'.format(e))

    return dataset

    """
    def backfill_dataset(bar_time, dataset_location, broker_id, model_name, symbol_name, timeframe, dataset_router_modulename=None):
        dataset = None
        
        # Will add code later for import performance using Eager Acquisition Pattern

        dt_format = '%Y-%m-%d %H:%M:%S'
        d1 = datetime.datetime.strptime('2019-04-01 17:35:00', dt_format)
        d2 = datetime.datetime.strptime('2019-05-27 14:40:00', dt_format)
        minutes_diff = (d2 - d1).total_seconds() / 60.0
        # Check

        return dataset
    """
