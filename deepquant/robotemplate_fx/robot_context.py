import logging


class BaseRobotContext:

    def __init__(self, config):
        # Prepare
        self.config = config
        self.ml_models = None
        self.datasets = None

        self.correl_id = None # Correlation ID
        self.base_time = None
        self.account = None
        # Just position related to this trading robot
        self.position = None
        self.server_time = None
        self.local_time = None
