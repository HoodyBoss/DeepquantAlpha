import os
from deepquant.ems.oanda_exec import OandaExec


class MockOandaExec(OandaExec):

    def __init__(self, robot_config):
        super().__init__(robot_config)

        token_env_var = 'TK_OANDA'
        #token = open(os.environ[self.token_env_var]).read().strip()
        token = ''
        self.base_domain = 'https://api-fxtrade.oanda.com'
        self.http_header = {'Authorization': 'Bearer {}'.format(token)
                            , 'Content-Type': 'application/json'}
