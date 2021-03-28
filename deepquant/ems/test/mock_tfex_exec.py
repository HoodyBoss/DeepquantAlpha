import os
from deepquant.ems.tfex_exec import TfexExec


class MockTfexExec(TfexExec):

    def __init__(self, robot_config):
        super().__init__(robot_config)

        self.base_domain = 'http://localhost:8089/api/seosd/v1'
        #base_token_env_var = 'TK_SETTRADE1'
        #google_app_cred_env_var = 'GOOGLE_APPLICATION_CREDENTIALS'
        #base_token = open(os.environ[base_token_env_var]).read().strip()
        #self.google_app_cred = open(os.environ[google_app_cred_env_var]).read().strip()
        base_token = ''
        self.google_app_cred = ''
        self.http_header = { 'Authorization' : 'Bearer {}'.format(base_token)
                             , 'Content-Type' : 'application/json' }
