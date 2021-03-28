import unittest

import time

from ecpy.curves import Curve
from ecpy.keys import ECPrivateKey
from ecpy.ecdsa import ECDSA
import base64
import hashlib
import binascii

import deepquant.common.datetime_util as datetime_util
import deepquant.common.json_util as json_util
import deepquant.common.http_util as http_util

from deepquant.ems.error import ExecutionError


class TestSettradeOpenAPI_Auth(unittest.TestCase):

    max_retry = 10
    wait_seconds = 1000 # 1000 milliseconds
    auth_accounts = {}
    login_url = 'https://open-api.settrade.com/api/oam/v1/{brokerId}/broker-apps/ALGO/login'
    refresh_token_url = 'https://open-api-test.settrade.com/api/oam/v1/{brokerId}/broker-apps/ALGO/refresh-token'
    http_header = {'Content-Type' : 'application/json'}
    auth_accounts = {}

    def send_http_post_request(self, url, json_request_payload, header_dict=None):
        req_data = json_util.encode(json_request_payload)
        if header_dict is not None:
            response = http_util.post(url, req_data, header_dict=header_dict, require_response=True)
        else:
            response = http_util.post(url, req_data, header_dict=self.http_header, require_response=True)
        return response


    def send_http_get_request(self, url, header_dict=None):
        if header_dict is not None:
            response = http_util.get(url, header_dict=header_dict)
        else:
            response = http_util.get(url, header_dict=self.http_header)
        return response


    def _sign(self, api_key, api_secret, params):
        cv = Curve.get_curve('secp256r1')

        dt = datetime_util.utcnow()
        timestamp = int(dt.timestamp() * 1000)

        payload = "{}.{}.{}".format(api_key, params, timestamp)
        hashed_payload = hashlib.sha256(payload.encode("UTF-8")).hexdigest()

        pv_key = ECPrivateKey(
            int(binascii.hexlify(base64.b64decode(api_secret)), 16), cv)
        signature_bytes = ECDSA().sign(bytearray.fromhex(hashed_payload), pv_key)
        return binascii.hexlify(signature_bytes).decode("UTF-8"), timestamp


    def _login(self, account_conf):
        # Load signature
        #base_token = json_util.load(os.environ[account_conf['base_token_env_var']])
        base_token = json_util.load('/Users/minimalist/settrade_open_api_tk/tk_caf1.dq')

        app_id = base_token['app_id']
        app_secret = base_token['app_secret']

        login_signature, login_timestamp = self._sign(app_id, app_secret, "")

        api_key = app_id

        # Login
        retry = 0
        login_result = False
        while login_result != True and retry < self.max_retry:
            json_payload = {'apiKey': api_key, 'params': '', 'signature': login_signature, 'timestamp': login_timestamp}
            response = self.send_http_post_request(self.login_url.replace('{brokerId}', str(account_conf['broker_id']))
                                                   , json_payload
                                                   , header_dict=self.http_header)
            if response.status_code == 200:
                response_dict = json_util.decode(response.text)
                response_dict['last_login_timestamp'] = login_timestamp
                self.auth_accounts[account_conf['account_number']] = response_dict
                login_result = True
            else:
                time.sleep(self.wait_seconds)
                retry = retry + 1

        if login_result == False:
            raise ExecutionError("Log in failed for account '{}'".format(account_conf['account_number']))


    def _refresh_token(self, account_conf):
        # Load signature
        # base_token = json_util.load(os.environ[account_conf['base_token_env_var']])
        base_token = json_util.load('/Users/minimalist/settrade_open_api_tk/tk_caf1.dq')
        app_id = base_token['app_id']
        api_key = app_id

        # Refresh token
        retry = 0
        refresh_result = False
        while refresh_result != True and retry < self.max_retry:
            refresh_token = self.auth_accounts[account_conf['account_number']]['refresh_token']
            json_payload = {'apiKey': api_key, 'refreshToken': refresh_token}
            response = self.send_http_post_request(self.refresh_token_url.replace('{brokerId}', account_conf['broker_id'])
                                                   , json_payload
                                                   , header_dict=self.http_header)
            if response.status_code == 200:
                response_dict = json_util.decode(response.text)
                self.auth_accounts[account_conf['account_number']]['access_token'] = response_dict['access_token']
                refresh_result = True
            else:
                time.sleep(self.wait_seconds)
                retry = retry + 1

        if refresh_result == False:
            raise ExecutionError("Refresh token failed for account '{}'".format(account_conf['account_number']))


    def _prepare_auth(self, account_conf):
        account_number = account_conf['account_number']
        if account_number in list(self.auth_accounts.keys() and self.auth_accounts[account_number] is not None):
            auth_acc = self.auth_accounts[account_number]
            last_login_timestamp = auth_acc['last_login_timestamp']
            expires_in = auth_acc['expires_in'] * 1000 # convert millisecond to second
            cur_timestamp = int(datetime_util.utcnow().timestamp() * 1000)

            if cur_timestamp - last_login_timestamp >= int(expires_in / 3):
                # Refresh token
                self._refresh_token(account_conf)
        else:
            # Login
            self._login(account_conf)


    def test_get_account_info(self):

        account_conf = {'broker_id' : '063'
                        , 'account_number' : '0038027'
                        , 'base_token_env_var' : 'TK_CAF1'}
        self._prepare_auth(account_conf)

        url = 'https://open-api.settrade.com/api/seosd/v1/{}/accounts/{}/account-info'\
            .format(account_conf['broker_id'], account_conf['account_number'])

        auth_acc = self.auth_accounts[account_conf['account_number']]
        print(self.auth_accounts)
        access_token = auth_acc['access_token']
        header_dict = {'Authorization' : 'bearer {}'.format(access_token)}
        response = self.send_http_get_request(url, header_dict)

        self.assertEqual(response.status_code, 200)
        acc_info = json_util.decode(response.text)
        print(response.status_code)
        print(acc_info)
        self.assertEqual(acc_info, 100000000)


if __name__ == '__main__':
    unittest.main()