from ecpy.curves import Curve
from ecpy.keys import ECPrivateKey
from ecpy.ecdsa import ECDSA
import base64
import hashlib
import binascii
import datetime, pytz


def sign(api_key, api_secret, params):
    cv = Curve.get_curve('secp256r1')

    utc_tz = pytz.timezone('UTC')
    dt = datetime.datetime.now().astimezone(utc_tz)
    timestamp = int(dt.timestamp() * 1000)

    payload = "{}.{}.{}".format(api_key, params, timestamp)
    hashed_payload = hashlib.sha256(payload.encode("UTF-8")).hexdigest()

    pv_key = ECPrivateKey(
        int(binascii.hexlify(base64.b64decode(api_secret)), 16), cv)
    signature_bytes = ECDSA().sign(bytearray.fromhex(hashed_payload), pv_key)
    return binascii.hexlify(signature_bytes).decode("UTF-8"), timestamp


# ===========================================
app_secret = 'SaiwVvdcG//Hi+PbMtw97etyRsBYc8YwE8z8V0cWwIU='
app_id = 'hXnFe65Zid56eNuB'
params = ''

signature, timestamp = sign(app_id, app_secret, params)
print(signature)
print(timestamp)
