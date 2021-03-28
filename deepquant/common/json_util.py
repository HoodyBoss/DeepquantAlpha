import json
import numpy as np

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

def encode(data):
    output = json.dumps(data, cls=NpEncoder)
    #output = json.JSONEncoder().encode(data)
    return output


def decode(data):
    output = json.JSONDecoder().decode(data)
    return output


def load(file_path):
    with open(file_path) as f:
        output = json.load(f)
    return output


def loads(data):
    output = json.loads(data)
    return output
