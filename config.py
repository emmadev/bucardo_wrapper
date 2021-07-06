import os

import yaml

_dirname = os.path.dirname(os.path.abspath(__file__))


def load_config(filename):
    with open(os.path.join(_dirname, filename)) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config
