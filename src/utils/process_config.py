import yaml
import json
import pandas as pd

with open("config.yml", "r") as f:
    data = yaml.load(f, Loader=yaml.FullLoader)
    # df = pd.DataFrame(data)
data