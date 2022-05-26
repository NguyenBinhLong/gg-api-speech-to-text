import requests
from pymongo import MongoClient
import pandas as pd
import numpy as np
from pandas import DataFrame

conn = MongoClient(
    'mongodb+srv://nguyenbinhlong:MbFgZls0VfDtzehQ@cluster0.iylkd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
)
db = conn['crawler']
collection_data = db['data']

# list_data_des = collection_data.find({}, {"_id": 0, "des": 1})
list_data = collection_data.find({}, {"_id": 0, "is_politics": 1, "des": 1})

# list_cur = list(list_data)
#
# df = DataFrame(list_cur)
#
# my_numpy = df.to_numpy()
# np.savetxt('out.csv', my_numpy, fmt='%s', delimiter='####')

# list of name, degree, score
_data = []
for d in list_data:
    _data.append("label_is_politics_" + str(d["is_politics"]) + "####" + d["des"])

df = pd.DataFrame(_data)

# saving the dataframe
df.to_csv('text_website.csv', sep="#", header=False, index=False, encoding='utf-8')
