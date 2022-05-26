import requests
from pymongo import MongoClient
import pandas as pd
import numpy as np
from pandas import DataFrame

conn = MongoClient(
    'mongodb+srv://nguyenbinhlong:MbFgZls0VfDtzehQ@cluster0.iylkd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
)
db = conn['crawler']
collection_tiktok = db['tiktok']

# list_data_des = collection_data.find({}, {"_id": 0, "des": 1})
list_tiktok = collection_tiktok.find(
    {"mp3_downloaded": 1, "is_texted": 1},
    {"_id": 0, "is_politics": 1, "text": 1})

# list_cur = list(list_data)
#
# df = DataFrame(list_cur)
#
# my_numpy = df.to_numpy()
# np.savetxt('out.csv', my_numpy, fmt='%s', delimiter='####')

# list of name, degree, score
_data = []
for d in list_tiktok:
    if d['text'] is not None and d['text'] != "":
        s = d["text"]
        p = str(d["is_politics"])
        _data.append("label_is_politics_" + p.encode("UTF-8") + "####" + s.encode("UTF-8"))

df = pd.DataFrame(_data)

# saving the dataframe
df.to_csv('text_tiktok.csv', sep="#", header=False, index=False, encoding='utf-8')
