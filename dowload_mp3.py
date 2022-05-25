import requests
from pymongo import MongoClient

conn = MongoClient(
    'mongodb+srv://nguyenbinhlong:MbFgZls0VfDtzehQ@cluster0.iylkd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
)
db = conn['crawler']
collection_tiktok = db['tiktok']

list_mp3 = collection_tiktok.find({
    "$or": [
        {"mp3_downloaded": 0},
        {"mp3_downloaded": {"$exists": False}}
    ]
}, {"_id": 1, "playUrl": 1}
).limit(500)


def get_name(_mp3):
    name = _mp3['playUrl'].rsplit('/', 1)[-1]
    return name


def download_mp3(_mp3):
    with requests.Session() as req:
        download = req.get(_mp3['playUrl'])
        name = get_name(_mp3)
        if download.status_code == 200:
            with open('mp3/' + name, 'wb') as f:
                f.write(download.content)
                print("--------------------DONE")
                return True
        else:
            print(f"Download Failed For File {_mp3['playUrl']}")
            return False


for mp3 in list_mp3:
    print("download _id:", mp3['_id'], 'playUrl', mp3["playUrl"])
    if "playUrl" in mp3 and mp3["playUrl"] is not None and mp3["playUrl"] != "" and ".mp3" in mp3["playUrl"]:
        result = download_mp3(mp3)
        if result:
            collection_tiktok.update_one(
                {'_id': mp3['_id']},
                {"$set": {"mp3_downloaded": 1}},
                upsert=False
            )
            continue

    # crawl fall
    print("--------------------FALL")
    collection_tiktok.update_one(
        {'_id': mp3['_id']},
        {"$set": {"mp3_downloaded": 2}},
        upsert=False
    )
