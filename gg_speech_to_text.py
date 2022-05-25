from google.cloud import speech_v1p1beta1
import os
import io
from pymongo import MongoClient

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'bk-tiktok-29e9951ff1b8.json'

conn = MongoClient(
    'mongodb+srv://nguyenbinhlong:MbFgZls0VfDtzehQ@cluster0.iylkd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
)
db = conn['crawler']
collection_tiktok = db['tiktok']

list_mp3 = collection_tiktok.find(
    {"mp3_downloaded": 1,
     "$or": [
         {"is_texted": 0},
         {"is_texted": {"$exists": False}}
     ]},
    {"_id": 1, "playUrl": 1}).limit(100)


def get_name(_mp3):
    name = _mp3['playUrl'].rsplit('/', 1)[-1]
    return name


def speech_to_text(file):
    # Creates google client
    client = speech_v1p1beta1.SpeechClient()

    # Full path of the audio file, Replace with your file name
    file_name = os.path.join(os.path.dirname(__file__), "mp3/" + file)

    # Loads the audio file into memory
    with io.open(file_name, "rb") as audio_file:
        content = audio_file.read()
        audio = speech_v1p1beta1.RecognitionAudio(content=content)

    config = speech_v1p1beta1.RecognitionConfig(
        encoding=speech_v1p1beta1.RecognitionConfig.AudioEncoding.MP3,
        audio_channel_count=2,
        language_code="vi-VN",
        alternative_language_codes=["en-US", "vi-VN"],
        model="command_and_search",
        sample_rate_hertz=16000,
    )

    try:
        # Sends the request to google to transcribe the audio
        response = client.recognize(request={"config": config, "audio": audio})

        # Reads the response
        _text = ""
        for result in response.results:
            print("Transcript: {}".format(result.alternatives[0].transcript))
            _text = _text + " " + format(result.alternatives[0].transcript)
        return _text
    except:
        return ''


for mp3 in list_mp3:
    print("speech to text _id:", mp3['_id'], 'playUrl', mp3["playUrl"])
    name = get_name(mp3)
    text = speech_to_text(name)
    collection_tiktok.update_one(
        {'_id': mp3['_id']},
        {"$set": {
            "is_texted": 1,
            'text': text
        },
        },
        upsert=False
    )
