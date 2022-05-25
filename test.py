from google.cloud import speech_v1p1beta1
import os
import io

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'bk-tiktok-29e9951ff1b8.json'


def speech_to_text(config, audio):
    client = speech_v1p1beta1.SpeechClient()
    response = client.recognize(config=config, audio=audio)
    print_sentences(response)


def print_sentences(response):
    for result in response.results:
        best_alternative = result.alternatives[0]
        transcript = best_alternative.transcript
        confidence = best_alternative.confidence
        print("-" * 80)
        print(f"Transcript: {transcript}")
        print(f"Confidence: {confidence:.0%}")


config = dict(language_code="en-US")
audio = dict(uri="gs://cloud-samples-data/speech/brooklyn_bridge.flac")
speech_to_text(config, audio)
