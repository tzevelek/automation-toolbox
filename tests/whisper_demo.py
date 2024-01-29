# This code is based on the code example here https://github.com/aws-samples/amazon-live-translation-polly-transcribe and is only for testing/demonstration purposes.

import pyaudio
import os
import boto3
import sagemaker
import sys
from sagemaker.predictor import Predictor
from sagemaker.deserializers import JSONDeserializer
from sagemaker.serializers import JSONSerializer
from botocore.exceptions import BotoCoreError, ClientError
import sys
import audioop
import pyaudio

import sounddevice as sd
from scipy.io.wavfile import write

endpoint_name="ENTER YOUR ENDPOINT"
role="ENTER YOUR IAM ROLE" #REPLACE WITH YOUR ROLE
sm_session = sagemaker.Session(boto3.session.Session())

predictor = Predictor(endpoint_name=endpoint_name, sagemaker_session=sm_session)
#predictor.serializer = sagemaker.serializers.NumpySerializer()
predictor.serializer = sagemaker.serializers.JSONSerializer
predictor.content_type = "application/json"
predictor.accept = "application/json"
predictor.deserializer = JSONDeserializer()

polly = boto3.client('polly', region_name = 'us-east-1')
translate = boto3.client(service_name='translate', region_name='us-east-1', use_ssl=True)


chunk = 1024  # Record in chunks of 1024 samples
sample_format = pyaudio.paInt16  # 16 bits per sample
channels = 2
fs = 44100  # Record at 44100 samples per second
seconds = 3
filename = "output.wav"
p = pyaudio.PyAudio()  # Create an interface to PortAudio

try:
    default_device_index = p.get_default_input_device_info()
except IOError:
    default_device_index = -1

#Select Device
print ("Available devices:\n" )
for i in range(0, p.get_device_count()):
    info = p.get_device_info_by_index(i)
    print (str(info["index"]) + ": \t %s \n \t %s \n" % (info["name"], p.get_host_api_info_by_index(info["hostApi"])["name"]))
    if default_device_index == -1:
        default_device_index = info["index"]

#Handle no devices available
if default_device_index == -1:
    print ("No device available. Quitting.")
    exit()

#Get input or default
device_id = int(input("Choose device ["+ str(default_device_index) +"]: ") or default_device_index)
print ("")

#Get device info
try:
    device_info = p.get_device_info_by_index(device_id)
except IOError:
    device_info = p.get_device_info_by_index(default_device_index)
    print ("Selection not available, using default.")
    
#Choose between loopback or standard mode
is_input = device_info["maxInputChannels"] > 0
is_wasapi = (p.get_host_api_info_by_index(device_info["hostApi"])["name"]).find("WASAPI") != -1
if is_input:
    print ("Selection is input using standard mode.\n")
else:
    if is_wasapi:
        useloopback = True;
        print ("Selection is output. Using loopback mode.\n")
    else:
        print ("Selection is input and does not support loopback mode. Quitting.\n")
        exit()

print(device_info)

direction = 1
direction = int(input("Choose source and target language to translate. 1 for en to fr, 2 for fr to en [" + str(direction) + "]: ") or default_device_index)
params = {}

if direction == 1: #from english to non-english
    params['source_language'] = "en"
    params['target_language'] = "fr"
    params['voice_id'] = "Mathieu"
    params['lang_code_for_polly'] = "fr-FR"
elif direction == 2: #to english
    params['source_language'] = "french"
    params['lang_code_for_polly'] = "en-US"
    params['voice_id'] = "Joanna"
else:
    raise Exception("Languages not implemented!")

print('Recording\n')


def aws_translate(transcript):
    trans_result = translate.translate_text(
        Text = transcript,
        SourceLanguageCode = params['source_language'],
        TargetLanguageCode = params['target_language']
    )
    transcript = trans_result.get("TranslatedText")
    print("translated text with Amazon Translate:" + transcript)
    return trans_result.get("TranslatedText")
                        
def aws_sagemaker_whisper(audio, language, task):
    with open(audio, "rb") as file:
        wav_file_read = file.read()

    payload = {"audio_input": wav_file_read.hex(), "language":language, "task": task}

    predictor.serializer = JSONSerializer()
    predictor.content_type = "application/json"
    response = predictor.predict(payload)
    transcript=response["text"][0]
    print(task+" with Whisper:" + transcript)
    return transcript

def stream_data(stream):
    """Consumes a stream in chunks to produce the response's output'"""
    print("Streaming started...")
    chunk = 1024
    if stream:
    # Note: Closing the stream is important as the service throttles on
    # the number of parallel connections. Here we are using
    # contextlib.closing to ensure the close method of the stream object
    # will be called automatically at the end of the with statement's
    # scope.
        polly_stream = p.open(
                    format = pyaudio.paInt16,
                    channels = 1,
                    rate = 16000,
                    output = True,
                    )

        #this is a blocking call..
        while True:
            data = stream.read(chunk)
            polly_stream.write(data)
            # If there's no more data to read, stop streaming
            if not data:
                stream.close()
                polly_stream.stop_stream()
                polly_stream.close()
                break
            # Ensure any buffered output has been transmitted and close the
            # stream
            # self.wfile.flush() CLOSE STEAM 
        print("Streaming completed.")
    else:
        # The stream passed in is empty
        print("Nothing to stream.")
        
def aws_polly_tts(text):
    try:
        response = polly.synthesize_speech(
            Engine = 'standard',
            LanguageCode = params['lang_code_for_polly'],
            Text=text,
            VoiceId = params['voice_id'],
            OutputFormat = "pcm",
        )
        byte_stream = response['AudioStream']
        stream_data(byte_stream)
    except (BotoCoreError, ClientError) as error:
        # The service returned an error, exit gracefully
        print(error)
        sys.exit(-1)

def play_audio(stream):
    p = pyaudio.PyAudio()

    audio_stream = p.open(
        format=pyaudio.paInt16,
        channels=1,
        rate=16000,
        output=True
    )

    while True:
        data = stream.read(chunk)
        if len(data) == 0:
            break
        
        data = audioop.tomono(data, 2, 1, 0)
        audio_stream.write(data)

    audio_stream.stop_stream()
    audio_stream.close()
    p.terminate()


myrecording = sd.rec(int(seconds * fs), samplerate=fs, channels=1)
sd.wait()  # Wait until recording is finished
write('output.wav', fs, myrecording)  # Save as WAV file 
if (direction==1):
    transcript = aws_sagemaker_whisper('output.wav', 'english', 'transcribe')
    text= aws_translate(transcript)
else: #if translating to English
    text = aws_sagemaker_whisper('output.wav',  params['source_language'], 'translate')
output = aws_polly_tts(text)

# Play byte input as audio
play_audio(output)