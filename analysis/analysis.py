# coding: utf-8
import json
from watson_developer_cloud import AlchemyLanguageV1

def getApiKey(file):
    with open(file,'r') as fp:
        authdict = json.load(fp)
    return authdict['AlchemyAPIKey']

def getAlchemyApi():
    apikey = getApiKey('auth.json')
    alchemy_language = AlchemyLanguageV1(api_key=apikey)
    return alchemy_language

def slice_text(text):
    return_array = []
    while len(text) > 50000:
        return_array += [text[0:49999]]
        text = text[50000:]
    if len(text) > 0:
        return_array += [text]
    return return_array

def emotion_analysis(text):
    average_emotions = {'sadness':0.0,
                    'disgust':0.0,
                    'fear':0.0,
                    'joy':0.0,
                    'anger':0.0}
    chunks = slice_text(text)
    for chunk in chunks:
        emotion_json = getAlchemyApi().emotion(text=chunk)
        emotions = emotion_json['docEmotions']
        for emotion in emotions:
            average_emotions[emotion] += float(emotions[emotion])
    for emotion in average_emotions:
        average_emotions[emotion] = average_emotions[emotion] / len(chunks)
    return average_emotions

def sentiment_analysis(text): # [-1, 1] <0 = negative, 0 = neutral, 1 = positive
    average_sentiment_score = 0.0
    chunks = slice_text(text)
    for chunk in chunks:
        sentiment_json = getAlchemyApi().sentiment(text=chunk)
        sentiment = sentiment_json['docSentiment']
        average_sentiment_score += float(sentiment['score'])
    average_sentiment_score = average_sentiment_score / len(chunks)
    return average_sentiment_score

def concept_analysis(text):
    concept_dict = {}
    sumcount = {}
    chunks = slice_text(text)
    for chunk in chunks:
        concept_json = getAlchemyApi().concepts(text=chunk)
        concepts = concept_json['concepts']
        for concept in concepts:
            if concept['text'] in concept_dict.keys():
                sumcount[concept['text']] += 1
                concept_dict[concept['text']] += float(concept['relevance'])
            else:
                sumcount[concept['text']] = 1
                concept_dict[concept['text']] = float(concept['relevance'])
    for concept in concept_dict.keys():
        concept_dict[concept] = concept_dict[concept] / sumcount[concept]
    return concept_dict
    



