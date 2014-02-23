'''
Created on 2014-2-17

@author: yuzhang
'''
import os, re

stopWordsPath = os.path.dirname(__file__) + os.sep + 'stopWords'

def getStopWords():
    f = open(stopWordsPath + os.sep + 'chineseStopWords.txt', 'r')
    chnStopWords = f.read().split('\n')
    f.close()
    f = open(stopWordsPath + os.sep + 'englishStopWords.txt', 'r')
    engStopWords = f.read().split('\n')
    f.close()
    return chnStopWords + engStopWords

sw = getStopWords()

def removeStopWordsJbg(words):
    return [ w for w in words if (w.word not in sw) and (not re.compile(r'[\d]+').match(w.word)) ]

def removeStopWordsJb(words):
    return [ w for w in words if (w not in sw) and (not re.compile(r'[\d]+').match(w.word)) ]