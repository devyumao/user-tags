#coding=utf-8
'''
Created on 2014-2-16

@author: yuzhang
'''

import urllib2, json, re, os, math
import jieba.posseg as jbp
import jieba as jb
import logging
from bs4 import BeautifulSoup
from stopWords import removeStopWordsJbg
import MySQLdb as mdb

jb.enable_parallel()

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

weiboAppKey = '82966982'
tagsDict = {}
b = 2

def setEleInDict(d, e, n):
    if e not in d:
        d[e] = n
    else:
        d[e] += n

def setTagInDict(d, i, t, n):
    if i not in d:
        d[i] = [t, n]
    else:
        d[i][1] += n

def getWordsFromWeibo(text):
    text = text.lower()
    userNameRE = re.compile(ur'@[\u4e00-\u9fa5|\w|\-|_]+')
    text = userNameRE.sub(' ', text)
    expressionRE = re.compile(r'\[.*?\]')
    text = expressionRE.sub(' ', text)
    shortUrlRE = re.compile(r'http://t.cn/\w+')
    text = shortUrlRE.sub(' ', text)
    return removeStopWordsJbg(jbp.cut(text))

def getWordsFromWiki(text):
    text = text.lower()
    htmlRE = re.compile(r'<.*?>')
    text = htmlRE.sub(' ', text)
    urlRE = re.compile(r'[a-z]+://[^\s]*')
    text = urlRE.sub(' ', text)
    return removeStopWordsJbg(jbp.cut(text))

def isWordWithMaxFlag(flag):
    return flag != 'x'

def isWordWithMidFlag(flag):
    return 'n' == flag[0] or flag in ['v', 'vn', 'a', 'an', 'eng']

def isWordWithMinFlag(flag):
    return 'n' == flag[0] or flag in ['vn', 'an', 'eng']

def collectTags(text):
    words = getWordsFromWeibo(text)
    wordTimesDict = {}
    wordsCon = [] 
    for w in words:
        if isWordWithMidFlag(w.flag):
            wordsCon.append(w.word)
            if w.flag not in ['v', 'a']:
#                 print w.word, w.flag
                setEleInDict(wordTimesDict, w.word, 1)
    wordsConSet = set(wordsCon)
                                    
    for (word, times) in wordTimesDict.items():
        cur.execute("SELECT pageID FROM PageMapLine WHERE name LIKE %s ORDER BY id", [word])
        row = cur.fetchone()
        if row:
            pageID = row['pageID']
            cur.execute("SELECT name, text, isDisambiguation FROM Page WHERE id = %s", [pageID])
            row1 = cur.fetchone()
            
            if 1 == ord(row1['isDisambiguation']):
                print u'【歧义词】', pageID, row1['name']
                # 歧义词候选name有待完善
                disaText = row1['text'].lower()
                disaNames = [name.split('|')[0].replace(' ', '_')
                             for name in 
                             re.findall(r'\*+[ ]*\[\[(.*?)\]\]', disaText)
                             + re.findall(ur'参见\[\[(.*?)\]\]', disaText)]
#                 print '[', ', '.join(disaNames), ']'               
                cur.execute("SELECT outLinks FROM page_outlinks WHERE id = %s", [pageID])
                disaWords = {}
                for ol in cur.fetchall():
                    cur.execute("SELECT name, text, isDisambiguation FROM Page WHERE id = %s", [ol['outLinks']])
                    row3 = cur.fetchone()
                    if 0 == ord(row3['isDisambiguation']) and (row3['name'].lower() in disaNames):
                        wikiText = row3['text']
                        wikiWordsConSet = set([w.word for w in getWordsFromWiki(wikiText) if isWordWithMidFlag(w.flag)])       
                        disaWords[ol['outLinks']] = {
                                                     'name': row3['name'], 
                                                     'text': wikiText, 
                                                     'con': len(wordsConSet.intersection(wikiWordsConSet)), 
                                                     'sim': 0, 
                                                     'cat': 0, 
                                                     'tol': 0
                                                     }
                        print ol['outLinks'], row3['name'], disaWords[ol['outLinks']]['con']
                        
            else:
                cur.execute("SELECT pageID FROM PageMapLine WHERE name LIKE %s ORDER BY id", [word+u'_(消歧义)'])
                row2 = cur.fetchone()
                if row2:
                    continue
                else:
                    pageName = row1['name']
                    setEleInDict(tagsDict, pageName, times*1.0)
                    
                    cur.execute("SELECT pages AS catID FROM page_categories WHERE id = %s", [pageID])
                    crawlCategories(cur.fetchall(), 2, times, 1)
            
                    
def crawlCategories(rows, levels, times, d):
    if 0 != levels:
        for row in rows:
            catID = row['catID']
            cur.execute("SELECT name FROM Category WHERE id = %s", [catID])
            catName = cur.fetchone()['name']
            if re.compile(ur'.*[的]+.*').match(catName) or re.compile(ur'.*[\d]+.*').match(catName):
                continue
            stoptags = [u'分类', u'名词', u'动词', u'词汇', u'代词', u'条目', u'人名']
            endswithStoptag = False
            for st in stoptags:
                if catName.endswith(st):
                    endswithStoptag = True
                    break
            if not endswithStoptag:
                setEleInDict(tagsDict, catName, times/math.pow(b, d))
                cur.execute("SELECT inLinks AS catID FROM category_inlinks WHERE id = %s", [catID])
                crawlCategories(cur.fetchall(), levels-1, times, d+1)
    
 
res = urllib2.urlopen('https://api.weibo.com/2/statuses/user_timeline.json?source=' + weiboAppKey + '&uid=1631499041&count=20&trim_user=1')
data = json.loads(res.read())
statuses = data['statuses']

con = mdb.connect('localhost', 'yumao', 'yumao8899', 'wikidb', charset='utf8');
cur = con.cursor(mdb.cursors.DictCursor)

# for status in [statuses[3]]:
for status in statuses:
    text = ' '.join([status['text'], status['retweeted_status']['text']]) if status.has_key('retweeted_status') else status['text']
    print text
    collectTags(text)
    
cur.close()
con.close()


# stoptags = [u'术语', u'名词', u'概念']
# for w in stoptags:
#     if tagsDict.has_key(w):
#         del tagsDict[w]
# listDel = []
# for (tag, weight) in tagsDict.items():
#     for st in stoptags:
#         if tag.endswith(st):
#             listDel.append((tag, weight))
#             break    
# for t in listDel:
#     setEleInDict(t[0][0:-2], tagsDict, t[1])
#     del tagsDict[t[0]]
                      
# for t in sorted(tagsDict.iteritems(), key=lambda d:d[1], reverse = True):
#     print t[0], t[1]
# print len(tagsDict)