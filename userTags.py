#coding=utf-8
'''
Created on 2014-2-16

@author: yuzhang
'''

import urllib2, json, re, os, math, time
import jieba.posseg as jbp
import jieba as jb
import logging
from stopWords import removeStopWordsJbg
import MySQLdb as mdb
from langconv import Converter

jb.enable_parallel()

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

weiboAppKey = '82966982'
tagsDict = {}
b = 2
cvt = Converter('zh-hans')

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
            if len(w.word) > 1 and w.flag not in ['v', 'a']:
#                 print w.word, w.flag
                setEleInDict(wordTimesDict, w.word, 1)
    wordsSet = set(wordsCon)
                                    
    for (word, times) in wordTimesDict.items():
        cur.execute("SELECT pageID FROM PageMapLine WHERE name LIKE %s ORDER BY id", [word])
        row = cur.fetchone()
        if row:
            pageID = row['pageID']
            cur.execute("SELECT name, text, isDisambiguation FROM Page WHERE id = %s", [pageID])
            row1 = cur.fetchone()
            
            if 1 == ord(row1['isDisambiguation']):
                print "【歧义词】", pageID, row1['name']
                disambiguate(pageID, wordsSet)
                    
            else:
                cur.execute("SELECT pageID FROM PageMapLine WHERE name LIKE %s ORDER BY id", [word+u'_(消歧义)'])
                row2 = cur.fetchone()
                if row2:
                    print "【歧义词】", row2['pageID'], word+u'_(消歧义)'
                    disambiguate(row2['pageID'], wordsSet)
                else:
                    pageName = row1['name']
                    setEleInDict(tagsDict, pageName, times*1.0)
                    
                    cur.execute("SELECT pages AS catID FROM page_categories WHERE id = %s", [pageID])
                    crawlCategories(cur.fetchall(), 2, times, 1)

def disambiguate(disaID, wordsSet):
    cur.execute("SELECT candidates FROM page_candidates WHERE id = %s", [disaID])
    rows = cur.fetchall()
    for row in rows:
        cand = row['candidates']
        cur.execute("SELECT name, isDisambiguation, words FROM Page WHERE id = %s", [cand])
        row1 = cur.fetchone()
        if 0 == ord(row1['isDisambiguation']):
            wikiWordsSet = set([w[0] for w in json.loads(row1['words']) if isWordWithMidFlag(w[1])])     
            print '\t', cand, row1['name'], len(wordsSet.intersection(wikiWordsSet))
         
                    
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
    
 
res = urllib2.urlopen('https://api.weibo.com/2/statuses/user_timeline.json?source=' + weiboAppKey + '&uid=1631499041&count=50&trim_user=1')
data = json.loads(cvt.convert(res.read().decode('utf-8')).encode('utf-8'))
statuses = data['statuses']

con = mdb.connect('localhost', 'yumao', 'yumao8899', 'wikidb', charset='utf8');
cur = con.cursor(mdb.cursors.DictCursor)

start = time.clock()
for i in range(0, len(statuses)):
    status = statuses[i]
    text = ' '.join([status['text'], status['retweeted_status']['text']]) if status.has_key('retweeted_status') else status['text']
    print '(' + str(i) + ')', text
    collectTags(text)
    print
print 'Timeout:', time.clock() - start

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