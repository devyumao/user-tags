#coding=utf-8
'''
Created on 2014-2-20

@author: yuzhang
'''
import MySQLdb as mdb
from stopWords import removeStopWordsJbg
import jieba.posseg as jbp
import jieba as jb
import re, json

def getWordsFromWiki(text):
    text = text.lower()
    htmlRE = re.compile(r'<.*?>')
    text = htmlRE.sub(' ', text)
    urlRE = re.compile(r'[a-z]+://[^\s]*')
    text = urlRE.sub(' ', text)
    return removeStopWordsJbg(jbp.cut(text))

def isWordWithMaxFlag(flag):
    return flag != 'x'

con = mdb.connect('localhost', 'yumao', 'yumao8899', 'wikidb', charset='utf8');
cur = con.cursor(mdb.cursors.DictCursor)
cur.execute("SELECT id FROM Page")
rows = cur.fetchall()
len = len(rows)
print 'total:', len

jb.enable_parallel()

for i in range(1800, len):
    row = rows[i]
    cur.execute("SELECT text FROM Page WHERE id = %s", [row['id']])
    page = cur.fetchone()
    words = [[w.word, w.flag] for w in getWordsFromWiki(page['text']) if isWordWithMaxFlag(w.flag)]
    res = cur.execute("UPDATE Page SET words = %s WHERE id = %s", [json.dumps(words, ensure_ascii=False), row['id']])
    print i, res, row['id']
    if 0 == i % 1000:
        con.commit()
        print 'commit'
con.commit() 
# cur.execute("SELECT words FROM Page WHERE id = 397")
# row = cur.fetchone()
# print json.loads(row['words'])[2][0] == '世界'

   
cur.close()
con.close()
