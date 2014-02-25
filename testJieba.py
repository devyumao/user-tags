#coding=utf-8
'''
Created on 2014-2-22

@author: yuzhang
'''

import jieba.posseg as jbp
import jieba as jb
import time

jb.enable_parallel()
jb.initialize()
text = '''
'''

start = time.clock()
for i in range(1000000):
    jb.cut(text)
print time.clock() - start

start = time.clock()
for i in range(1000000):
    jbp.cut(text)
print time.clock() - start