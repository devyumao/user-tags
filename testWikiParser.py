#coding=utf-8
'''
Created on 2014-2-22

@author: yuzhang
'''

templates = {}

import codecs

from mediawikiParser.preprocessor import make_parser
preprocessor = make_parser(templates)

from mediawikiParser.text import make_parser
parser = make_parser()

fileObj = codecs.open("mediawikiParser/wikitext.txt", "r", "utf-8")
source = fileObj.read()

preprocessed_text = preprocessor.parse(source)
output = parser.parse(preprocessed_text.leaves())

print output.leaves()

