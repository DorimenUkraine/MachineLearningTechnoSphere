
# coding: utf-8

# In[75]:

import sys
import re
import random
from operator import itemgetter
from urlparse import urlparse
import urllib
from random import sample
import re

def inc(m, key):
	if (key not in m):
		m[key] = 0
	m[key] += 1

def extract_features(INPUT_FILE_1, INPUT_FILE_2, OUTPUT_FILE):
	res = {}
	data1 = []
	data2 = []

	with open(INPUT_FILE_1) as f:
		data1 = list(f)
	with open(INPUT_FILE_2) as f:
		data2 = list(f)

	data = sample(data1, 1000) + sample(data2, 1000)
	
	for s in data:
		s = urllib.unquote(s.strip())

		urp = urlparse(s)

		query = urp.query
		path = urp.path

		segments = path.split('/')[1:]
		if path.endswith('/'):
			del segments[-1]

		#query
		queries = query.split('&')
		for q in queries:
			key = ''
			idx = q.rfind('=')
			if idx == -1:
				key = 'param_name:'
			else:
				key = 'param:'
			key += q
			inc(res, key)

		#segments:<len>
		key = 'segments:' + str(len(segments))
		inc(res, key)

		for i in range(len(segments)):
			#segments[i] = urllib.unquote(segments[i])

			#segment_name_<index>:<string>
			key = 'segment_name_' + str(i) + ':' + segments[i]
			inc(res, key)

			#segment_[0-9]_<index>:1
			num = re.compile('^[0-9]+$')
			if num.match(segments[i]):
				key = 'segment_[0-9]_' + str(i) + ':1'
				inc(res, key)

			#segment_substr[0-9]_<index>:1
			sub = re.compile('^[^\d]+\d+[^\d]+$')
			if sub.match(segments[i]):
				key = 'segment_substr[0-9]_' + str(i) + ':1'
				inc(res, key)


			#segment_ext_<index>:<extension value>
			idx = segments[i].rfind('.')
			if idx != -1:
				key = 'segment_ext_' + str(i) + ':' + segments[i][idx + 1:]
				inc(res, key)

				#segment_ext_substr[0-9]_<index>:<extension value>
				if sub.match(segments[i]):
					key = 'segment_ext_substr[0-9]_' + str(i) + ':' + segments[i][idx + 1:]
					inc(res, key)

			#segment_len_<index>:<segment length>
			key = 'segment_len_' + str(i) + ':' + str(len(segments[i]))
			inc(res, key)
	
	for key in res.keys():
		if res[key] < 100:
			res.pop(key)

	out = [key + '\t' + str(value) for key, value in sorted(res.iteritems(), key=lambda (k, v): (v, k))]

	fout = open(OUTPUT_FILE, 'w+')
	fout.write('\n'.join(reversed(out)))
	fout.close()

#extract_features('./data/urls.lenta.examined', './data/urls.lenta.general', './out.txt')





