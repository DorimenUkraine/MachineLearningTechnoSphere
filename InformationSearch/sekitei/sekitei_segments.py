 # coding: utf-8


import sys
import os
import re
import random
import time
from urlparse import urlparse
import urllib
from sklearn.cluster import KMeans
from sklearn.metrics import jaccard_similarity_score

from sklearn.ensemble import GradientBoostingClassifier

import numpy as np

#from nltk.metrics.distance import jaccard_distance
#from nltk.cluster import KMeansClusterer, euclidean_distance

features = {}
sekitei = None;
quotas = []
clf = GradientBoostingClassifier(max_depth=3, n_estimators=55)
qlink_proba = np.array([])

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

def inc(m, key):
	if (key not in m):
		m[key] = 0
	m[key] += 1

def extract_features_from_str(inp, res):
	s = urllib.unquote(inp.strip())

	try:
		s = urllib.unquote(s).decode('utf8')
	except Exception as e:
		try:
			s = urllib.unquote(s).decode('cp1251')
		except Exception as e1: 
			pass

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


		#wiki features
		#global regs
		regs = ['^User_talk:[^/]+$', '^[u|U]ser:[^/]+$',
				u'^Категория:[^/]+$',
				u'^Файл:[^/]+$',
				u'^Портал:[^/]+$',
				u'^Участник:[^/]+$',
				'^Special:[^/]+$',
				u'^Служебная:[^/]+$'
				'^[^/]+\.png$',
				'^[^/]+\.jpg$',
				'[^/]+\.svg$',
				u'^Обсуждение:[^/]+$',
				u'^Шаблон:[^/]+$',
				u'^Обсуждение_категории[^/]+$',
				'^[^/]+:[^/]+$',
				u'^Проект:[^/]+$',
				'^[^/]+,[^/]+$',
				'^[^/]+:[^/]+$',
				'^[^/]+:[^/]+\.[^/]$',
				'^[^/]+-[^/]+',
				'[0-9]{1}',
				'^[^/]+,_[^/]+$',
				'[^/]+_[^/]+',
				'\([^/]+\)',
				'^[^:]+_[^:]+$',
				'[^/]+,_[^/]+_',
				'[0-9]+',
				]

		for reg in regs:
			wiki = re.compile(reg)
			if wiki.match(segments[i]):
				inc(res, reg)


		if is_ascii(segments[i]):
			inc(res, 'ascii')

		eng = re.compile('[\w]')
		if eng.match(segments[i]):
			inc(res, 'eng')

		for reg in regs:
			wiki = re.compile(reg)
			if wiki.match(segments[i]):
				inc(res, reg)

def features_mask(inp):

	global features
	result = [0 for i in range(len(features))]

	f = {}
	extract_features_from_str(inp, f)

	keys = features.keys()
	for i in range(len(keys)):
		if keys[i] in f:
			result[i] = 1;

	return result


def extract_features(data1, data2):
	res = {}
	data = data1 + data2
	
	for s in data:
		extract_features_from_str(s, res)

	# global regs
	# for reg in regs:
	# 	if reg in res.keys():
	# 		print(reg, res[reg])


	# mb create new dict instead of removing keys ?
	for key in res.keys():
		if res[key] < 50:
			res.pop(key)

	return res

def define_segments(QLINK_URLS, UNKNOWN_URLS, QUOTA):
	global features
	features = extract_features(QLINK_URLS, UNKNOWN_URLS)

	if QLINK_URLS[0].find('wiki') != -1:
		print('features', features)

	global quotas
	global sekitei
	global clf
	global qlink_proba

	data = np.array(QLINK_URLS + UNKNOWN_URLS)
	qlink_proba = np.zeros(len(data))
	qlinks = np.zeros(len(data))
	qlinks[:len(QLINK_URLS)] = 1
	perm = np.random.permutation(len(data))

	data = data[perm]
	qlinks = qlinks[perm]
	X = np.array([features_mask(data[i]) for i in range(len(data))], dtype=float)

	clf.fit(X, qlinks)

	N = 11

	for c in range(5):

		quotas = np.array([0 for i in range(N)])
		#im = map(lambda x: features_mask(x), np.random.choice(data, N))

		#print('IM: ', im)

		sekitei = 	KMeans(n_clusters=N, max_iter=200)
					#Kmeans(k=N, eps=0.00001, max_iter=10)
					#KMeansClusterer(num_means=N, distance=my_jaccard, initial_means=im, avoid_empty_clusters=True, repeats=1, conv_test=0.0001)
					#DBSCAN(eps=0.2, metric=my_jaccard)

		sekitei.fit(X)
		#labels = np.array(sekitei.cluster(X, True))

		#print('labels', labels)

		for i in range(N):
			mask = (sekitei.labels_ == i)
			quotas[i] = (mask.sum() * QUOTA * 1.0) / len(data)


			if quotas[i] != 0:
				qlink_proba[i] = 1.0 * (qlinks[mask] == 1).sum() / mask.sum()

			#quotas[i] = ((labels == i).sum() * QUOTA * 1.0) / len(data)

		#print('quotas = ', quotas)

		if quotas.max() > 3000:
			N += 1
		else:
			break

#
# returns True if need to fetch url
#
def fetch_url(url):
	global sekitei
	global quotas
	global clf

	uf = features_mask(url)
	label = sekitei.predict(np.array([np.array(uf)], dtype=float))
	#label = np.array(sekitei.classify_vectorspace(np.array([np.array(uf)])))

	#print('label = ', label)
	#print('quotas = ', quotas)

	if quotas[label] >= 1.0:
		if clf.predict(np.array([np.array(uf)]))[0] == 1:
			quotas[int(label)] -= 1.0
			return True
		else:
			pr = np.random.random()
			if pr > qlink_proba[int(label)]:
				quotas[int(label)] -= 1.0
				return True

	return False

	#return sekitei.fetch_url(url);
	#return True;