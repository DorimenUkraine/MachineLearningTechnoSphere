import index
import simple9
import varbyte

import struct

import os.path

import cPickle as pickle
import tree

import mmh3

def evaluate(head):

	idx = -1
	res = set([])

	while idx != 100000000:
		idx = head.index + 1
		head.goto(idx)

		a = head.recompute_index()

		idx = a
		#print('a', a)
		if a != 100000000:
			res.add(a)

	return res

if __name__ == '__main__':

	if os.path.exists('varbyte.bin'):
		encoder = varbyte
	else:
		encoder = simple9

	ids = {}
	m = {}
	with open('ids.bin', 'r') as fin:
		ids = pickle.load(fin)

	dct_head = open('dict_head.bin', 'rb')
	dct = open('dict.bin', 'rb')

	DOCS_CNT = max(ids.keys())
	n_of_buckets = struct.unpack('i', dct_head.read(4))[0]

	fin = open('back_index.bin', 'rb')

	query = raw_input()

	while query != None:
		s = query.strip().decode('utf-8').lower()

		tokens = tree.tokenize_query(s, 'back_index.bin', dct_head, dct, n_of_buckets, encoder, DOCS_CNT)
		head = tree.build_query_tree(tokens)

		res = evaluate(head)

		print(query)
		print(len(res))
		for r in sorted(res):
			print(ids[r])

		try:
			query = raw_input()
			continue
		except (EOFError):
			break

	fin.close()
	dct_head.close()
	dct.close()