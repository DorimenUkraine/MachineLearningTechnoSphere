from docreader import parse_command_line
from docreader import DocumentStreamReader

from doc2words import extract_words
import cPickle as pickle

import struct
import array

import mmh3
import os

res = {}
cnt = 0
ids = {}

def expand_back_index(doc, id):
	global ids

	ids[id] = doc.url

	words = set(extract_words(doc.text))

	for word in words:
		h = mmh3.hash(word.encode('utf-8'))

		if h in res:
			res[h].extend(struct.pack('I', id))
		else:
			res[h] = bytearray()
			res[h].extend(struct.pack('I', id))

	

def save(file, fids):
	pickle.dump(res, file)
	pickle.dump(ids, fids)
def load():
	fin = open('back_index.bin', 'r')
	data = pickle.load(fin)
	fin.close()

	fin = open('ids.bin', 'r')
	ids = pickle.load(fin)
	fin.close()

	return data, ids


if __name__ == '__main__':
	parsed_line = parse_command_line().files

	try:
		os.remove('varbyte.bin')
	except:
		pass

	try:
		os.remove('simple9.bin')
	except:
		pass

	if parsed_line[0] == 'varbyte':
		with open('varbyte.bin', 'wb') as f:
			f.write('a')
	else:
		with open('simple9.bin', 'wb') as f:
			f.write('a')

	reader = DocumentStreamReader(parsed_line[1:])

	cnt = 0
	for doc in reader:
		expand_back_index(doc, cnt)
		cnt += 1

	fout = open('back_index.bin', 'w')
	fids = open('ids.bin', 'w')
	save(fout, fids)
	fout.close()
 