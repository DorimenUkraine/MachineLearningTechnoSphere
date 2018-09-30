import index
import simple9
import varbyte
import os.path
import mmh3
import pickle

import struct
import array
import textwrap

def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i + n]

def optimize(encoder):
	m, ids = index.load()

	n_of_buckets = max(len(m) / 100 + 1, len(m))
	res = [bytearray() for i in xrange(n_of_buckets)]


	with open('back_index.bin', 'wb') as fout:

		ptr = 0
		for key in sorted(m.keys()):
			values = map(lambda x: struct.unpack('I', x)[0], [x for x in chunks(m[key], 4)])
			l = encoder.compress(values, fout)
			#res[key] = (ptr, l)
			res[key % n_of_buckets].extend(struct.pack('i', key))
			res[key % n_of_buckets].extend(struct.pack('i', ptr))
			res[key % n_of_buckets].extend(struct.pack('i', l))
			ptr += l

	# check compression/decompression function
	# with open('back_index.bin', 'rb') as f:
	# 	for key in m:
	# 		rr = simple9.decompress(f, res[key][0], res[key][1])
	# 		if not (set(sorted(m[key])) == rr):
	# 			print('ERROR')
	# 			print(sorted(m[key]))
	# 			print(rr)
	# 			print(res[key])
	# 			raise Exception()

	dct = open('dict.bin', 'wb')
	dct_head = open('dict_head.bin', 'wb')

	it = 0

	dct_head.write(struct.pack('I', n_of_buckets))

	for i in xrange(n_of_buckets):
		dct_head.write(struct.pack('I', it))
		dct.write(res[i])
		it += len(res[i])

	dct_head.write(struct.pack('I', it))

	dct_head.close()
	dct.close()

if __name__ == '__main__':

	if os.path.exists('varbyte.bin'):
		encoder = varbyte
	else:
		encoder = simple9

	optimize(encoder)