# Compress list of positive increasing int values
def compress(data, fout):
	#fout = open(file_name, 'wb')

	prev = 0
	l = 0

	for i in xrange(len(data)):
		residuals = []
		val = data[i] - prev

		while val >= 128:
			residuals.append(val % 128)
			val /= 128
		residuals.append(val)
		residuals[0] += 128

		for c in reversed(residuals):
			fout.write(chr(c))
			l += 1

		prev = data[i]
	return l
	#fout.close()

def decompress_generator(file_name, start, l):
	fin = open(file_name, 'rb')
	fin.seek(start)

	shift = 0
	#with open(file_name, 'rb') as fin:
	end = False

	while (not end) and (l > 0):
		total = 0
		while True:
			val = fin.read(1)
			l -= 1

			if val == '':
				end = True
				break
			else:
				val = ord(val)

			total = (total << 7) + val % 128

			flag = ((val >> 7) == 1)
			if flag:
				break

		shift += total
		
		yield shift

	fin.close()

# Saving positive increasing ints to set
def decompress(fin, start, l):
	fin.seek(start)

	st = set([])
	shift = 0
	#with open(file_name, 'rb') as fin:
	end = False

	while (not end) and (l > 0):
		total = 0
		while True:
			val = fin.read(1)
			l -= 1

			if val == '':
				end = True
				break
			else:
				val = ord(val)

			total = (total << 7) + val % 128

			flag = ((val >> 7) == 1)
			if flag:
				break

		shift += total
		st.add(shift)
	return st