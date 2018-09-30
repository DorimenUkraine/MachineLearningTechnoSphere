import re
import unittest
import struct
import mmh3

SPLIT_RGX = re.compile(r'\w+|[\(\)&\|!]', re.U)
MMAX = 100000000

def get_shifts(key, dct_head, dct, n_of_buckets):
    idx = key % n_of_buckets

    # print('idx', idx, n_of_buckets)
    dct_head.seek(4 + 4 * idx)

    val = dct_head.read(4)
    b1 = struct.unpack('I', val)[0]

    val = dct_head.read(4)
    b2 = struct.unpack('I', val)[0]

    dct.seek(b1)

    ptr = -1
    l = -1

    it = b1
    while it < b2:
        dct.seek(it)

        val = dct.read(4)
        k = struct.unpack('i', val)[0]

        if k == key:
            ptr = struct.unpack('i', dct.read(4))[0]
            l = struct.unpack('i', dct.read(4))[0]

        it += 12

    return ptr, l

class QtreeTypeInfo:
    def __init__(self, value, op=False, bracket=False, term=False):
        self.value = value
        self.is_operator = op
        self.is_bracket = bracket
        self.is_term = term
        self.last = MMAX
        self.index = -1

    def __repr__(self):
        return repr(self.value)

    def __eq__(self, other):
        if isinstance(other, QtreeTypeInfo):
            return self.value == other.value
        return self.value == other


class QTreeTerm(QtreeTypeInfo):
    def __init__(self, term, file_name, dct_head, dct, n_of_buckets, encoder):
        QtreeTypeInfo.__init__(self, term, term=True)
        h = mmh3.hash(term.strip())

        ptr, l = get_shifts(h, dct_head, dct, n_of_buckets)

        if ptr != -1 and l != -1:
            self.generator = encoder.decompress_generator(file_name, ptr, l)
        else:
            self.index = MMAX
            self.generator = xrange(0)

    def goto(self, next_idx):
        if self.index >= next_idx:
            return

        for i in self.generator:
            # print('i', i)
            if i >= next_idx:
                self.index = i
                return

        # let it be max possible index
        self.index = MMAX

    def recompute_index(self):
        return self.index

class QTreeOperator(QtreeTypeInfo):
    def __init__(self, op, last_index):
        QtreeTypeInfo.__init__(self, op, op=True)
        self.priority = get_operator_prio(op)
        self.left = None
        self.right = None
        self.last_index = last_index

    def goto(self, next_idx):
        if self.value == '|':

            self.left.goto(next_idx)
            self.right.goto(next_idx)
            self.index = min(self.left.index, self.right.index)

            return


        if self.value == '&':
            self.left.goto(next_idx)
            self.right.goto(next_idx)

            self.index = max(self.left.index, self.right.index)

            return



        if self.value == '!':
            self.right.goto(next_idx)
            self.index = min(self.right.index, next_idx)

            if self.right.index != MMAX:
                self.last = self.right.index

    def recompute_index(self):
        if self.value == '|':

            a = self.left.recompute_index()
            b = self.right.recompute_index()

            self.index = min(a, b)

            return self.index

        if self.value == '&':
            while True:
                if self.left.index == MMAX or self.right.index == MMAX:
                    self.index = MMAX
                    break

                a = self.left.recompute_index()
                b = self.right.recompute_index()

                if a == b:
                    self.index = self.left.index
                    break

                if a < b:
                    self.left.goto(b)
                if a > b:
                    self.right.goto(a)

            return self.index

        if self.value == '!':
            if self.index == MMAX:
                return MMAX


            while True:
                b = self.right.recompute_index()

                if b != MMAX:
                    self.last = b
                else:
                    if self.index < self.last:
                        break
                    else:
                        if self.index <= self.last_index:
                            break
                        else:
                            self.index = MMAX
                            break

                if self.index < b:
                    break
                else:
                    self.index += 1
                    self.right.goto(self.right.index + 1)
                
            return self.index

class QTreeBracket(QtreeTypeInfo):
    def __init__(self, bracket):
        QtreeTypeInfo.__init__(self, bracket, bracket=True)


def get_operator_prio(s):
    if s == '|':
        return 0
    if s == '&':
        return 1
    if s == '!':
        return 2

    return None


def is_operator(s):
    return get_operator_prio(s) is not None


def tokenize_query(q, file_name, dct_head, dct, n_of_buckets, encoder, last_index):
    tokens = []
    for t in map(lambda w: w.encode('utf-8'), re.findall(SPLIT_RGX, q)):
        if t == '(' or t == ')':
            tokens.append(QTreeBracket(t))
        elif is_operator(t):
            tokens.append(QTreeOperator(t, last_index))
        else:
            tokens.append(QTreeTerm(t, file_name, dct_head, dct, n_of_buckets, encoder))

    return tokens


def build_query_tree(tokens):    
    balance = 0
    op = None
    min_op = 3
    min_op_balance = 100000000
    op_idx = -1
    
    if len(tokens) == 0:
        return None
    if len(tokens) == 1:
        return tokens[0]
    
    i = 0
    for token in tokens:
        
        if token.is_bracket:
            if token.value == '(':
                balance += 1
            else:
                balance -= 1
        elif token.is_operator:

            if (token.priority <= min_op and balance <= min_op_balance) or (balance < min_op_balance):
                op = token
                min_op = token.priority
                min_op_balance = balance
                op_idx = i
        
        i += 1
    
    if op is None:
        for token in tokens:
            if token.is_term:
                return token
    
    
#     print('op', op.value)
    
    left = build_query_tree(tokens[: op_idx])
    right = build_query_tree(tokens[op_idx + 1 :])
    
#     print('left', left)
#     print('right', right)
    
    res = tokens[op_idx]
    res.left = left
    res.right = right
    
    return res

def parse_query(q):
    tokens = tokenize_query(q)
    return build_query_tree(tokens)


""" Collect query tree to sting back. It needs for tests. """
def qtree2str(root, depth=0):
    if root.is_operator:
        need_brackets = depth > 0 and root.value != '!'
        res = ''
        if need_brackets:
            res += '('

        if root.left:
            res += qtree2str(root.left, depth+1)

        if root.value == '!':
            res += root.value
        else:
            res += ' ' + root.value + ' '

        if root.right:
            res += qtree2str(root.right, depth+1)

        if need_brackets:
            res += ')'

        return res
    else:
        return root.value