#!/usr/bin/env python

import re
import sys
import time
import random
import itertools
from zlib import crc32
import simplejson as json
from memcache import Client
from urllib2 import urlopen

chain_length = 25
# chains of longer lengths are weighted more heavily when picking the
# next follower. This list defines how heavily
chain_weights = range(1, chain_length)

class LookBehind(object):
    def __init__(self, size, init=[]):
        self.size = size
        self.data = []
        for x in init:
            self.data.append(x)

    def append(self, x):
        self.data.append(x)
        if len(self.data) > self.size:
            return self.data.pop(0)

    def __iter__(self):
        for x in reversed(self.data):
            yield x

    def __contains__(self, item):
        return item in self.data

    def __getitem__(self, n):
        return self.data[-(n+1)]

    def __len__(self):
        return len(self.data)

    def __bool__(self):
        return bool(self.data)

    def __repr__(self):
        return "LookBehind(%d, %r)" % (self.size, self.data)

class Token(object):
    types = dict(punc = re.compile(r'[?,!;:.()]').match,
                 word = re.compile(r'[A-Za-z0-9\'-]+').match,
                 whitespace = re.compile(r'|\s+').match)
    # must keep the splitter in sync with the types
    split_re = re.compile(r'(\s+|[A-Za-z0-9\'-]+|[?,!;:.()])')
    capnexts = '?!.'
    nospaces_after = '('

    def __init__(self, tok, kind = None):
        self.tok = tok.lower()
        self.kind = kind or self._kind()

    def _kind(self):
        for (t, fn) in self.types.iteritems():
            if fn(self.tok):
                return t
        raise TypeError('Unknown token type %r' % self)

    @classmethod
    def tokenize(cls, text, beginend = True):
        """Given a string of text, yield the non-whitespace tokens
           parsed from it"""
        if beginend:
            yield BeginToken()
        for x in cls.split_re.split(text):
            tok = cls(x)
            if tok.kind != 'whitespace':
                yield tok
        if beginend:
            yield EndToken()

    @classmethod
    def detokenize(cls, tokens):
        """Given a stream of tokens, yield strings that look like
           English sentences"""
        lookbehind = LookBehind(1)

        for tok in tokens:
            if isinstance(tok, BeginToken):
                continue
            elif isinstance(tok, EndToken):
                break

            text = tok.tok

            if (lookbehind
                and tok.kind == 'word'
                and lookbehind[0].tok not in cls.nospaces_after):
                yield ' '

            if not lookbehind or (lookbehind[0].tok in cls.capnexts):
                text = text[0].upper() + text[1:]

            yield text

            lookbehind.append(tok)

    def __repr__(self):
        return "Token(%r, %r)" % (self.tok, self.kind)

class BeginToken(Token):
    tok = '\01'
    kind = 'special'

    def __init__(self):
        pass
    def __repr__(self):
        return "BeginToken()"

class EndToken(Token):
    tok = '\02'
    kind = 'special'

    def __init__(self):
        pass
    def __repr__(self):
        return "EndToken()"

def limit(it, lim=None):
    if lim == 0:
        return
    if lim is None:
        return it
    return itertools.islice(it, 0, lim)

def token_followers(tokens):
    """Given a list of tokens, yield tuples of lists of tokens (up to
       chain_length) and the tokens that follow them. e.g.:

       >>> list(token_followers([1,2,3,4,5]))
       [([1], 2),
        ([2], 3),
        ([1, 2], 3),
        ([3], 4),
        ([2, 3], 4),
        ([1, 2, 3], 4),
        ([4], 5),
        ([3, 4], 5),
        ([2, 3, 4], 5),
        ([1, 2, 3, 4], 5)]
    """
    # TODO: we could generate SkipTokens too to match 'i really like
    # bacon' to 'i don't like bacon'. At the loss of some accuracy we
    # could even match 'i like bacon' to 'i don't really like bacon'
    lookbehind = LookBehind(chain_length)
    for token in tokens:
        if lookbehind:
            for x in token_predecessors(lookbehind):
                yield x, token
            
        lookbehind.append(token)

def token_predecessors(lb):
    """Given a LookBehind buffer, yield all of the sequences of the
       last N items, e.g.

    >>> lb = LookBehind(5)
    >>> lb.append(1)
    >>> lb.append(2)
    >>> lb.append(3)
    >>> lb.append(4)
    >>> lb.append(5)
    >>> list(token_predecessors(lb))
    [[5], [4, 5], [3, 4, 5], [2, 3, 4, 5], [1, 2, 3, 4, 5]]
    """
    [[1], [2, 1], [3, 2, 1], [4, 3, 2, 1], [5, 4, 3, 2, 1]]
    l = list(reversed(lb))
    for x in range(len(l)):
        yield l[-x-1:]

def hash_tokens(tokens):
    return str(crc32(''.join(tok.tok.encode('utf8')
                             for tok in tokens)))

def sum_dicts(d1, d2):
    ret = {}
    for d in d1, d2:
        for k, v in d.iteritems():
            ret[k] = ret.get(k, 0) + v
    return ret

def get_reddit_comments(cache):
    """Continually yield new comment-bodies from reddit.com"""
    url = 'http://www.reddit.com/comments.json?limit=100'

    def _seen_key(i):
        return str('seen_%s' % i)

    while True:
        s = urlopen(url).read().decode('utf8')

        js = json.loads(s)
        cms = js['data']['children']
        bodies = {}

        for cm in cms:
            cm = cm['data']
            if cm.get('body', None):
                bodies[cm['id']] = cm['body']

        seen = cache.get_multi([_seen_key(k)
                                for k in bodies.keys()])
        new = [k for k in bodies
               if _seen_key(k) not in seen]

        if new:
            print '%d new comments' % len(new)

            for k in new:
                body = bodies[k]
                # we have to pick between being able to sometimes
                # yield the same item twice, or sometimes never
                # yielding an item (e.g. if an exception is thrown
                # before control is passed back to us). we've chosen
                # the former here
                yield body

            cache.set_multi(dict((_seen_key(k), True)
                                 for k in new))

        time.sleep(35)

def save_chains(cache):
    """Continually get reddit comments and dump the resulting chains
       into memcached"""
    for cm in get_reddit_comments(cache):
        tokens = Token.tokenize(cm)
        followers = token_followers(tokens)
        hashed_followers = [(hash_tokens(f), tok)
                            for (f, tok)
                            in followers]
        if hashed_followers:
            hashes = map(lambda x: x[0], hashed_followers)
            cached_hashes = cache.get_multi(hashes)
            for h, tok in hashed_followers:
                text = tok.tok

                for_hash = cached_hashes.setdefault(h, {})
                for_hash[text] = for_hash.get(text, 0) + 1

            # TODO: it's possible to create items that are too big to
            # store in memcached (an obvious example is the followers
            # of a BeginToken()). We should trim those before storing
            cache.set_multi(cached_hashes)

def create_chain(cache):
    """Read the chains created by save_chains from memcached and yield
       a stream of predicted tokens"""
    lb = LookBehind(chain_length, [BeginToken()])

    while True:
        preds = list(token_predecessors(lb))
        hashes = dict((hash_tokens(x), len(x))
                      for x in preds)
        # dict(hash -> dict(follower -> weight))
        cached_hashes = cache.get_multi(hashes.keys())

        if not cached_hashes:
            # no idea what the next token should be. This should only
            # happen if the storage backend has dumped the list of
            # followers for the previous token (since if it has no
            # followers, it would at least have an EndToken follower)
            break

        # build up the weights for the next token based on
        # occurrence-counts in the source data * the length weight
        weights = {}
        for h, f_weights in cached_hashes.iteritems():
            for tok, weight in f_weights.iteritems():
                weights[tok] = (weights.get(tok, 0) + weight
                                * chain_weights[hashes[h]-1])

        # now with the finished weights, build a list by duplicating
        # the items according to their weight. So given {a: 2, b: 3},
        # generate the list [a, a, b, b, b]
        weighted_list = []
        for tok, weight in weights.iteritems():
            weighted_list.extend([tok] * weight)

        next = random.choice(weighted_list)

        if next == EndToken.tok:
            break

        token = Token(next)
        yield token

        lb.append(token)

def create_sentences(cache, length):
    """Create chains with create_chain and yield lines that look like
       English sentences"""
    while True:
        chain = limit(create_chain(cache), length)
        yield ''.join(Token.detokenize(chain))

if __name__ == '__main__':
    memc, op = sys.argv[1:]
    memc = memc.split(',')

    cache = Client(memc)

    if op == 'save':
        save_chains(cache)
    elif op == 'create':
        for x in limit(create_sentences(cache, 100), 100):
            print x
