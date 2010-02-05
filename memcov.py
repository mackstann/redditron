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

chain_length = 20

class LookBehind(object):
    def __init__(self, size, init=[]):
        self.size = size
        self.data = list(itertools.islice(init, size))

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
    lookbehind = LookBehind(chain_length)
    for token in tokens:
        if lookbehind:
            for x in token_predecessors(lookbehind):
                yield x, token
            
        lookbehind.append(token)

def token_predecessors(lb):
    l = list(reversed(lb))
    for x in range(len(l)):
        yield l[-x-1:]

def hash_tokens(tokens):
    return "%d_%d" % (crc32(''.join(tok.tok.encode('utf8')
                                    for tok in tokens)),
                      len(tokens))

def sum_dicts(d1, d2):
    keys = set(d1.keys() + d2.keys())
    return dict((k, d1.get(k, 0) + d2.get(k, 0))
                for k in keys)

def get_reddit_comments(cache):
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

        print 'sleeping'
        time.sleep(35)

def save_chains(cache):
    for cm in get_reddit_comments(cache):
        print 'making tokens for len==%d' % len(cm)
        tokens = list(Token.tokenize(cm))
        print 'making followers for %d tokens' % len(tokens)
        followers = list(token_followers(tokens))
        print 'made %d followers' % len(followers)
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
    lb = LookBehind(chain_length, [BeginToken()])

    while True:
        preds = token_predecessors(lb)
        hashes = map(hash_tokens, preds)
        cached_hashes = cache.get_multi(hashes)
        # TODO: we should add some weighting in here instead of
        # considering them all equal
        candidates = reduce(lambda x,y: x.union(y),
                            (set(hs.keys())
                             for hs
                             in cached_hashes.values()),
                            set())
        candidates = list(candidates)

        if not candidates:
            break

        next = random.choice(candidates)

        if next == EndToken.tok:
            break

        token = Token(next)
        yield token

        lb.append(token)

def create_sentences(cache, length):
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
