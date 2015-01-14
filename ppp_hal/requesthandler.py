"""Request handler of the module."""

import pickle
import hashlib
try:
    import pylibmc as memcache
except ImportError:
    try:
        import memcache
    except ImportError:
        raise ImportError('Neither pylibmc or python3-memcached is installed')
import requests
import functools
import itertools

from ppp_datamodel import Triple, Resource, Missing, List
from ppp_datamodel import Response, TraceItem
from ppp_libmodule.exceptions import ClientError
from ppp_libmodule.simplification import simplify

from .config import Config

def connect_memcached():
    mc = memcache.Client(Config().memcached_servers)
    return mc

def _query(query, fields):
    params = {'q': query, 'wt': 'json', 'fl': fields}
    streams = (requests.get(url, params=params, stream=True)
               for url in Config().apis)
    docs_lists = (s.json()['response']['docs'] for s in streams)
    return list(itertools.chain.from_iterable(docs_lists))

def query(query, fields):
    mc = connect_memcached()
    key = 'ppp-hal-%s' + hashlib.md5(pickle.dumps((query, fields))).hexdigest()
    r = mc.get(key)
    if not r:
        r = _query(query, fields)
        mc.set(key, r, time=Config().memcached_timeout)
    return r

def replace_author(triple):
    if not isinstance(triple.subject, Resource):
        # Can't handle subtrees that are not a paper name
        return triple
    paper_title = triple.subject.value
    papers = query('title_s:"%s"~3' % paper_title,
            'authFullName_s,title_s')
    authors = itertools.chain(*(x['authFullName_s'] for x in papers))
    return List([Resource(x) for x in authors])

def replace_paper(triple):
    if not isinstance(triple.object, Resource):
        # Can't handle subtrees that are not a paper name
        return triple
    papers = query('authFullName_s:"%s"' % triple.object.value, 'title_s')
    return List([Resource(x['title_s'][0]) for x in papers])

def replace(triple):
    if triple.subject == Missing() and triple.object == Missing():
        # Too broad
        return triple
    elif triple.subject != Missing() and triple.object != Missing():
        # TODO: yes/no question
        return triple
    elif triple.object == Missing():
        # Looking for the author of a paper
        return replace_author(triple)
    elif triple.subject == Missing():
        # Looking for the papers of a researcher
        return replace_paper(triple)
    else:
        raise AssertionError(triple)

def traverser(tree):
    if isinstance(tree, Triple) and \
            tree.predicate in (Resource('author'), Resource('writer')):
        return replace(tree)
    else:
        return tree

def fixpoint(tree):
    old_tree = None
    tree = simplify(tree)
    while tree and old_tree != tree:
        old_tree = tree
        tree = tree.traverse(traverser)
        if not tree:
            return None
        tree = simplify(tree)
    return tree

class RequestHandler:
    def __init__(self, request):
        self.request = request

    def answer(self):
        tree = fixpoint(self.request.tree)
        if tree and \
                (not isinstance(tree, List) or tree.list):
            trace = self.request.trace + [TraceItem('HAL', tree, {})]
            return [Response(self.request.language, tree, {}, trace)]
        else:
            return []
