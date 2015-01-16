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

from ppp_datamodel import Triple, Resource, Missing, List, JsonldResource
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

PAPER_FIELDS = ('abstract_s', 'releasedDate_s', 'modifiedDateY_i',
        'uri_s', 'halId_s', 'title_s', 'authFullName_s', 'arxivId_s',
        'authFirstName_s', 'authLastName_s', 'authOrganism_s',
        'labStructName_s', 'version_i', 'language_s')

def graph_from_paper(paper):
    same_as = list(filter(bool, (
        paper['halId_s'],
        paper.get('arxivId_s', None),
        )))
    organizations = paper.get('authOrganism_s', []) + \
            paper.get('labStructName_s', [])
    authors = [{'@type': 'Person',
                'name': fullname,
                'givenName': firstname,
                'familyName': lastname,
               }
               for (fullname, firstname, lastname) in
               zip(paper['authFullName_s'], paper['authFirstName_s'],
                   paper['authLastName_s'])]
    d = {
            '@type': 'ScholarlyArticle',
            '@context': 'http://schema.org',
            'description': paper.get('abstract_s', None),
            'datePublished': paper['releasedDate_s'],
            'dateModified': paper['modifiedDateY_i'],
            '@id': paper['uri_s'],
            'isSameAs': paper['halId_s'],
            'name': paper['title_s'],
            'sourceOrganization': [
                {'@type': 'Organization',
                 'name': x,
                } for x in organizations],
            'version': paper.get('version_i', None),
            'author': authors,
            'inLanguage': paper['language_s'],
            }
    d = {x: y for (x, y) in d.items() if y is not None}
    return d

def paper_resource_from_paper(paper):
    paper_graph = graph_from_paper(paper)
    paper_graph['author'] = [
            {'@type': 'Person',
             '@context': 'http://schema.org',
             '@id': author, # TODO: Use an actual ID
             }
            for author in paper['authFullName_s']]
    return JsonldResource(paper['title_s'][0],
            graph=paper_graph)

def author_resources_from_paper(paper):
    paper_graph = graph_from_paper(paper)
    authors = paper_graph.pop('author')
    return [JsonldResource(author['name'],
            graph={
                '@context': 'http://schema.org',
                '@type': 'Person',
                '@id': author['name'], # TODO: Use an actual ID
                '@reverse': {
                    'author': paper_graph
                    },
                })
            for author in authors]


def replace_author(triple):
    if not isinstance(triple.subject, Resource):
        # Can't handle subtrees that are not a paper name
        return triple
    paper_title = triple.subject.value
    papers = query('title_s:"%s"~3' % paper_title,
            PAPER_FIELDS)
    return List(list(itertools.chain.from_iterable(
            map(author_resources_from_paper,  papers))))

def replace_paper(triple):
    if not isinstance(triple.object, Resource):
        # Can't handle subtrees that are not a paper name
        return triple
    papers = query('authFullName_s:"%s"' % triple.object.value,
            PAPER_FIELDS)
    return List(list(map(paper_resource_from_paper,  papers)))

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
