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

class APIS:
    PAPERS = 'search'
    AUTHORS = 'ref/author'

def _query(query, fields, api):
    params = {'q': query, 'wt': 'json', 'fl': ','.join(fields)}
    streams = (requests.get('%s/%s/' % (url, api), params=params, stream=True)
               for url in Config().apis)
    docs_lists = (s.json()['response']['docs'] for s in streams)
    return list(itertools.chain.from_iterable(docs_lists))

def query(query, fields, api=APIS.PAPERS):
    mc = connect_memcached()
    key = (query, fields, api)
    key = 'ppp-hal-%s' + hashlib.md5(pickle.dumps(key)).hexdigest()
    r = mc.get(key)
    if not r:
        r = _query(query, fields, api)
        mc.set(key, r, time=Config().memcached_timeout)
    return r


AUTHOR_FIELDS = ('url_s', 'email_s',
        'firstName_s', 'lastName_s', 'fullName_s')

def author_graph_from_docid(docid):
    authors = query('docid:%s' % docid, AUTHOR_FIELDS, api=APIS.AUTHORS)
    assert len(authors) == 1, authors
    author = authors[0]
    urls = author.get('url_s', [])
    emails = author.get('email_s', [])
    if isinstance(urls, str):
        urls = [urls]
    # Some researchers put their lab's URL instead of a personal URL.
    # This heuristic seems to have no false positives (but a few
    # false negatives)
    urls = [x for x in urls if author['lastName_s'].lower() in x.lower()]

    if isinstance(emails, str):
        emails = [emails]
    emails = ['mailto:%s' % x for x in emails]
    uris = emails + urls
    d = {'@context': 'http://schema.org',
         '@type': 'Person',
         'name': author['fullName_s'],
         'givenName': author['firstName_s'],
         'familyName': author['lastName_s']}
    if uris:
        d['sameAs'] = uris
    return d




PAPER_FIELDS = ('abstract_s', 'releasedDate_s', 'modifiedDateY_i',
        'uri_s', 'halId_s', 'title_s', 'authFullName_s', 'arxivId_s',
        'authOrganism_s', 'authId_i',
        'labStructName_s', 'version_i', 'language_s')

def graph_from_paper(paper):
    same_as = []
    if 'arxivId_s' in paper:
        same_as.append('http://arxiv.org/abs/%s' % paper['arxivId_s'])
    organizations = paper.get('authOrganism_s', []) + \
            paper.get('labStructName_s', [])
    # TODO: group everything in a single request
    authors = [author_graph_from_docid(x)
               for x in paper['authId_i']]
    d = {
            '@type': 'ScholarlyArticle',
            '@context': 'http://schema.org',
            'description': paper.get('abstract_s', None),
            'datePublished': paper['releasedDate_s'],
            'dateModified': paper['modifiedDateY_i'],
            '@id': paper['uri_s'],
            'sameAs': same_as,
            'name': paper['title_s'],
            'sourceOrganization': [
                {'@type': 'Organization',
                 'name': x,
                } for x in organizations],
            'version': paper.get('version_i', None),
            'author': authors,
            'inLanguage': paper['language_s'],
            'url': paper['uri_s'],
            }
    d = {x: y for (x, y) in d.items() if y is not None}
    return d

def paper_resource_from_paper(paper):
    paper_graph = graph_from_paper(paper)
    return JsonldResource(paper['title_s'][0],
            graph=paper_graph)

def author_resources_from_paper(paper):
    paper_graph = graph_from_paper(paper)
    authors = paper_graph.pop('author')
    for author in authors:
        author['@reverse'] = {'author': paper_graph}
    return (JsonldResource(author['name'], graph=author)
            for author in authors)


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
