"""Request handler of the module."""

import pickle
import hashlib
import requests
import functools
import itertools

# Import pylibmc if possible; import memcache otherwise.
# pylibmc is is more strict (ie. detects and raises errors instead
# of just ignoring them), but is not compatible with Pypy.
try:
    import pylibmc as memcache
except ImportError:
    try:
        import memcache
    except ImportError:
        raise ImportError('Neither pylibmc or python3-memcached is installed')

from ppp_datamodel import Triple, Resource, Missing, List, JsonldResource
from ppp_datamodel import Response, TraceItem
from ppp_libmodule.exceptions import ClientError
from ppp_libmodule import shortcuts

from .config import Config

def connect_memcached():
    mc = memcache.Client(Config().memcached_servers)
    return mc

# URLs of the different APIs provided by HAL, relative to the
# root of the website.
class APIS:
    PAPERS = 'search'
    AUTHORS = 'ref/author'

def _query(query, fields, api):
    """Perform a query to all configured APIs and concatenates all
    results into a single list."""
    params = {'q': query, 'wt': 'json', 'fl': ','.join(fields)}
    streams = (requests.get('%s/%s/' % (url, api), params=params, stream=True)
               for url in Config().apis)
    docs_lists = (s.json()['response']['docs'] for s in streams)
    return list(itertools.chain.from_iterable(docs_lists))

def query(query, fields, api=APIS.PAPERS):
    """Perform a query to all configured APIs and concatenates all
    results into a single list.
    Also handles caching."""
    mc = connect_memcached()

    # Construct a key suitable for memcached (ie. a string of less than
    # 250 bytes)
    key = (query, fields, api)
    key = 'ppp-hal-%s' + hashlib.md5(pickle.dumps(key)).hexdigest()

    # Get the cached value, if any
    r = mc.get(key)
    if not r:
        # If there is no cached value, query HAL and add the result to
        # the cache.
        r = _query(query, fields, api)
        mc.set(key, r, time=Config().memcached_timeout)
    return r


AUTHOR_FIELDS = ('url_s', 'email_s',
        'firstName_s', 'lastName_s', 'fullName_s')
"""Fields requested when making a request to the authors API."""

def author_graph_from_docid(docid):
    """Constructs the JSON-LD graph of an author from their docid
    (aka. authId in the papers API)."""
    authors = query('docid:%s' % docid, AUTHOR_FIELDS, api=APIS.AUTHORS)

    # There is always only one author per docid.
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
    # We are never sure to have an id, so we only use sameAs attributes
    # if there is one
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
"""Fields requested when making a request to the papers API."""

def graph_from_paper(paper):
    """Constructs the JSON-LD graph of a paper from the paper's
    data returned by HAL."""
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
            'potentialAction': {
                '@type': 'ViewAction',
                'image': 'https://hal.archives-ouvertes.fr/img/favicon.png',
                'target': paper['uri_s'],
                'name': [
                    {'@language': 'en',
                     '@value': 'View on HAL'},
                    {'@language': 'fr',
                     '@value': 'Voir sur HAL'},
                    ]
                }

            }
    d = {x: y for (x, y) in d.items() if y is not None}
    return d

def paper_resource_from_paper(paper):
    """Instanciate a JsonldResource from a paper data."""
    paper_graph = graph_from_paper(paper)
    return JsonldResource(paper['title_s'][0],
            graph=paper_graph)

def author_resources_from_paper(paper):
    """Instanciate a list of JsonldResource of the authors of a paper
    from a paper's data."""
    paper_graph = graph_from_paper(paper)
    authors = paper_graph.pop('author')
    for author in authors:
        author['@reverse'] = {'author': paper_graph}
    return (JsonldResource(author['name'], graph=author)
            for author in authors)


def replace_author(triple):
    """Tree traversal predicate that acts on triples to replace them
    with an author resource, if possible."""
    if not isinstance(triple.subject, Resource):
        # Can't handle subtrees that are not a paper name
        return triple
    paper_title = triple.subject.value
    papers = query('title_s:"%s"~3' % paper_title,
            PAPER_FIELDS)
    return List(list(itertools.chain.from_iterable(
            map(author_resources_from_paper,  papers))))

def replace_paper(triple):
    """Tree traversal predicate that acts on triples to replace them
    with an author resource, if possible."""
    if not isinstance(triple.object, Resource):
        # Can't handle subtrees that are not a paper name
        return triple
    papers = query('authFullName_s:"%s"' % triple.object.value,
            PAPER_FIELDS)
    return List(list(map(paper_resource_from_paper,  papers)))

def replace(triple):
    """Tree traversal predicate that acts on triples."""
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
    """Tree traversal predicate."""
    if isinstance(tree, Triple) and \
            not tree.predicate_set \
            .isdisjoint({Resource('author'), Resource('writer')}):
        return replace(tree)
    else:
        return tree

class RequestHandler:
    def __init__(self, request):
        self.request = request

    def answer(self):
        tree = shortcuts.traverse_until_fixpoint(traverser, self.request.tree)
        if tree and \
                (not isinstance(tree, List) or tree.list):
            return [shortcuts.build_answer(self.request, tree, {}, 'HAL')]
        else:
            return []
