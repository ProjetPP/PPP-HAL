"""Request handler of the module."""

import requests
import functools
import itertools

from ppp_datamodel import Triple, Resource, Missing, List
from ppp_datamodel import Response, TraceItem
from ppp_libmodule.exceptions import ClientError

@functools.lru_cache(maxsize=128)
def query(query, fields):
    params = {'q': query, 'wt': 'json', 'fl': fields}
    d = requests.get('http://api.archives-ouvertes.fr/search/',
                     params=params).json()
    return d['response']['docs']

def replace_author(triple):
    if not isinstance(triple.subject, Resource):
        # Can't handle subtrees that are not a paper name
        return []
    papers = query(triple.subject.value, 'authFullName_s')
    return (List([Resource(y) for y in x['authFullName_s']]) for x in papers)

def replace_paper(triple):
    if not isinstance(triple.object, Resource):
        # Can't handle subtrees that are not a paper name
        return []
    papers = query('authFullName_s:"%s"' % triple.object.value, 'title_s')
    return [List([Resource(x['title_s'][0]) for x in papers])]

def replace(triple):
    if triple.subject == Missing() and triple.object == Missing():
        # Too broad
        return []
    elif triple.subject != Missing() and triple.object != Missing():
        # TODO: yes/no question
        return []
    elif triple.object == Missing():
        # Looking for the author of a paper
        return replace_author(triple)
    elif triple.subject == Missing():
        # Looking for the papers of a researcher
        return replace_paper(triple)
    else:
        raise AssertionError(triple)

def traverse_subtrees(tree):
    traverse_left = (Triple(x, tree.predicate, tree.object)
                     for x in traverse(tree.subject))
    traverse_right = (Triple(tree.subject, tree.predicate, x)
                      for x in traverse(tree.object))
    return itertools.chain(traverse_left, traverse_right)

def traverse(tree):
    if not isinstance(tree, Triple):
        return []
    elif tree.predicate not in (Resource('author'), Resource('writer')):
        return traverse_subtrees(tree)
    else:
        return replace(tree)

class RequestHandler:
    def __init__(self, request):
        self.request = request

    def answer(self):
        return (Response(self.request.language,
                         x,
                         {},
                         self.request.trace + [TraceItem('HAL', x, {})])
                for x in traverse(self.request.tree))
