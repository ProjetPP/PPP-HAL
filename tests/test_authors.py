import unittest

from ppp_datamodel import Missing, Triple, Resource, Sentence, List
from ppp_datamodel import Intersection, JsonldResource
from ppp_datamodel.communication import Request, TraceItem, Response
from ppp_libmodule.tests import PPPTestCase
from ppp_hal import app

class TestDefinition(PPPTestCase(app)):
    config_var = 'PPP_HAL_CONFIG'
    config = '''{"apis": ["http://api.archives-ouvertes.fr/search/"],
                "memcached": {"servers": ["127.0.0.1"], "timeout": 10}}'''

    def testSearchAuthors(self):
        q = Request('1', 'en', Triple(
            Resource('A Hierarchical Resource Reservation Algorithm for Network Enabled Servers of the french department of research.'),
            Resource('author'),
            Missing()))
        r = self.request(q)
        self.assertEqual(len(r), 1, r)
        self.assertIsInstance(r[0].tree, List)
        self.assertEqual({x.value for x in r[0].tree.list}, {
            'Eddy Caron',
            'Frédéric Desprez',
            'F. Petit',
            'V. Villain'})
        self.assertIn(JsonldResource('Eddy Caron', graph={'@id': 'Eddy Caron'}),
                r[0].tree.list)

    def testSearchPapers(self):
        q = Request('1', 'en', Triple(
            Missing(),
            Resource('author'),
            Resource('Eddy Caron')))
        r = self.request(q)
        self.assertEqual(len(r), 1, r)
        self.assertIsInstance(r[0].tree, List)
        self.assertIn('Deployment of a hierarchical middleware',
                {x.value for x in r[0].tree.list})

    def testIntersection(self):
        q = Request('1', 'en', Intersection([
            Triple(
                Resource('A Hierarchical Resource Reservation Algorithm for Network Enabled Servers of the french department of research.'),
                Resource('author'),
                Missing()),
            Triple(
                Resource('Deployment of a hierarchical middleware'),
                Resource('author'),
                Missing())]))
        q.__class__.from_dict(q.as_dict())
        r = self.request(q)
        self.assertEqual(len(r), 1, r)
        self.assertEqual({x.value for x in r[0].tree.list},
                {'Eddy Caron', 'Frédéric Desprez'})
        ec = JsonldResource('EC', graph={'@id': 'Eddy Caron'})
        fd = JsonldResource('FD', graph={'@id': 'Frédéric Desprez'})
        self.assertIn(r[0].tree.list, ([ec, fd], [fd, ec]))

    def testRecursive(self):
        q = Request('1', 'en', Triple(
            Triple(
                Resource('The first cycles in an evolving graph'),
                Resource('author'),
                Missing()),
            Resource('birth date'),
            Missing()))
        r = self.request(q)
        self.assertEqual(len(r), 1, r)
        self.assertIsInstance(r[0].tree, Triple)
        self.assertIsInstance(r[0].tree.subject, List)
        self.assertEqual(r[0].tree.predicate, Resource('birth date'))
        self.assertIn(
                'Donald E. Knuth',
                {x.value for x in r[0].tree.subject.list})
        self.assertIn(JsonldResource('DK', graph={'@id': 'Donald E. Knuth'}),
                r[0].tree.subject.list)

    def testNotTooLarge(self):
        q = Request('1', 'en', Triple(
            Resource('Le Petit Prince'),
            Resource('author'),
            Missing()))
        r = self.request(q)
        self.assertEqual(r, [])

    @unittest.skip # This test would work if HAL was not buggy
    def testNotTooStrict(self):
        q = Request('1', 'en', Triple(
            Resource('Le petit prince et la mathématicienn'),
            Resource('author'),
            Missing()))
        r = self.request(q)
        self.assertEqual(len(r), 1, r)
