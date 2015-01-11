from ppp_datamodel import Missing, Triple, Resource, Sentence, List
from ppp_datamodel.communication import Request, TraceItem, Response
from ppp_libmodule.tests import PPPTestCase
from ppp_hal import app

class TestDefinition(PPPTestCase(app)):
    config_var = 'PPP_HAL'
    config = ''
    def testSearchAuthors(self):
        q = Request('1', 'en', Triple(
            Resource('A Hierarchical Resource Reservation Algorithm'),
            Resource('author'),
            Missing()))
        r = self.request(q)
        self.assertEqual(len(r), 1, r)
        self.assertIsInstance(r[0].tree, List)
        self.assertEqual(set(r[0].tree.list), {
            Resource('Eddy Caron'),
            Resource('Frédéric Desprez'),
            Resource('F. Petit'),
            Resource('V. Villain')})

    def testSearchPapers(self):
        q = Request('1', 'en', Triple(
            Missing(),
            Resource('author'),
            Resource('Eddy Caron')))
        r = self.request(q)
        self.assertEqual(len(r), 1, r)
        self.assertIsInstance(r[0].tree, List)
        self.assertIn(
                Resource('Deployment of a hierarchical middleware'),
                r[0].tree.list)

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
                Resource('Donald E. Knuth'),
                r[0].tree.subject.list)
