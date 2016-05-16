# -------------------------------------------
# package  : epana
# author   : evan.phelps@gmail.com
# created  : Sat May 14 19:04:23 EST 2016
# vim      : ts=4


import os
import sys
import unittest


sys.path.insert(0, os.path.abspath(__file__ + "/../../src"))


class tabular_test(unittest.TestCase):

    def test_rxnorm_req(self):
        pass


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(tabular_test)
    unittest.TextTestRunner(verbosity=2).run(suite)
