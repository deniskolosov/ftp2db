import unittest

from consumer import validate


class TestValidateRowFunc(unittest.TestCase):
    def test_validate_row_func(self):
        valid_row = ['1', '1100', '4.2', '3', '05/10/2016']

        invalid_rows = [['1100', '4.2', '3', '05/10/2016'],
                        ['', '1100', '4.2', '3', '05/10/2016'],
                        ['1', '', '4.2', '3', '05/10/2016'],
                        ['1', '0', '4.2', '3', '05/10/2016'],
                        ['1', '1100', '', '3', '05/10/2016'],
                        ['1', '1100', '4.2', '3', ''],
                        ['1', '1100', '4.2', '', '05/10/2016'],
                        ['1', '1100', '4.2', '1', '05/10/2016']]

        self.assertTrue(validate(valid_row))

        for row in invalid_rows:
            self.assertFalse(validate(row))
