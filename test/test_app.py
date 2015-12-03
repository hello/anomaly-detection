import unittest
from app import feature_extraction
from app import normalize_data
import math

class TestFeatureExtractions(unittest.TestCase):

    def test_feature_extraction_empty(self):
        '''Test for empty data set'''
        features = feature_extraction({})
        self.assertEqual(0, len(features))

    def test_feature_extraction(self):
        dataset = {
            'key' : [1,1,1,1]
        }
        features = feature_extraction(dataset)
        self.assertEqual(1, len(features))
        self.assertEqual(2, len(features[0]))
        self.assertEqual([1,1] , features[0])
    


class TestNormalizeData(unittest.TestCase):
    def test_nan(self):
        matrix = [[285560.5, 0.0, 0.0], [106637.5, 0.0, 0.0]]
        normalized = normalize_data(matrix)
        for row in normalized:
            for col in row:
                self.assertEquals(False, math.isnan(col))

if __name__ == '__main__':
    unittest.main()
