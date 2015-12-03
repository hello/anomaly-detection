import unittest
from app import feature_extraction


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
    

if __name__ == '__main__':
    unittest.main()