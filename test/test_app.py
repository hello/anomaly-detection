import unittest
from app import feature_extraction
from app import normalize_data
import math
import numpy as np

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
        matrix1 = [[11.0, 2.0, 1.0], [10.0, 2.0, 1.0]]
        matrix2 = [[11.0, 2.0, 1.0], [10.0, 2.0, 1.0]]
        matrix3 = [[11.0, 2.0, 1.0], [10.0, 2.0, 1.0], [10.0, 1.0, 1.0]]
        matrix4 = [[11.0, 1.0, 2.0], [10.0, 2.0, 1.0]]
        matrix5 = [[0.0, 1.0, 2.0], [10.0, 2.0, 1.0]]

        all_matrices = [matrix1, matrix2, matrix3, matrix4, matrix5]
        for matrix in all_matrices:
            normalized = normalize_data(matrix)
            for row in normalized:
                for col in row:
                    self.assertEquals(False, math.isnan(col))

    def test_empty(self):
        matrix1 = [[11.0, 2.0, 1.0], [10.0, 2.0, 1.0]]
        matrix2 = [[11.0, 2.0, 1.0], [10.0, 2.0, 1.0]]
        matrix3 = [[11.0, 2.0, 1.0], [10.0, 2.0, 1.0], [10.0, 1.0, 1.0]]

        empty_matrices = [matrix1, matrix2, matrix3]

        for matrix in empty_matrices:
            normalized = normalize_data(matrix)
            self.assertEquals(True, len(normalized)==0)

    def test_result(self):
        matrix4 = [[0.0, 0.0, 0.0], [1.0, 1.0, 1.0], [2.0, 2.0, 2.0]]
        matrix5 = [[0.0, 0.0, 0.0], [1.0, 1.0, 1.0], [2.0, 2.0, 2.0]]

        std = (2.0/3.0)**0.5
        result4 = np.asarray([[-1.0/std, -1.0/std, -1.0/std], [0.0, 0.0, 0.0], [1.0/std, 1.0/std, 1.0/std]])
        result5 = np.asarray([[-1.0/std, -1.0/std, -1.0/std], [0.0, 0.0, 0.0], [1.0/std, 1.0/std, 1.0/std]])

        matrices_results = [(matrix4, result4), (matrix5, result5)]
       
        for (matrix, result) in matrices_results: 
            normalized = normalize_data(matrix)
            self.assertEquals(np.ndarray, type(normalized))
            self.assertEquals(normalized.all(), result.all()) 

if __name__ == '__main__':
    unittest.main()
