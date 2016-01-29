import unittest

from app import feature_extraction
from app import normalize_data
from app import get_eps
from app import get_anomaly_days

import math
import numpy as np
from datetime import datetime

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

    def test_feature_extraction(self):
        dataset = {
            'key' : [1]
        }
        features = feature_extraction(dataset)
        self.assertEqual(1, len(features))
        self.assertEqual(1, len(features[0]))
        self.assertEqual([1] , features[0])

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

class TestGetEps(unittest.TestCase):
    
    def test_fail_empty(self):
        normalized = np.asarray([])
        eps = get_eps(normalized)
        self.assertEquals(-1.0, eps)

    def test_fail_single(self):
        normalized = np.asarray([[1,1,2]])
        eps = get_eps(normalized)
        print eps
        self.assertEquals(-1.0, eps)

    def test_result(self):
        norm1 = np.asarray([[0],[1]])
        eps1 = 1.0

        norm2 = np.asarray([[0,0],[1,1]])
        eps2 = 2**0.5

        norm3 = np.asarray([[0,0],[1,1],[-1,-1]])
        eps3 = 2**0.5

        norm4 = np.asarray([[0,1],[0,1]])
        eps4 = 0.0

        norms_epses = [(norm1, eps1)]
        for (normalized, eps_true) in norms_epses:
            eps_result = get_eps(normalized)
            self.assertEquals(eps_result, eps_true)

class TestGetAnomalyDays(unittest.TestCase):

    def test_one(self):
        sorted_days = ['2016-01-01', '2016-01-02']
        labels = [0, -1]
        anomaly_days = get_anomaly_days(sorted_days, labels, 21561)
        anomaly_days_expected = [datetime.strptime('2016-01-02', '%Y-%m-%d')] 
        self.assertEquals(anomaly_days, anomaly_days_expected)  

if __name__ == '__main__':
    unittest.main()
