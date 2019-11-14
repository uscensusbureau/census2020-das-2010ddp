import scipy.sparse as ss
import numpy as np

class multiSparse:
    """
    This class is used to store a multi-dimensional numpy array as a sparse array and transform it back to a dense array when needed.
    """
    
    def __init__(self, array, shape=None):
        """
        constructor for class multiSparse
        
        Inputs:
            array: some kind of array or matrix (see Notes for more information)
            shape: (optional) a tuple of positive integers that describes the levels of each dimension
                   of the histogram
            
        Outputs:
            creates a multiSparse object
        
        Notes:
            There are two ways to build a multiSparse object
            1. Using a numpy ndarray
               e.g. arr = np.zeros((2,3))
                    sparse_arr = multiSparse(arr)
            
            2. Using a csr_matrix and a shape tuple
               e.g. mat = ss.csr_matrix(np.zeros((6,)))
                    shape = (2,3)
                    sparse_arr = multiSparse(mat, shape)
        """
        if isinstance(array, np.ndarray): # a numpy ndarray is expected to initialize multiSparse
            self.sparse_array = ss.csr_matrix(array.flatten())
            self.shape = array.shape
        elif isinstance(array, ss.csr_matrix) and shape is not None:
            self.sparse_array = array
            self.shape = shape
        else:
            raise TypeError("array must be of class numpy.ndarray or of type ss.csr_matrix, along with the shape")
    
    def toDense(self):
        return self.sparse_array.toarray().reshape(self.shape)
    
    def __add__(self, b):
        assert self.shape == b.shape
        tmp = multiSparse(np.array([0]))
        tmp.shape = self.shape
        tmp.sparse_array = self.sparse_array + b.sparse_array
        return tmp
    
    def sum(self, dims = None):
        if dims:
            out = self.toDense().sum(dims)
        else:
            out = np.array(self.sparse_array.sum())
        return out
        
    def __sub__(self, b):
        assert self.shape == b.shape
        tmp = multiSparse(np.array([0]))
        tmp.shape = self.shape
        tmp.sparse_array = self.sparse_array - b.sparse_array
        return tmp

    def __eq__(self, other):
        assert self.shape == other.shape
        if np.issubdtype(self.sparse_array.dtype, float):
            return np.isclose(np.array(self.sparse_array.todense()), np.array(other.sparse_array.todense()), equal_nan=True).all()
        else:
            return not (self.sparse_array != other.sparse_array).todense().any()
    
    def abs(self):
        self.sparse_array.data = np.abs(self.sparse_array.data)
        return self
    
    def sqrt(self):
        self.sparse_array = self.sparse_array.sqrt()
        return self
    
    def square(self):
        self.sparse_array.data = np.square(self.sparse_array.data)
        return self
    
    def max(self):
        self.sparse_array = self.sparse_array.max()
        return self
