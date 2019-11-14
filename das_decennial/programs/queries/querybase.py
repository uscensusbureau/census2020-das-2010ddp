"""This module contains classes for the representation of linear queries.

Linear queries should subclass AbstractLinearQueries. Use this module with
the following design principles:

1) Subclasses of AbstractLinearQuery should be simple. The only functionality
    they should provide is a more efficient version of some base class functions
2) Classes should not be instantiated directly. Instead, the QueryFactory should
    provide functions for instantiating the classes. Any complicated construction
    code should be moved from the class constructors and into a QueryFactory function

"""

import functools
from typing import List, Union
from abc import ABCMeta, abstractmethod
import numpy as np
import scipy.sparse as ss


class AbstractLinearQuery(metaclass=ABCMeta):
    """ This class describes the api for queries

    There are 3 fields:
        self.name: the name of the query. Do not use
            directly. Call getName()
        self.matrix_rep: either none or a cached
           copy of the matrix represntation. Do not
           use directly. Call matrixRep()
        self._query_sensitivity: a cached sensitivity, do not use directly
    """

    def __init__(self, name):
        self.matrix_rep = None
        self.name = name
        self._query_unbounded_sensitivity = None

    def getName(self):
        """ Returns the name of the query. """
        return self.name

    def answer(self, data):
        """ Returns the query answer over data

        Inputs:
            data: a multidimensional numpy array
        Output:
            a vector (not multi-d array) of answers
        Note: the default implementation might not be efficient
        """
        mydata = data.reshape(data.size)
        return self.matrixRep() * mydata

    @abstractmethod
    def kronFactors(self):
        """ Returns a list of matrices whose kron product is the matrix representation """
        pass

    def matrixRep(self) -> ss.csr_matrix:
        """ returns a cached version of the matrix representation if available """
        if self.matrix_rep is None:
            self.matrix_rep = self._matrixRep()
        return self.matrix_rep

    def _matrixRep(self) -> ss.csr_matrix:
        """ Should compute and return matrix representation of the query in csr format

        Note: when the data is flattened as a vector x, this function should return a
             matrix Q such that Qx is the answer to the query
        """
        # Need to do explicitly convert to csr_matrix for the case when there is only a single kronFactor in ndarray format
        rep = ss.csr_matrix(functools.reduce(lambda x, y: ss.kron(x, y, format="csr"), self.kronFactors()))
        # Note: ss.kron differs from np.kron because in the case of kron of 1d vectors, ss.kron
        # returns a 1xc matrix  but np.kron returns a 1d array
        return rep

    def unboundedDPSensitivity(self):
        """ returns the sensitivity of this query set in the UNBOUNDED version of DP

        The sensitivity is the maximum l1 norm of the columns in the matrix representation.
        It can be efficiently computed as the product of the sensitivities of the kron factors.
        Override this  method for query classes where this can be computed faster.
        """
        if self._query_unbounded_sensitivity is None:
            sen = 1.0
            for mat in self.kronFactors():
                # verify that the kron matrices are either matrices or arrays
                assert len(mat.shape) in [2, 1]
                if len(mat.shape) == 2:
                    minisen = np.abs(mat).sum(axis=0).max()
                else:
                    minisen = np.abs(mat).max()
                sen = sen * minisen
            self._query_unbounded_sensitivity = sen
        return self._query_unbounded_sensitivity

    def isIntegerQuery(self):
        """ override this with a fast function to determine if the query coefficients are all
        integers. This will allow us to use Geometric mechanism to answer it """
        return False

    def domainSize(self):
        """ returns the expected size (number of cells) in the data histogram.

        Note: This should equal the number of columns in the matrix representation
        The default implementation is slow. override this function
        """
        dims = [x.shape[1] if len(x.shape) == 2 else x.shape[0] for x in self.kronFactors()]
        result = np.prod(dims)
        return result

    def numAnswers(self) -> int:
        """ returns the number of cells to expect  in the answer

        Note: this should equal the number of rows in the matrix representation
        The default implementation is slow. Override this function
        """
        result: int = np.prod([x.shape[0] if len(x.shape) == 2 else 1 for x in self.kronFactors()]).astype(int)
        return result

    def queryShape(self):
        """
        returns the multidimensional shape of the query if there is one
        """
        pass

    def answerWithShape(self, data):
        """
        returns the answer to the query with a multidimensional shape if there is one
        """
        pass

    def __repr__(self):
        output = ''
        output += "------------- Query looks like -----------------" + '\n'
        output += "Name:" + str(self.name) + '\n'
        output += "Matrix rep: " + str(self.matrix_rep) + '\n'
        output += "-----------------------------------------------------" + '\n'
        return output

    def __eq__(self, other):
        """
        Equal if matrix representation equal (for now, may be changed)
        """
        if self.matrixRep().shape != other.matrixRep().shape:
            return False
        if self.matrixRep().nnz != other.matrixRep().nnz:
            return False
        return (self.matrixRep() != other.matrixRep()).nnz == 0


class SparseKronQuery(AbstractLinearQuery):
    """ This class represents queries as a Kron product of sparse matrices """

    def __init__(self, matrices, name=""):
        """ Constructor for SparseKronQuery

        Input:
            matrices: a python list of sparse matrices (preferably in CSR format)
               The query is interpreted as the kron product of the matrices in this list
            name: an optional name for the query
        """
        super().__init__(name)
        self.matrices = matrices
        self.num_columns = np.prod(
            [x.shape[1] if len(x.shape) == 2 else x.shape[0] for x in matrices]
        ).astype(int)
        self.num_rows = np.prod([x.shape[0] if len(x.shape) == 2 else 1 for x in matrices]).astype(int)
        # check if all coefficients are integer by making sure they equal round versions
        self._has_integer_coeffs = all([np.abs(m-np.round(m)).sum() == 0 for m in self.matrices])

    def kronFactors(self):
        return self.matrices

    def isIntegerQuery(self):
        return self._has_integer_coeffs

    def domainSize(self):
        return self.num_columns

    def numAnswers(self):
        return self.num_rows

class InefficientCountQuery(AbstractLinearQuery):
    """
        This class represents a count/predicate query with no assumed structure, instantiated by a (long) list of explicit indices
        WARNING: inefficient! But used for quick development of public-historical-data idea
        TODO: replace this with more principled/efficient UnionOfMarginal-ishQueries class
        (publish historical data uses marginal queries from SF1 2000, w/ some final rows dropped)
    """
    
    def __init__(self, array_dims, multiindices, name=""):
        """
            This initializes the necessary matrices/shape parameters.

            Inputs:
                array_dims: the natural dimensions of the data as a multidimensional histogram
                multiindices: list of multiindices indicating locations in array_dims w/ coefficient of 1.0
                
        """
        print("WARNING: instantiating inefficient predicate query. Use sparingly!")
        super().__init__(name)
        self.array_dims = tuple(array_dims)
        self.multiindices = multiindices

    def domainSize(self):
        return np.prod(self.array_dims).astype(int) # InefficientCountQ is inefficient b/c it works on the whole histogram directly! 

    def numAnswers(self):
        return 2 # Purpose of InefficientCountQ is to form a single inefficient scalar-valued query over the whole histogram (and its complement; since we assume boundedDP sensitivity of 2 anyway, there is no benefit to asking just 1 query and not its complement)

    def _matrixRep(self) -> ss.csr_matrix:
        """ Should compute and return matrix representation of the query in csr format

        Note: when the data is flattened as a vector x, this function should return a
             matrix Q such that Qx is the answer to the query
        """
        # Add code to instantiate csr matrix rep from explicit list of multiindices indicating locations of 1's
        tempNdarray = np.zeros(shape=(2,)+self.array_dims, dtype=np.bool)
        for multiindex in self.multiindices:
            tempNdarray[(0,) + multiindex] = 1
        tempNdarray = tempNdarray.reshape((2,self.domainSize()))
        tempNdarray[1,:] = 1. - tempNdarray[0,:]
        rep = ss.csr_matrix(tempNdarray)
        return rep

    def isIntegerQuery(self):
        return True

class SumoverQuery(AbstractLinearQuery):
    """ This class provides a more efficient way of answering subset-marginal queries """

    def __init__(self, array_dims, subset=(), add_over_margins=(), name=""):
        """
            This initializes the necessary matrices/arrays.

            Inputs:
                array_dims: the natural dimensions of the data as a multidimensional histogram
                subset: a tuple of lists or iterators (e.g., range() objects)  that should match
                    the number of array dimensions that represent what subset of each dimension
                    we should use to construct the queries e.g ( [0] , range(0,2,1), [1,52,77],
                    range(0,2,1), range(0,63,1), range(0,52,1)).
                add_over_margins: the list of dimensions we will be summing over. None or empty
                    list or empty tuple imply no summation
        """
        super().__init__(name)
        self.array_dims = tuple(array_dims)
        if subset in (None, (), [], False):
            self.subset = np.ix_(*(range(self.array_dims[x]) for x in range(len(self.array_dims))))
            self.subset_input = tuple(
                range(self.array_dims[x]) for x in range(len(self.array_dims))
            )
        else:
            self.subset = np.ix_(*tuple(subset))
            self.subset_input = tuple(subset)
        if add_over_margins in ([], None):
            self.add_over_margins = ()
        else:
            self.add_over_margins = tuple(add_over_margins)
        self.num_columns = np.prod(self.array_dims).astype(int)
        self.num_rows = np.prod(
            [len(x) for (i, x) in enumerate(self.subset_input) if i not in self.add_over_margins]
        ).astype(int)

    def domainSize(self):
        return self.num_columns

    def numAnswers(self):
        return self.num_rows

    def kronFactors(self) -> List[Union[ss.csr_matrix, np.ndarray]]:
        """
            This computes the Kronecker representation of the query.
        """
        # matrices = [None] * len(self.array_dims)
        matrices = []
        for i, dim in enumerate(self.array_dims):
            if i in self.add_over_margins:
                x = np.zeros(dim, dtype=bool)
                x[self.subset_input[i]] = True
            else:
                full = ss.identity(dim, dtype=bool, format="csr")
                x = full[self.subset_input[i], :]
            # matrices[i] = x
            matrices.append(x)
        return matrices

    def answer(self, data):
        dsize = self.domainSize()
        assert data.size == dsize, "Data does not match except cell count of {}".format(dsize)
        return data[self.subset].sum(axis=self.add_over_margins, keepdims=False).flatten()

    def isIntegerQuery(self):
        return True


class SumOverGroupedQuery(AbstractLinearQuery):
    """ This class provides a more efficient way of answering grouped/collapsed-marginal queries """

    def __init__(self, array_dims, groupings=None, add_over_margins=(), name=""):
        """
            This initializes the necessary matrices/arrays.

            Inputs:
                array_dims: the natural dimensions of the data as a multidimensional histogram

                groupings: this replaces the functionality of subsetting and additionally allows
                            to collapse levels of a dimension into one or more groupings.
                            The argument "groupings" is a dictionary in which the keys are the
                            dimensions (or axes) over which levels should be grouped and
                            the values are each a list containing 1 or more possible groupings
                            which themselves are represented as a list.  For example:

                            groupings = {
                            1: [[1],],
                            3: [range(0, 6), range(6, 21), range(21, 41),
                                range(41, 56), range(56, 62), range(62,63)]
                            }

                            Assume array_dims in this case is (8,2,2,63). This groupings argument
                            would take dimension/axis 1 and form a grouping with single level (1),
                            which is equivalent to subseting this dimension. In the dimension/axis
                            3 it will create 6 groupings that collapse the 63 levels into the
                            corresponding 6 groups by the range arguments listed.

                add_over_margins: the list of dimensions we will be summing over. None or empty
                    list or empty tuple imply no summation
        """
        super().__init__(name)
        self.array_dims = tuple(array_dims)
        self.groupings = groupings if isinstance(groupings, dict) else {}
        self.collapse_axes = list(self.groupings.keys())
        if add_over_margins in ([], None):
            self.add_over_margins = ()
        else:
            self.add_over_margins = tuple(add_over_margins)
        self.num_columns = np.prod(self.array_dims).astype(int)
        nun_rows_temp = []
        for i in range(len(self.array_dims)):
            if i in self.add_over_margins:
                x = 1
            elif i in self.collapse_axes:
                x = len(self.groupings[i])
            else:
                x = self.array_dims[i]
            nun_rows_temp.append(x)
        self.num_rows = np.prod(nun_rows_temp).astype(int)

    def domainSize(self):
        return self.num_columns

    def numAnswers(self):
        return self.num_rows

    def kronFactors(self) -> List[Union[ss.csr_matrix, np.ndarray]]:
        """
            This computes the Kronecker representation of the query.
        """
        # matrices = [None] * len(self.array_dims)
        matrices = []
        for i, dim in enumerate(self.array_dims):
            if i in self.add_over_margins:
                x = np.ones((1, dim), dtype=bool)
            elif i in self.collapse_axes:
                n_groups = len(self.groupings[i])
                x = np.zeros((n_groups, dim), dtype=bool)
                for j in range(n_groups):
                    subset = self.groupings[i][j]
                    x[j, subset] = True
            else:
                x = np.eye(dim, dtype=bool)
            # matrices[i] = x
            matrices.append(x)
        return matrices

    def collapseLevels(self, data):
        """
        This function is used to iterively collapse a single axis at a time
        in order to compute the answer to a query

        inputs:
                data: a numpy multi-array
        outputs:
                a numpy multi-array
        """
        # if there is no collapse_axes, just return the array
        # result = None
        if not self.collapse_axes:
            result = data
        else:
            x = data
            # iteratively call the collapse function for each grouping
            for i in self.collapse_axes:
                x = self.collapse(x, i, self.groupings[i])
            result = x
        return result

    @staticmethod
    def collapse(array, axis=None, groups=None):
        """
        This function collapses (adds over) groups of levels in a
        particular dimension of a multi-array

        Inputs:
            array: a numpy multiarray
            axis: the dimension/axis to be grouped (int)
            groups: a list of lists pertaining to the levles to be grouped together
        Output:
            a numpy multiarray
        """
        # result = None
        if axis in (None, []) or groups in (None, []):
            assert False, "axis {} {}".format(axis, groups)
            # result = array
        else:
            array_dims = array.shape
            n_groups = len(groups)
            # what are all the levels for the particular dimension
            subset = [range(x) for x in array_dims]
            # take a subset for each grouped
            # and append them to a list
            mylist = []
            for i in range(n_groups):
                subset[axis] = groups[i]
                mylist.append(array[np.ix_(*tuple(subset))].sum(axis=axis, keepdims=False))
            # finally stack the sub-arrays
            out = np.stack(mylist, axis=axis)
            result = out
        return result

    def answer(self, data):
        dsize = self.domainSize()
        assert data.size == dsize, "Data does not match except cell count of {}".format(dsize)
        return self.collapseLevels(data).sum(axis=self.add_over_margins, keepdims=False).flatten()

    def queryShape(self):
        """
        returns the multidimensional shape of the query if there is one
        """
        dim_sizes = [None] * len(self.array_dims)
        for i in range(len(self.array_dims)):
            if i in self.add_over_margins:
                x = 1
            elif i in self.collapse_axes:
                x = len(self.groupings[i])
            else:
                x = self.array_dims[i]
            dim_sizes[i] = x
        return dim_sizes

    def answerWithShape(self, data):
        """
        returns the answer to the query with a multidimensional shape if there is one
        """
        dsize = self.domainSize()
        assert data.size == dsize, "Data does not match except cell count of {}".format(dsize)
        return self.collapseLevels(data).sum(axis=self.add_over_margins, keepdims=False)

    def isIntegerQuery(self):
        return True

    def __repr__(self):
        output = ''
        output += "------------- query looks like -----------------" + '\n'
        output += "name: " + str(self.name) + '\n'
        output += "array_dims: " + str(self.array_dims) + '\n'
        output += "groupings: " + str(self.groupings) + '\n'
        output += "add_over_margins: " + str(self.add_over_margins) + '\n'
        output += "-----------------------------------------------------" + '\n'
        return output

    def answerSparse(self, sparse_data):
        """
        answers the query on data in sparse matrix form

        Inputs:
            sparse_data: a scipy.sparse sparse matrix

        Outputs:
            a sparse matrix - the answer to the query

        Notes:
            when working with a multiSparse object, use the sparse_array attribute as an input to this function.
            if the dimensions are incompatible, try using sparse_array.transpose() as input instead.

            the * operator is overloaded in the __mul__ function in scipy.sparse's base.py module, where
            the spmatrix class is defined... When two sparse matrices are being multiplied, the overloaded
            * operator computes matrix multiplication, which is what we want.

            https://www.programiz.com/python-programming/operator-overloading
            https://github.com/scipy/scipy/blob/v1.1.0/scipy/sparse/base.py#L452
            https://github.com/scipy/scipy/blob/v1.1.0/scipy/sparse/compressed.py#L448
        """
        A = self.matrixRep()
        m, k1 = A.shape

        B = sparse_data
        k2, n = B.shape

        assert k1 == k2, f"Dimensions don't match. {m} x {k1} (Query) and {k2} x {n} (Data) are incompatible."

        ans = A * B
        return ans


class StackedQuery(AbstractLinearQuery):
    """ This class stacks queries together """

    def __init__(self, queries, domain_subset=None, name=""):
        """ Constructs a stacked query

        Inputs:
            queries: a python list of queries with the same domain size
            domain_subset: a list of indices within the range of domain size
               or None if we want to keep the original domain
            name: an optional name for the query

        Note: this creates a stacked version of the queries over the specified
            Suppose we are stacking two queries with matrix representations A
            and B with dimensons nxd and mxd respectively. Then the new query
            has an (n+m)xd  matrix representation where the first n rows come
            from A then the next m come from B. If domain_subset is specified,
            it reduces the number of columns. For example if domain_subset=[1,3]
            then the matrix representation becomes (n+m)x2 and the top nx2 part of
            the matrix consists of A[:, [1,3]] (i.e. columns 1 and 3) and the bottom
            part consists of B[:, [1,3]]
        """
        super().__init__(name)
        if domain_subset is None:
            self.matrix_rep = ss.vstack([q.matrixRep() for q in queries], format="csr")
        else:
            self.matrix_rep = ss.vstack(
                [x.matrixRep()[:, domain_subset] for x in queries],
                format="csr")
        self._has_integer_coeffs = all([q.isIntegerQuery() for q in queries])

    def isIntegerQuery(self):
        return self._has_integer_coeffs

    def kronFactors(self) -> List[ss.csr_matrix]:
        return [self.matrix_rep]


class ComboQuery(AbstractLinearQuery):
    """ Creates a linear combination of existing queries

    queries = list of query objects
    linear_combo = list of linear coefficients
    name = name

    eg queries = [q1, q2, q3]
       linear_combo = [2, -1, 3]
    will form the matrix representation of 2*q1 - q2 + 3*q3 """

    def __init__(self, queries, linear_combo=None, name=""):
        super().__init__(name)
        self.matrix_rep = sum([q.matrixRep() * wgt for q, wgt in zip(queries, linear_combo)])
        are_queries_integers = all([q.isIntegerQuery() for q in queries])
        are_combo_integers = all([np.abs(c-np.round(c)) == 0 for c in linear_combo])
        self._has_integer_coeffs = are_queries_integers and are_combo_integers

    def kronFactors(self) -> List[ss.csr_matrix]:
        return [self.matrix_rep]

    def isIntegerQuery(self):
        return self._has_integer_coeffs

class QueryFactory:
    """Class for instantiaing queries.

    Subclasses of AbstractLinearQuery should be constructed only by functions in this class.
    The job of the QueryFactory is to decide which class to use to represent a desired query

    """

    @staticmethod
    def makeKronQuery(kron_factors, name=""):
        """ Create a kron query """
        query = SparseKronQuery(kron_factors, name)
        return query

    @staticmethod
    def makeTabularQuery(array_dims, subset=(), add_over_margins=(), name=""):
        """ Creates a linear marginal query """
        query = SumoverQuery(array_dims, subset, add_over_margins, name)
        return query

    @staticmethod
    def makeTabularGroupQuery(array_dims, groupings=None, add_over_margins=(), name=""):
        """ Creates a linear query """
        myarray_dims = tuple(array_dims)
        for x in myarray_dims:
            assert isinstance(x, int), "array_dims {} must be ints".format(myarray_dims)
        if add_over_margins in ([], None):
            myadd_over_margins = ()
        else:
            for x in add_over_margins:
                # check that x is int and in range
                assert x in range(len(array_dims)), \
                    f"{x} in add_over_margins {add_over_margins} is not within histogram array dimensions number: from 0 to {len(array_dims)}"
            # check that we don't get any duplicate dims
            assert len(add_over_margins) == len(set(add_over_margins)), (
                "add_over_margins {} has duplicate dims").format(add_over_margins)
            # passed checks, set myadd_over_margins
            myadd_over_margins = tuple(add_over_margins)
        if groupings in (None, (), [], False, {}):
            query = SumOverGroupedQuery(array_dims=myarray_dims,
                                        add_over_margins=myadd_over_margins, name=name)
        else:
            assert isinstance(groupings, dict), "groupings must be a dictionary"
            keys = list(groupings.keys())
            for x in keys:
                assert isinstance(x, int), "groupings keys must be integers"
            # check that the intersection is empty
            assert not set(keys).intersection(set(myadd_over_margins)), (
                "add_over_margins {} and groupings {} overlap").format(myadd_over_margins, keys)
            # if so set groupings
            mygroupings = groupings
            query = SumOverGroupedQuery(array_dims=myarray_dims, groupings=mygroupings,
                                        add_over_margins=myadd_over_margins, name=name)
        return query

    @staticmethod
    def makeComboQuery(queries, linear_combo=None, name=""):
        """ Creates a query which is a linear combination of existing"""
        # for q in queries:
        #    assert(isinstance(q, SumOverGroupedQuery))
        if linear_combo in [None, []]:
            linear_combo = [1] * len(queries)
        query = ComboQuery(queries, linear_combo=linear_combo, name=name)
        return query

    @staticmethod
    def makeInefficientCountQuery(array_dims, multiindices, name=""): 
        """
            Creates query with 1/0 coefficients over full histogram/data vector
            This query type should be used sparingly; a few of them is fine, but hundreds or
            thousands (or more!) of them would quickly create RAM/CPU problems for data products
            with large histograms (like DHC-H/DHC-P)
        """
        query = InefficientCountQuery(array_dims, multiindices, name)
        return query
