"""
Module implementing RNGs (primarily those, based on hardware, i.e. using RDRAND instruction)
and wrapping them into an interface identical to numpy.random for calling distributions
"""
from typing import Tuple
try:
    import rdrand  # It's not on all clusters yet
except ImportError:
    pass
import numpy as np
try:
    import programs.engine.unif2geometric as unif2geometric  # Need to figure out how to ship compiled Cython lib to spark
except ImportError:
    pass
try:
    import mkl_random  # It's not on all clusters yet
except ImportError:
    pass

import programs.engine.unif2geometricpython


class StillsonRDRandRNG:

    #u2g = unif2geometric

    def __init__(self):
        self.rng = rdrand.RdSeedom()

    def geometric(self, p: float, size: Tuple[int, ...]) -> np.ndarray:
        n = int(np.prod(size))
        return np.array([self.u2g.random_geometric(self.rng.random(), p) for _ in range(n)]).astype(int).reshape(size)


class StillsonRDRandRNGPython(StillsonRDRandRNG):

    u2g = programs.engine.unif2geometricpython


class MKLRandom:
    def __init__(self):
        self.rng = mkl_random.RandomState(brng='nondeterm')
        if self.rng.get_state()[0] != 'NON_DETERMINISTIC':
            raise RuntimeError('RNG returned by mkl_random is not non-deterministic RDRAND (most likely, substituted by default Mersenne Twister,  '
                               'since mkl_random installation on one of the nodes is of the version which does not yet support RDRAND)')

    def geometric(self, p: float, size: Tuple[int, ...]) -> np.ndarray:
        return self.rng.geometric(p, size)


class NumPyRNG:
    def __init__(self):
        self.rng = np.random.RandomState()

    def geometric(self, p: float, size: Tuple[int, ...]) -> np.ndarray:
        return self.rng.geometric(p, size)