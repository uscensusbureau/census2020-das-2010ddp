"""
The function random_geometric converts a uniformly [0, 1) distributed
random variable U to a geometrically dictributed with parameter p

As per numpy C implementation
https://github.com/numpy/numpy/blob/master/numpy/random/src/distributions/distributions.c#L1079
"""
from math import ceil, log

def random_geometric_search(U: float, p: float) -> float:
    """ Aux function for random_geometric"""

    X = 1
    sum = prod = p
    q = 1.0 - p
    while (U > sum):
        prod *= q
        sum += prod
        X += 1

    return X


def random_geometric_inversion(U: float, p: float) -> float:
    """ Aux function for random_geometric"""
    ans = ceil(log(1.0 - U) / log(1.0 - p))
    return ans


def random_geometric(U: float, p: float) -> float:
    """
    Converts a uniformly [0, 1) distributed
    random variable U to a geometrically distributed variable k with parameter p:
    f(k) = (1 - p)^{k - 1} p
    :param U: uniformly [0, 1) distributed random variable
    :param p: parameter of the geometric distribution
    :return: random variable distributed as ~f(k)= (1 - p)^{k - 1} p
    """
    if (p >= 0.333333333333333333333333):
        return random_geometric_search(U, p)

    return random_geometric_inversion(U, p)
