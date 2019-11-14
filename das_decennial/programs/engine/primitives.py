"""
    This module contains primitive operations used in differential privacy.

"""
import numpy as np
import scipy.stats

from programs.engine.rngs import StillsonRDRandRNG, StillsonRDRandRNGPython, NumPyRNG, MKLRandom


class DPMechanism:
    """
    To check type in other parts of the code, use as dummy for node aggregation
     and to possibly put here some of the common things if any
    """
    protected_answer: np.ndarray
    epsilon: float
    variance: float
    sensitivity: float

    def __repr__(self):
        return "Dummy DP Mechanism"


class GeometricMechanism(DPMechanism):
    """
    Implementation of the Geometric Mechanism for differential privacy
    Adding noise from the (two-sided) Geometric distribution, with p.d.f (of integer random variable k):

                      p          -│k│       1 - α    -│k│
                   ──────── (1 - p)     or  ──────── α
                    2 - p                   1 + α

    where p is the parameter, or α = 1 - p in different conventional notation

    """
    # rng = StillsonRDRandRNG
    #rng = StillsonRDRandRNGPython
    rng = MKLRandom
    #rng = NumPyRNG

    def __init__(self, epsilon: float, sensitivity: float, true_answer: np.ndarray):
        """

        :param epsilon (float): the privacy budget to use
        :param sensitivity (int): the sensitivity of the query. Addition or deletion of one
                                   person from the database must change the query answer vector
                                   by an integer amount
        :param true_answer (float or numpy array): the true answer
        :param prng: a numpy random number generator

        Output:
            The mechanism object
            The result of the geometric mechanism (x-y + true_answer).

        numpy.random.geometric draws from p.d.f. f(k) = (1 - p)^{k - 1} p   (k=1,2,3...)

                                                        p          -│k│
        The difference of two draws has the p.d.f    ──────── (1 - p)
                                                      2 - p
        """
        # Parameter p of the Geometric distribution as per class docstring
        self.p = 1 - np.exp(-epsilon / float(sensitivity))

        # Save for __repr__ and other reporting
        self.epsilon = epsilon
        self.sensitivity = sensitivity

        # Variance is twice that of the one-sided (1-p)/p^2
        self.variance = 2 * (1 - self.p) / self.p**2

        shape = np.shape(true_answer)
        # TODO: Implement CSPRNG
        rng = self.rng()
        x = rng.geometric(self.p, size=shape) - 1  # numpy geometrics start with 1
        y = rng.geometric(self.p, size=shape) - 1

        self.protected_answer = x - y + true_answer

    def __repr__(self):
        return f"Geometric(eps={self.epsilon}, sens={self.sensitivity}, p={self.p})"

    def pmf(self, x, location=0):
        """
        f(x) = p/(2-p) (1-p)^(abs(x))
        """
        centered_x = np.abs(x-location)
        centered_int_x = int(centered_x)
        if np.abs(centered_x - centered_int_x) > 1e-5:
            return 0

        return (self.p / (2 - self.p)) * ((1 - self.p)**centered_int_x)

    def inverseCDF(self, quantile, location=0.0):
        """
        To be used to create CIs
        quantile: float between 0 and 1
        location: center of distribution

        for all geometric points {0, ... inf} the pdf for the two-sided
        geometric distribution is 1/(2-p) times the pdf for the one-sided
        geometric distribution wiht the same parameterization. Therefore,
        the area in the rhs tail (>0) for the two-sided cdf for a given
        x value should be 1/(2-p) that of the one-sided geometric.

        Say we want the x s.t P(X<=x) = 0.9 for the two-sided geometric.  Then if we find
        the P(X<=x) = 1 - (0.1*(2-p)), the x value should be the same.

        Because this is a discrete distribution, cdf(quantile) will give
        the smallest x s.t P(X>=x) >= quantile and P(P>=x-1) < quantile
        """
        tail_prob = 1.0 - quantile if quantile > 0.5 else quantile
        one_sided_geom_tail_prob = tail_prob * (2 - self.p)

        answer = scipy.stats.geom.ppf(q=1-one_sided_geom_tail_prob, p=self.p, loc=-1)  # b/c we want to start at 1 not 0
        answer = location-answer+1 if quantile < 0.5 else location+answer+1

        return answer


class LaplaceMechanism(DPMechanism):
    """
    Implements Laplace mechanism for differential privacy.
    Adding noise from the Laplace distribution, with p.d.f

                          1      -│x-a│
                        ───── exp ───────,
                         2b         b
    where a is known as the location, and b as the scale of Laplace distribution
    """

    def __init__(self, epsilon: float, sensitivity: float, true_answer: np.ndarray, prng: np.random.RandomState):
        """

        :param epsilon (float): the privacy budget to use
        :param sensitivity (float): the sensitivity of the query
        :param true_answer: true_answer (float or numpy array): the true answer
        :param prng: a numpy random number generator

         Add noise according to Laplace mechanism

        """
        self.epsilon = epsilon

        # Scale of the Laplace distribution, denoted as b in the docstring above
        self.scale = float(sensitivity) / self.epsilon

        # Save for __repr__ and other reporting
        self.sensitivity = sensitivity

        # Variance is 2b^2
        self.variance = 2 * (self.scale ** 2)

        shape = np.shape(true_answer)

        # TODO: Implement CSPRNG and floating point
        self.protected_answer = prng.laplace(loc=true_answer, scale=self.scale, size=shape)

    def __repr__(self):
        return f"Laplace(eps={self.epsilon}, sens={self.sensitivity})"

    def inverseCDF(self, quantile, location):
        """
        To be used to create CIs
        quantile: float between 0 and 1
        location: center of distribution
        """
        return scipy.stats.laplace.ppf(q=quantile, loc=location, scale=self.scale)


def basic_dp_answer(*, true_data, query, epsilon: float, bounded_dp=True):
    sensitivity = query.unboundedDPSensitivity() * 2.0 if bounded_dp else 1.0
    if query.isIntegerQuery():
        mycls = GeometricMechanism
    else:
        mycls = LaplaceMechanism
    return mycls(epsilon=epsilon, sensitivity=sensitivity, true_answer=query.answer(true_data))


class GaussianzCDPMechanism:
    """
    Implements Gaussian noise infusion for differential(-like?) privacy, known
    as zero-concentrated differential privacy (zCDP)

     Mark Bun, Thomas Steinke, Concentrated Differential Privacy: Simplifications,
     Extensions, and Lower Bounds, 2016, https://arxiv.org/abs/1605.02065

    Adding noise from the centered Gaussian distribution, with p.d.f

                                              2
                               1            x
                         ─────────── exp ───────
                            ____              2
                           /   2           2σ
                          √2πσ

    where σ = l2_sensitivity / sqrt( 2 * rho)
    """
    def __init__(self, rho, l2_sensitivity, true_answer, prng):
        """

        :param rho:
        :param l2_sensitivity:
        :param true_answer:
        :param prng:
        """

        self.rho = rho
        self.sigma = l2_sensitivity / np.sqrt(2. * self.rho)
        self.variance = self.sigma ** 2

        # For compatibility
        self.epsilon = np.inf  # Or maybe None is better used, that will be seen

        # Save for __repr__ and other reporting
        self.l2_sensitivity = l2_sensitivity

        shape = np.shape(true_answer)
        # TODO: implement CSPRNG
        self.protected_answer = prng.normal(loc=0., scale=self.sigma, size=shape)

    def __repr__(self):
        return f"zCPDGaussian(rho={self.rho}, sigma={self.sigma}, l2sen={self.l2_sensitivity})"
