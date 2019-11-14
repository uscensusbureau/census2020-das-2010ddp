from das_framework.driver import AbstractDASErrorMetrics


class ErrorMetricsStub(AbstractDASErrorMetrics):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run(self, engine_tuple):
        """
        As a placeholder/stub, this does nothing.
        """
        return None

    
