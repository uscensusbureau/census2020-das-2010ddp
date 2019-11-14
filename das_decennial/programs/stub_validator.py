#stub program
from das_framework.driver import AbstractDASValidator

class validator(AbstractDASValidator):

    def __init(self, **kwargs):
        super().__init__(**kwargs)

    def validate(self, original_data, written_data_reference, **kwargs):
        return True
