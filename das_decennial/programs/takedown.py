from das_framework.driver import AbstractDASTakedown
import shutil

class takedown(AbstractDASTakedown):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def takedown(self):
        shutil.rmtree(self.setup.dir4sparkzip,ignore_errors=True)
        return True

    def removeWrittenData(self, reference):
        return True
