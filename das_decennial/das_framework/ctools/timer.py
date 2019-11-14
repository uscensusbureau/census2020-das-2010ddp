import time


class Timer:    
    def __init__(self,message='Elapsed time: '):
        if '%' in message:
            self.message = message
        else:
            self.message = message + " %f secondspy"

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
        if self.message:
            print(self.message % self.interval)
