from typing import (
    Any)

class Task:
    def __init__(self, taskname):
        self.taskname = taskname

    def __str__(self):
        return f"{self.taskname})"

    def setup(self):
        pass
    def do(self,*args:Any, **kwargs: Any):
        pass

    # ThisTask(params) or ThisTask.do(params)
    def __call__(self,*args:Any, **kwargs: Any):
        self.do(*args, **kwargs)


