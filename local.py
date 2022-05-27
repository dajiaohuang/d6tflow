import d6tflow
import pandas as pd
import pathlib
import luigi

# Vars
df = pd.DataFrame({'a': range(10)})
cfg_write_dir = pathlib.Path('tests')
cfg_write_filename_tasks = 'tasks_d6tpipe.py'

class Task1A0(d6tflow.tasks.TaskCache):
    pass

class Task1A(Task1A0):
    persist=['df','df2']
    # path = "usr/test/ok.com"
    idx=luigi.IntParameter(default=1)
    idx2=luigi.Parameter(default='test')
    def run(self):
        self.save({'df': self.df, 'df2': self.df})

class Task1B(d6tflow.tasks.TaskCache):
    persist=['df','df2']
    idx3=luigi.Parameter(default='test3')
    def run(self):
        self.save({'df': self.df,'df2': self.df})

class Task1C(d6tflow.tasks.TaskCache):
    persist=['df','df2']
    idx3=luigi.Parameter(default='test3')
    export = False
    def run(self):
        self.save({'df': self.df,'df2': self.df})

@d6tflow.requires(Task1A, Task1B, Task1C)
class Task1All(d6tflow.tasks.TaskCache):
    pipename = ['d6tpipe_pipe1']
    path = "usr/test/ok.com"
    def run(self):
        self.save(self.df)

# Tests
def test_task():

    e = d6tflow.FlowExport(task_path_base="/test/ok/", tasks=[Task1A(), Task1All()])
    e.generate()

    code = '''
import d6tflow
import luigi
import datetime

class Task1All(d6tflow.tasks.TaskCache):
    external=True
    persist=['data']
    idx=luigi.parameter.IntParameter(default=1)
    idx2=luigi.parameter.Parameter(default='test')
    idx3=luigi.parameter.Parameter(default='test3')

'''

test_task()
