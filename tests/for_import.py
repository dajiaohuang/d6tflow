import d6tflow
import pandas as pd
import pathlib

# Vars
df = pd.DataFrame({'a': range(10)})
cfg_write_dir = pathlib.Path('tests')
cfg_write_filename_tasks = 'tasks_export.py'

class Task1A0(d6tflow.tasks.TaskCache):
    pass

class Task1A(Task1A0):
    persist=['df','df2']
    # path = "usr/test/ok.com"
    idx=d6tflow.IntParameter(default=1)
    idx2=d6tflow.Parameter(default='test')
    def run(self):
        self.save({'df': df, 'df2': df})

class Task1B(d6tflow.tasks.TaskCache):
    persist=['df','df2']
    idx3=d6tflow.Parameter(default='test3')
    def run(self):
        self.save({'df': df,'df2': df})

class Task1C(d6tflow.tasks.TaskCache):
    persist=['df','df2']
    idx3=d6tflow.Parameter(default='test3')
    export = False
    def run(self):
        self.save({'df': df,'df2': df})

@d6tflow.requires(Task1A, Task1B, Task1C)
class Task1All(d6tflow.tasks.TaskCache):
    task_group = "upwork"
    def run(self):
        self.save(df)

# Tests
def test_task():
    flow = d6tflow.Workflow(Task1All, path='data/data2/wf_change', env='prod')
    e = d6tflow.FlowExport(flows=Task1All())
    e.generate()

