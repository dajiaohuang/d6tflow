import pytest

import d6tflow
import sklearn, sklearn.datasets, sklearn.svm, sklearn.linear_model
import pandas as pd
import numpy as np
from pathlib import Path, PosixPath

# define workflow
class TaskGetData(d6tflow.tasks.TaskPqPandas):  # save dataframe as parquet

    def run(self):
        ds = sklearn.datasets.load_breast_cancer()
        df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_train['y'] = ds.target
        self.save(df_train) # quickly save dataframe

@d6tflow.requires(TaskGetData) # define dependency
class TaskPreprocess(d6tflow.tasks.TaskPqPandas):
    do_preprocess = d6tflow.BoolParameter(default=True) # parameter for preprocessing yes/no

    def run(self):
        df_train = self.input().load() # quickly load required data
        if self.do_preprocess:
            df_train.iloc[:,:-1] = sklearn.preprocessing.scale(df_train.iloc[:,:-1])
        self.save(df_train)

@d6tflow.requires(TaskPreprocess) # automatically pass parameters upstream
class TaskTrain(d6tflow.tasks.TaskPickle): # save output as pickle
    model = d6tflow.Parameter(default='ols') # parameter for model selection
    scale = d6tflow.BoolParameter(default=False)

    def run(self):
        df_train = self.input().load()
        if self.model=='ols':
            model = sklearn.linear_model.LogisticRegression()
        elif self.model=='svm':
            model = sklearn.svm.SVC()
        else:
            raise ValueError('invalid model selection')
        model.fit(df_train.drop('y',axis=1), df_train['y'])
        self.save(model)

class Task1(d6tflow.tasks.TaskCache):
    param1 = d6tflow.IntParameter(default=0)

    def run(self):
        self.save({'hello':self.param1})


class TestWorkflow:

    def test_single_flow_preview(self):
        flow = d6tflow.Workflow(params = {'do_preprocess': False})

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow.preview(TaskTrain)
            output = buf.getvalue()
            assert output.count('PENDING')==0
            assert output.count('COMPLETE')==3
            assert output.count("'do_preprocess': 'False'")==1


    def test_single_workflow_no_params_passes(self):
        flow = d6tflow.Workflow()
        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow.preview(TaskTrain)
            output = buf.getvalue()
            assert output.count("'do_preprocess': 'True'")==1


    def test_single_flow_parameters_passes_properly(self):
        flow1 = d6tflow.Workflow(params = {'do_preprocess': False})
        flow2 = d6tflow.Workflow(params = {'do_preprocess': True})

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow1.preview(TaskTrain)
            output = buf.getvalue()
            assert output.count("'do_preprocess': 'False'")==1

            flow2.preview(TaskTrain)
            output2 = buf.getvalue()
            assert output2.count("'do_preprocess': 'True'")==1


    def test_task_runs_properly_and_gives_outputload(self):
        flow = d6tflow.Workflow(params = {'do_preprocess': False})
        flow.preview(TaskTrain)
        flow.run(TaskTrain)
        out1 = flow.outputLoad(TaskTrain)
        out2 = flow.outputLoad(TaskPreprocess)
        ds = sklearn.datasets.load_breast_cancer()
        df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
        assert type(out1).__name__ == "LogisticRegression"
        assert np.all(out2.drop(['y'], axis=1) == df_train)


    def test_output_load_comparing_with_task_run(self):
        params = {'param1': 1}
        flow = d6tflow.Workflow(params = params, task=Task1)

        flow.run()
        assert Task1(**params).complete() == True
        assert Task1(**params).outputLoad()['hello'] == params['param1']
        assert flow.outputLoad() == Task1(**params).outputLoad()


    def test_output_load_all_comparing_with_task_run(self):
        params = {'param1': 1}
        flow = d6tflow.Workflow(params = params, task=Task1)

        flow.run()
        outputs = flow.outputLoadAll()
        assert outputs['Task1'] == Task1(**params).outputLoad()


    def test_no_default_expect_error(self):
        with pytest.raises(RuntimeError):
            flow = d6tflow.Workflow(params = {'do_preprocess': False})
            flow.run()


    def test_default_task(self):
        flow = d6tflow.Workflow({'do_preprocess': False})
        flow.set_default(TaskTrain)
        out_class = flow.get_task()
        type(out_class).__name__ == "TaskTrain"


    def test_tasks_with_dependencies_outputloadall(self):
        flow = d6tflow.Workflow(params = {'do_preprocess': False}, task=TaskTrain)
        flow.preview()
        flow.run()
        flow.run(TaskPreprocess)
        flow.outputLoad()
        flow.outputLoad(TaskPreprocess)

        # load data for all dependencies
        data = flow.outputLoadAll()
        assert 'TaskTrain' in data.keys()
        assert 'TaskPreprocess' in data.keys()
        assert 'TaskGetData' in data.keys()
        assert type(data['TaskTrain']).__name__ == "LogisticRegression"
        ds = sklearn.datasets.load_breast_cancer()
        df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
        assert np.all(data['TaskPreprocess'].drop(['y'], axis=1) == df_train)
        assert np.all(data['TaskGetData'].drop(['y'], axis=1) == df_train)


    def test_get_task(self):
        flow0 = d6tflow.Workflow(task=Task1)
        assert flow0.get_task().param_kwargs['param1'] == 0
        params = {'param1': 1}
        flow = d6tflow.Workflow(params = params, task=Task1)
        assert flow.get_task().param_kwargs['param1'] == params['param1']


class TestWorkflowMulti:

    def test_preview_single_flow(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow2.preview(tasks = TaskTrain, flow = 'experiment1')
            output = buf.getvalue()
            assert output.count('PENDING')==0
            assert output.count('COMPLETE')==3
            assert output.count("'do_preprocess': 'False'")==1


    def test_preview_all_flow(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow2.preview(flow = None, tasks = TaskTrain)
            output = buf.getvalue()
            assert output.count('PENDING')==0
            assert output.count('COMPLETE')==6
            assert output.count("'do_preprocess': 'False'")==1
            assert output.count("'do_preprocess': 'True'")==1


    def test_no_default_expect_error(self):
        with pytest.raises(RuntimeError):
            flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}})
            flow2.run()


    def test_no_exp_params_passed(self):
        with pytest.raises(Exception) as e:
            flow2 = d6tflow.WorkflowMulti()
            assert str(e.value) == "Experments not defined"


    def test_default_task(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}})
        flow2.set_default(TaskTrain)
        out_class = flow2.get_task()
        type(out_class).__name__ == "TaskTrain"


    def test_multi_exp_single_flows_single_outputload(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)
        flow2.run(flow = "experiment1")

        out = flow2.outputLoad(task=TaskTrain)
        type(out).__name__ == "LogisticRegression"


    def test_multi_exp_all_flows_outputloadall(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)
        flow2.run()

        data = flow2.outputLoadAll()
        type(data['experiment1']['TaskTrain']).__name__ == "LogisticRegression"
        type(data['experiment2']['TaskTrain']).__name__ == "LogisticRegression"


    def test_multi_get_task(self):
        params2 = {'param1': 2}
        params = {'param1': 1}
        flow2 = d6tflow.WorkflowMulti(params = {1: params, 2: params2}, task=Task1)
        assert flow2.get_task(flow=1).param_kwargs['param1'] == 1
        assert flow2.get_task(flow=2).param_kwargs['param1'] == 2


    def test_multi_params_generator(self):
        params = {'model': ['ols', 'gbm'], 'scale': [False, True]}
        flow2 = d6tflow.WorkflowMulti(params = params, task=TaskTrain)
        assert flow2.get_task(flow="model_ols_scale_False").param_kwargs["model"] == "ols"
        assert flow2.get_task(flow="model_ols_scale_False").param_kwargs["scale"] == False
        assert flow2.get_task(flow="model_ols_scale_True").param_kwargs["model"] == "ols"
        assert flow2.get_task(flow="model_ols_scale_True").param_kwargs["scale"] == True
        assert flow2.get_task(flow="model_gbm_scale_False").param_kwargs["model"] == "gbm"
        assert flow2.get_task(flow="model_gbm_scale_False").param_kwargs["scale"] == False
        assert flow2.get_task(flow="model_gbm_scale_True").param_kwargs["model"] == "gbm"
        assert flow2.get_task(flow="model_gbm_scale_True").param_kwargs["scale"] == True


    def test_multi_get_task_with_list_params(self):
        params2 = {'param1': 2}
        params = {'param1': 1}
        flow2 = d6tflow.WorkflowMulti(params = [params, params2], task=Task1)
        assert flow2.get_task(flow=0).param_kwargs['param1'] == 1
        assert flow2.get_task(flow=1).param_kwargs['param1'] == 2


    def test_output_load_comparing_with_task_run(self):
        params2 = {'param1': 2}
        params = {'param1': 1}
        flow2 = d6tflow.WorkflowMulti(params = {1: params, 2: params2}, task=Task1)

        flow2.run()
        assert flow2.outputLoad(flow = 1) == Task1(**params).outputLoad()
        assert flow2.outputLoad(flow = 2) == Task1(**params2).outputLoad()


    def test_output_load_all_comparing_with_task_run(self):
        params2 = {'param1': 2}
        params = {'param1': 1}
        flow2 = d6tflow.WorkflowMulti(params = {1: params, 2: params2}, task=Task1)

        flow2.run()
        outputs = flow2.outputLoadAll()
        assert outputs[1]['Task1'] == Task1(**params).outputLoad()
        assert outputs[2]['Task1'] == Task1(**params2).outputLoad()


# Path Test
class PathTask1(d6tflow.tasks.TaskCache):

    def run(self):
        df = pd.DataFrame({'a': range(3)})
        self.save(df)  # quickly save dataframe

@d6tflow.requires(PathTask1)
class PathTask2(d6tflow.tasks.TaskPqPandas):
    path = 'data/data2_changed'

    def run(self):
        df1 = self.inputLoad() * 2
        self.save(df1)

@d6tflow.requires(PathTask2)
class PathTask3(d6tflow.tasks.TaskPqPandas):

    def run(self):
        df2 = self.inputLoad() * 3
        self.save(df2)

class PathTaskMultiOutput(d6tflow.tasks.TaskPqPandas):
    persist = ['xx','yy']

    def run(self):
        ds = sklearn.datasets.load_boston()
        df_trainX = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_trainY = pd.DataFrame(ds.target, columns=['target'])
        self.save({'xx': df_trainX, 'yy': df_trainY}) # persist/cache training data

class TestWorkflowOutput:
    '''
    tasks.output().path is correct
    task2.outputLoad() is task1.outputLoad() * 2
    after reset no output in 'data/data2_changed/task2
    '''
    def test_workflow_df_and_path_correct(self):
        # Execute task including all its dependencies
        flow = d6tflow.Workflow(PathTask3)
        flow.run()
        
        # DF
        df_1 = flow.outputLoad(PathTask1)
        df_2 = flow.outputLoad(PathTask2)
        df_3 = flow.outputLoad(PathTask3)
        assert df_1.equals(pd.DataFrame({'a': range(3)}))
        assert df_2.equals(pd.DataFrame({'a': range(3)}) * 2)
        assert df_2.equals(df_1 * 2)
        assert df_3.equals(pd.DataFrame({'a': range(3)}) * 6)

        # Complete
        is_complete = flow.complete()
        assert is_complete

        # Path
        path_1 = flow.output(PathTask1).path
        path_2 = flow.output(PathTask2).path
        path_3 = flow.output(PathTask3).path
        assert str(path_1) == "data/PathTask1/PathTask1__99914b932b-data.cache"
        # Should be a POSIXPATH
        assert str(path_2) == "data/data2_changed/PathTask2/PathTask2__99914b932b-data.parquet"
        assert str(path_3) == "data/PathTask3/PathTask3__99914b932b-data.parquet"
        # Dir is not Empty
        p = Path('.')
        dir = list(p.glob('data/data2_changed/PathTask2/*'))
        assert len(dir) == 1
        dir = list(p.glob('data/PathTask3/*'))
        # dir = os.listdir("data/PathTask3")
        assert len(dir) == 1

        # Reset
        flow.reset(PathTask2)
        flow.reset(PathTask3)
        dir = list(p.glob('data/data2_changed/PathTask2/*'))
        assert len(dir) == 0
        dir = list(p.glob('data/PathTask3/*'))
        assert len(dir) == 0

    def test_workflow_outputPath(self):
        # Execute task including all its dependencies
        flow = d6tflow.Workflow(PathTask3)
        flow.run()

        path_1 = flow.outputPath(PathTask1)
        path_2 = flow.outputPath(PathTask2)
        assert str(path_1) == "data/PathTask1/PathTask1__99914b932b-data.cache"
        assert str(path_2) == "data/data2_changed/PathTask2/PathTask2__99914b932b-data.parquet"

        # Test Multi Output
        # Execute task including all its dependencies
        flow = d6tflow.Workflow(PathTaskMultiOutput)
        flow.run()

        multi_path_1 = flow.outputPath(PathTaskMultiOutput)
        assert str(multi_path_1['xx']) == "data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-xx.parquet"
        assert str(multi_path_1['yy']) == "data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-yy.parquet"

    def test_workflow_multi_outputPath(self):
        flow_multi = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=PathTask3)
        flow_multi.run()

        path_1 = flow_multi.outputPath(PathTask1)
        path_2 = flow_multi.outputPath(PathTask2)
        assert path_1 == {
            'experiment1': 'data/PathTask1/PathTask1__99914b932b-data.cache', 
            'experiment2': 'data/PathTask1/PathTask1__99914b932b-data.cache'
        }
        assert path_2 == {
            'experiment1': PosixPath('data/data2_changed/PathTask2/PathTask2__99914b932b-data.parquet'), 
            'experiment2': PosixPath('data/data2_changed/PathTask2/PathTask2__99914b932b-data.parquet')
        }

        # Test Multi with Multi Output
        flow_multi = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=PathTaskMultiOutput)
        flow_multi.run()

        path_1 = flow_multi.outputPath(PathTaskMultiOutput)
        assert path_1 == {
            'experiment1': 
                {
                    'xx': PosixPath('data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-xx.parquet'),
                    'yy': PosixPath('data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-yy.parquet')
                }, 
            'experiment2': 
                {
                    'xx': PosixPath('data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-xx.parquet'),
                    'yy': PosixPath('data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-yy.parquet')
                }
            }

    def test_workflow_set_path_and_env(self):
        # Execute task including all its dependencies
        flow = d6tflow.Workflow(PathTask3, env='prod')
        flow.run()

        path_2 = flow.outputPath(PathTask2)
        path_3 = flow.outputPath(PathTask3)
        assert str(path_2) == "data/env=prod/PathTask2/PathTask2__99914b932b-data.parquet"
        assert str(path_3) == "data/env=prod/PathTask3/PathTask3__99914b932b-data.parquet"
        flow.reset()

        # Execute task including all its dependencies
        flow = d6tflow.Workflow(PathTask3, path='data/data2/wf_change')
        flow.run()

        path_2 = flow.outputPath(PathTask2)
        path_3 = flow.outputPath(PathTask3)
        assert str(path_2) == "data/data2/wf_change/PathTask2/PathTask2__99914b932b-data.parquet"
        assert str(path_3) == "data/data2/wf_change/PathTask3/PathTask3__99914b932b-data.parquet"
        flow.reset()

        # Execute task including all its dependencies
        flow = d6tflow.Workflow(PathTask3, path='data/data2/wf_change', env='prod')
        flow.run()

        path_2 = flow.outputPath(PathTask2)
        path_3 = flow.outputPath(PathTask3)
        assert str(path_2) == "data/data2/wf_change/env=prod/PathTask2/PathTask2__99914b932b-data.parquet"
        assert str(path_3) == "data/data2/wf_change/env=prod/PathTask3/PathTask3__99914b932b-data.parquet"
        flow.reset()

        # Execute task including all its dependencies
        flow = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                        task=PathTask3, path='data/data2/wfm_change', env='prod')
        flow.run()

        path_2 = flow.outputPath(PathTask2)
        path_3 = flow.outputPath(PathTask3)
        assert path_2 == {
            'experiment1': PosixPath('data/data2/wfm_change/env=prod/PathTask2/PathTask2__99914b932b-data.parquet'),
            'experiment2': PosixPath('data/data2/wfm_change/env=prod/PathTask2/PathTask2__99914b932b-data.parquet')
        }
        assert path_3 == {
            'experiment1': PosixPath('data/data2/wfm_change/env=prod/PathTask3/PathTask3__99914b932b-data.parquet'),
            'experiment2': PosixPath('data/data2/wfm_change/env=prod/PathTask3/PathTask3__99914b932b-data.parquet')
        }
        flow.reset()

        # Execute task including all its dependencies
        flow = d6tflow.Workflow(path='data/data2/wf_change_run')
        flow.run(PathTask3, forced_all=True)

        path_2 = flow.outputPath(PathTask2)
        path_3 = flow.outputPath(PathTask3)
        assert str(path_2) == "data/data2/wf_change_run/PathTask2/PathTask2__99914b932b-data.parquet"
        assert str(path_3) == "data/data2/wf_change_run/PathTask3/PathTask3__99914b932b-data.parquet"
        flow.reset(PathTask3)


class TestWorkflowReset:
    def test_reset_downstream(self):
        # Execute task including all its dependencies
        flow = d6tflow.Workflow(PathTask3)
        flow.run()
        # Reset
        flow.reset_downstream(PathTask1)
        assert not flow.complete()

    def test_reset_downstream_multi(self):
        # Execute task including all its dependencies
        flow_multi = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': True}, 'experiment2': {'do_preprocess': True}},
                                  task=PathTask3)
        flow_multi.run()
        # Reset
        flow_multi.reset_downstream(PathTask1)
        # assert not flow_multi.complete()


import sklearn.datasets

# get training data and save it
class GetData(d6tflow.tasks.TaskPqPandas):
    persist = ['xx','yy']

    def run(self):
        ds = sklearn.datasets.load_boston()
        df_trainX = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_trainY = pd.DataFrame(ds.target, columns=['target'])
        self.save({'xx': df_trainX, 'yy': df_trainY}) # persist/cache training data

class TestOutputLoad:
    '''
    df=GetData().outputLoad(keys='x') is dataframe
    df=GetData().outputLoad(keys='x')[0] causes error
    df=GetData().outputLoad(keys=['x','y']) is list
    df=GetData().outputLoad(keys=['x','y'])[0] is dataframe
    df=GetData().outputLoad() works as it normally does
    '''
    def test_outputLoad_correct(self):
        # Execute task including all its dependencies
        flow = d6tflow.Workflow(GetData)
        flow.run()

        # Complete
        is_complete = flow.complete()
        assert is_complete

        # Get Data
        df = GetData().outputLoad(keys='xx')
        assert (type(df)) is pd.DataFrame
        with pytest.raises(Exception):
            exeption_df = df[0]

        df = GetData().outputLoad(keys=['xx'])
        assert (type(df)) is list
        assert (type(df[0])) is pd.DataFrame

        df_2 = GetData().outputLoad(keys=['xx', 'yy'])
        assert (type(df_2)) is list
        assert (type(df_2[0])) is pd.DataFrame
        assert (type(df_2[1])) is pd.DataFrame

        df_3 = GetData().outputLoad()
        assert (type(df_3)) is list
        assert (type(df_3[0])) is pd.DataFrame
        assert (type(df_3[1])) is pd.DataFrame

        # Key not present
        with pytest.raises(Exception):
            GetData().outputLoad(keys='not-there')


class SubTask1(d6tflow.tasks.TaskCache):

    def run(self):
        df = pd.DataFrame({'a': range(3)})
        self.save(df)  # quickly save dataframe

@d6tflow.requires(SubTask1)
class SubTask2(d6tflow.tasks.TaskPqPandas):
    path = 'data/data2_changed'

    def run(self):
        df1 = self.inputLoad() * 2
        self.save(df1)

class SubTask3(d6tflow.tasks.TaskPqPandas):
    
    def run(self):
        temp_flow_df = self.flows['flow'].outputLoad()
        self.save(temp_flow_df)

class TestSubFlows:

    def test_sub_flow_correct(self):
        # Assign Flows
        flow = d6tflow.Workflow(SubTask2)
        flow2 = d6tflow.Workflow(SubTask3)
        flow.run()

        flow2.attach_flow(flow, 'flow')
        flow2.run()

        df_flow_task_1 = flow.outputLoad(SubTask1)
        df_flow_task_2 = flow.outputLoad()
        df_flow_2 = flow2.outputLoad()
        assert df_flow_task_1.equals(pd.DataFrame({'a': range(3)}))
        assert df_flow_task_2.equals(pd.DataFrame({'a': range(3)}) * 2)
        assert df_flow_2.equals(pd.DataFrame({'a': range(3)}) * 2)
