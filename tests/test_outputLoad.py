import d6tflow
import sklearn.datasets
import pandas as pd
import pytest

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

        df_2 = GetData().outputLoad(keys=['xx', 'yy'])
        assert (type(df_2)) is list
        assert (type(df_2[0])) is pd.DataFrame
        assert (type(df_2[1])) is pd.DataFrame

        df_3 = GetData().outputLoad()
        assert (type(df_3)) is list
        assert (type(df_3[0])) is pd.DataFrame
        assert (type(df_3[1])) is pd.DataFrame