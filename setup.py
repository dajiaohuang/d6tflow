from setuptools import setup

setup(
    name='d6tflow',
    version='26.6.6',
    packages=['d6tflow','d6tflow.targets','d6tflow.tasks'],
    url='https://github.com/d6t/d6tflow',
    license='MIT',
    author='DataBolt Team',
    author_email='support@databolt.tech',
    description='For data scientists and data engineers, d6tflow is a python library which makes building complex data science workflows easy, fast and intuitive.',
    long_description='d6tflow is a python library which makes it easier to build data workflows'
        'See https://github.com/d6t/d6tflow for details',
    install_requires=['pandas', 'pyarrow', 'markdown', 'openpyxl'
    ],
    extras_require={
        'dask': ['toolz','dask[dataframe]'],
        'cloud-base': ['universal_pathlib'],
        'gcs': ['gcsfs','universal_pathlib'],
        's3': ['s3fs','universal_pathlib'],
        'export': ['jinja2']},
include_package_data=True,
    python_requires='>=3.5',
    keywords=['d6tflow', 'data workflow', 'data pipelines'],
    classifiers=[]
)

'''
pip install -e .

# publish
# pip install setuptools wheel twine
python setup.py sdist bdist_wheel
twine upload dist/*  --skip-existing
'''
