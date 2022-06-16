Sharing Workflows and Outputs
==============================================

Introduction
------------------------------------------------------------

With d6tflow you can Export and Import Tasks from other projects and files.
This makes sharing flow output and handing projects off to others very seamless. Some cases when you want to do this include:

* data engineers share data with data scientists
* vendors sharing data with clients
* teachers sharing data with students

Exporting Tasks
------------------------------------------------------------

You can Export your tasks into a new File or print the tasks in the console.
All parameters, paths, task_group will be exported.

.. code-block:: python
    class Task1():
        def run(self):
            #Save

    @d6tflow.requires(Task1)
    class Task2():
        def run(self):
            #Save

    # This will only export Task 2 to console
    e = d6tflow.FlowExport(tasks=Task2())
    e.generate() 

    # This will export All the flow (Task 1, Task2) to a file
    e = d6tflow.FlowExport(flows=Task2(), save=True, path_export='/path/to/file.py')
    e.generate()

Importing Tasks
------------------------------------------------------------

You can import tasks from other folder and files into the file you want to run the flow in.

.. code-block:: python
    # tasks/tasks.py

    class TaskScrape():
        def run():
            # load input data
            # process data
            # save data

The `tasks/tasks.py` file above contains all the tasks classes we need.
Inside the `repos.py` we will import the tasks we want and run the flow.

.. code-block:: python
    # repos.py
    scraper = d6tflow.FlowImport(path='tasks/', module='tasks', path_data='data/')
    flow_scrape = d6tflow.Workflow(scraper.tasks.TaskScrape, path=scraper.dirpath, env='prod')
    flow_scrape.run()

FlowImport returns an object which contains all the Tasks imported from the file under `tasks` and dirpath which is the data directory location.