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

    flow = d6tflow.Workflow(Task2())

    # This will only export Task 2 to console
    e = d6tflow.FlowExport(tasks=Task2())
    e.generate() 

    # This will export All the flow (Task1, Task2) to a file
    e = d6tflow.FlowExport(flows=flow, save=True, path_export='tasks_export.py')
    e.generate()

Importing Tasks
------------------------------------------------------------

You can import tasks from other folder and files into the file you want to run the flow in.

.. code-block:: python
    scraper = d6tflow.FlowImport(path='../another-project/', module='tasks_export.py', path_data='data/')
    flow_import = d6tflow.Workflow(scraper.tasks.TaskScrape, path=scraper.dirpath, env='prod')
    flow_import.run()

FlowImport returns an object which contains all the Tasks imported from the file under `tasks` and dirpath which is the data directory location.