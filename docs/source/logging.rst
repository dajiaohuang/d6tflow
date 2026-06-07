Logging
============================================================

d6tflow uses `loguru <https://loguru.readthedocs.io>`_ for logging. So it never interferes with
your application's own logging, **d6tflow logging is disabled by default** — out of the box
d6tflow emits nothing except the execution summary print. You opt in when you want to see what
the engine is doing or to log from inside your own tasks.

Turning logging on and off
------------------------------------------------------------

.. code-block:: python

    import d6tflow

    d6tflow.enable_logging()                 # INFO and above, written to stderr
    d6tflow.enable_logging(level="DEBUG")    # more detail (see levels below)
    d6tflow.disable_logging()                # silence d6tflow again

    flow = d6tflow.Workflow(MyTask)
    flow.run()

With the default ``sink=sys.stderr`` you get one clean d6tflow log stream: ``enable_logging()``
removes loguru's pristine default stderr handler and installs a d6tflow-filtered one at the
chosen level (so records are not printed twice). Calling it again just replaces that handler
rather than stacking another. ``disable_logging()`` silences d6tflow again at the source.

``enable_logging()`` returns the loguru handler id of the sink it added. Keep it if you want to
remove that specific sink later:

.. code-block:: python

    from loguru import logger
    hid = d6tflow.enable_logging()
    ...
    logger.remove(hid)

What gets logged
------------------------------------------------------------

The default ``enable_logging()`` level is ``INFO``. Each level is cumulative — ``DEBUG`` shows
everything ``INFO`` shows, plus more.

* **INFO** — the "what's happening" stream: task start, task complete (with duration), task and
  dependency failures, run summary, and task invalidation.
* **DEBUG** — adds the verbose detail: skipped (already-complete) tasks, save / load / input
  I/O with their keys, and generator yields.
* **WARNING** — an ``external=True`` task whose output is missing.
* **ERROR** — a task's ``run()`` raised; the full traceback is logged via loguru.

Example INFO output for a two-task flow::

    ... | INFO  | d6tflow.core - task start: Task1 (Task1__99914b932b)
    ... | INFO  | d6tflow.core - task complete: Task1__99914b932b in 0.041s
    ... | INFO  | d6tflow.core - task start: Task3 (Task3__a1b2c3d4e5)
    ... | INFO  | d6tflow.core - task complete: Task3__a1b2c3d4e5 in 0.046s
    ... | INFO  | d6tflow.core - run summary: scheduled=2 ran=2 complete=0 failed=0

At ``DEBUG`` you additionally see the I/O and cached skips::

    ... | DEBUG | d6tflow.tasks - saved Task1__99914b932b keys=['data']
    ... | DEBUG | d6tflow.tasks - loaded input for Task3__a1b2c3d4e5 keys=['input1']
    ... | DEBUG | d6tflow.core - task skipped (already complete): Task1__99914b932b

Logging inside your own tasks
------------------------------------------------------------

Every task has a contextual ``self.logger``: a loguru logger pre-tagged with this task's
``task_id`` and ``task_family``. Use it in your ``run()`` (or any task method) so your messages
carry the task identity automatically:

.. code-block:: python

    class TaskTrain(d6tflow.tasks.TaskPickle):
        def run(self):
            df = self.inputLoad()
            self.logger.info("training on {} rows", len(df))    # tagged task_id / task_family
            model = train(df)
            self.logger.debug("converged in {} iterations", model.n_iter_)
            self.save(model)

``self.logger`` lives in d6tflow's logging namespace (its records show under the name
``d6tflow.task``), so — like the engine logs — it is silent until you call
``d6tflow.enable_logging()`` and is silenced again by ``disable_logging()``, no matter which
module your task class is defined in. The ``task_id`` / ``task_family`` tags are attached to each
record's ``extra`` dict; include them in a custom format to display them:

.. code-block:: python

    from loguru import logger
    d6tflow.enable_logging(level="DEBUG")
    logger.add("flow.log", filter="d6tflow",
               format="{time} | {level} | {extra[task_family]} | {message}")

If you would rather log independently of d6tflow's on/off switch, just use your own
``from loguru import logger`` directly in your task code instead of ``self.logger``.

Routing d6tflow logs into your application's logging
------------------------------------------------------------

By default ``enable_logging()`` takes over loguru's default stderr handler (see above). If your
application configures its own loguru handlers and you want d6tflow's records to flow into *your*
sinks rather than have d6tflow touch any handler, pass ``sink=None`` — it only re-enables the
namespace:

.. code-block:: python

    from loguru import logger
    logger.add("app.log")          # your application's own sink

    import d6tflow
    d6tflow.enable_logging(sink=None)   # re-enable the namespace, add/remove NO handler
    # d6tflow records now go wherever your app's loguru sinks point

Note: with ``sink=None``, if you have your own catch-all sink it will receive d6tflow records at
whatever level *that* sink is set to (``enable_logging``'s ``level=`` only governs the sink it
adds, which it does not add in this mode).

Default log level setting
------------------------------------------------------------

When you call ``enable_logging()`` without a ``level=``, it uses ``d6tflow.settings.log_level``
(default ``'INFO'``). Set it once to change the default for every later ``enable_logging()`` call,
or pass ``level=`` to override per call:

.. code-block:: python

    d6tflow.settings.log_level = 'DEBUG'   # change the global default
    d6tflow.enable_logging()               # now defaults to DEBUG
    d6tflow.enable_logging(level='INFO')   # per-call override still wins
