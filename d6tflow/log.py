import sys
from loguru import logger

# Library best practice: emit nothing until the host app opts in.
logger.disable("d6tflow")

# Handler id of the sink the most recent enable_logging() added, so repeat calls
# replace it instead of stacking duplicate sinks.
_handler_id = None


def enable_logging(level=None, sink=sys.stderr):
    """Turn on d6tflow's internal logging.

    With the default ``sink=sys.stderr`` this gives one clean d6tflow log stream:
    loguru's pristine default handler is removed (so records aren't printed twice)
    and a d6tflow-filtered handler is added at ``level``. Calling it again replaces
    that handler rather than adding another. Pass ``sink=None`` to leave all loguru
    handlers untouched and route d6tflow records into the host app's existing sinks.

    Args:
        level (str): minimum level to surface. Defaults to
            ``d6tflow.settings.log_level`` ("INFO").
        sink: where to write. Default sys.stderr. Pass sink=None to only
            re-enable the 'd6tflow' namespace and rely on the host app's
            existing loguru sinks (no new handler added).
    Returns:
        handler id (int) if a sink was added, else None. Pass it to
        logger.remove() for fine-grained teardown.
    """
    global _handler_id
    if level is None:
        # lazy import: settings -> core -> log would be circular at import time.
        from d6tflow import settings
        level = settings.log_level
    logger.enable("d6tflow")
    if sink is None:
        return None
    # Drop the sink a previous enable_logging() added, and loguru's pristine
    # default handler (id 0), so d6tflow records appear exactly once.
    for hid in ([_handler_id] if _handler_id is not None else []) + [0]:
        try:
            logger.remove(hid)
        except ValueError:
            pass
    _handler_id = logger.add(sink, level=level, filter="d6tflow")
    return _handler_id


def disable_logging():
    """Silence d6tflow's internal logging again."""
    logger.disable("d6tflow")


class TaskLogger:
    """Contextual logger facade returned by ``Task.logger``.

    Two things make it behave like the rest of d6tflow's logging:

    * It pre-binds the task's ``task_id`` / ``task_family`` onto every record.
    * The actual emit happens *here*, inside the ``d6tflow`` package, so records
      land in the ``d6tflow`` logging namespace and are therefore governed by
      ``enable_logging`` / ``disable_logging`` -- silent until the app opts in --
      no matter which module the task class is defined in. (The record is patched
      to display the name ``d6tflow.task`` for readability.)

    Forwards every loguru logger method (``debug``/``info``/``warning``/``error``/
    ``exception``/``bind``/...) unchanged; use loguru's ``{}`` brace style for args,
    e.g. ``self.logger.info("rows: {}", len(df))``.
    """
    __slots__ = ("_logger",)

    def __init__(self, _logger=None, **context):
        self._logger = _logger if _logger is not None else logger.patch(
            lambda record: record.update(name="d6tflow.task")).bind(**context)

    def __getattr__(self, name):
        # Delegate to the bound loguru logger, but wrap so the actual call happens
        # in THIS module: loguru derives record["name"] from the calling frame, and
        # only a d6tflow.* name is governed by enable_logging/disable_logging.
        # bind() must stay a TaskLogger so the wrapping survives added context.
        if name == "_logger":  # slot unset (e.g. pre-init) -> avoid recursion
            raise AttributeError(name)
        attr = getattr(self._logger, name)
        if name == "bind":
            return lambda **kwargs: TaskLogger(_logger=attr(**kwargs))
        if not callable(attr):
            return attr
        return lambda *args, **kwargs: attr(*args, **kwargs)
