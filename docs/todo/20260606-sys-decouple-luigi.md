# Remove the `luigi` dependency from d6tflow

## Context

d6tflow is a thin data-science workflow layer built on top of `luigi`. In practice it only
uses a small, well-defined slice of luigi: the `Task` base class, a few parameter types, a
dependency-graph executor (`luigi.build`), and helpers (`flatten`, `find_deps`,
`inherits`/`requires`, `bcolors`). The full luigi install drags in a central scheduler,
multiprocessing workers, RPC, a CLI/config system, and contrib targets — none of which
d6tflow needs.

Goal: drop the `luigi` dependency entirely and vendor a minimal, self-contained
re-implementation of just the pieces d6tflow uses, removing the bloat.

### Confirmed scope decisions (from the user)
- **Execution engine: sequential only.** Run the DAG in dependency order in-process. Keep the
  `workers` argument for API compatibility but ignore it. Still support generator-style
  `run()` that `yield`s tasks (needed for `TaskAggregator`).
- **Parameter types: trim to the common set.** Keep `Parameter`, `IntParameter`,
  `FloatParameter`, `BoolParameter`, `DateParameter`, `DictParameter`, `ListParameter`,
  `EnumParameter`. Drop the rest (Month/Year/DateHour/DateMinute/DateSecond/DateInterval/
  TimeDelta/Task/Tuple/Numerical/Choice/Optional). Accepted breaking change.
- **On-disk format: no back-compat required.** Free to use a simplified deterministic
  `task_id`/hash. Must stay deterministic and keep `task_id.split('_')[0] == TaskFamily`
  (the `_getpath` directory convention in `tasks/__init__.py:112`).
- **`d6tflow2` is a separate follow-up.** It (`D:\OneDrive\dev\d6tlib\d6tflow2`) subclasses
  d6tflow's `TaskData`/targets and mixes in luigi params; it will be updated *after* this
  rewrite, not now. This rewrite must therefore **keep the surface d6tflow2 depends on
  stable** (see "Stable contract" below) but need not preserve luigi internals.

## Validated against real projects (6 surveyed)
- **Parameters actually used:** `Parameter`, `IntParameter`, `FloatParameter`,
  `BoolParameter`, `ListParameter` — all within the kept set. `significant=False` is used
  widely (e.g. `env = d6tflow.Parameter(significant=False)`). No `Date/Dict/Enum` seen, but
  they're cheap to keep.
- **Task bases:** `TaskPqPandas`, `TaskPickle`, `TaskJson`, `TaskExcelPandas`,
  `TaskCSVPandas`, `TaskMarkdown`, `TaskAggregator`.
- **Dependencies:** `@d6tflow.requires(Single)`, tuple form, and **dict form**
  `{'key': Task}` with nested `self.input()['key']['sub'].load()`. `TaskAggregator` with
  `yield self.clone(Task)` is in real use.
- **Orchestration:** `Workflow`, `WorkflowMulti`, `flow.run(forced_all=…, forced_all_upstream=…,
  confirm=…, execution_summary=…)`, `preview`, `reset`/`reset_downstream`/`reset_upstream`,
  `get_task`, `outputLoad(as_dict=True)`, `saveMeta`/`metaLoad`, `set_dir`, `settings.dirpath`,
  `d6tflow.pipes` (FlowExport/pull/push). **No project uses `workers>1`** → sequential is safe.
- **`external=True`** tasks appear (and in d6tflow2) → the engine must handle them.
- **`to_str_params(only_significant=True)`** is load-bearing (d6tflow2 builds table names from
  it) → must be preserved with that signature.

## Current luigi surface used (what we must replace)
- `luigi.Task` → base for `TaskData`/`TaskAggregator` (`tasks/__init__.py`). Relied-on members:
  `get_params`, `get_param_names`, `get_param_values`, `to_str_params`, `clone`, `task_id`,
  `task_family`/`get_task_family`, `complete`, `output`, `requires`, `input`, `__eq__`/`__hash__`.
- `luigi.LocalTarget` → base for `CacheTarget`/`_LocalPathTarget` (`targets/__init__.py`); only
  needs `__init__(path)` storing `self.path`.
- `luigi.task.flatten` (`__init__.py`, `tasks/__init__.py`, `utils.py`).
- `luigi.build` (`__init__.py:160`) → object with `.scheduling_succeeded` (True iff every task
  ran or was already complete) and `.summary_text`.
- `luigi.tools.deps.find_deps(task, family)` (`__init__.py:199`).
- `luigi.util.inherits`/`luigi.util.requires` (`__init__.py:5,356,364`).
- `luigi.task_register.Register` (`__init__.py:949`, FlowImport class detection).
- `luigi.task.TASK_ID_INCLUDE_PARAMS`/`TASK_ID_TRUNCATE_PARAMS` (`settings.py:18-21`).
- `luigi.tools.deps_tree.bcolors` (`utils.py:8`).
- `luigi.parameter.*` (`__init__.py:6-13`).
- `luigi.worker.worker().no_install_shutdown_handler` (`__init__.py:122`) — drop entirely.

> `d6tflow/functional.py` is **secondary** (uses only `d6tflow.requires`) — no changes needed.

## Stable contract for the later d6tflow2 update
Keep these names/behaviors so the separate d6tflow2 pass is mechanical: `d6tflow.tasks.TaskData`,
`d6tflow.tasks.TaskPqPandas`, `d6tflow.targets._LocalPathTarget`, `d6tflow.targets.DataTarget`
(same `load(fun,...)`/`save(df,fun,...)` interface); `Task.to_str_params(only_significant=…)`,
`Task.clone`, `Task.get_params`, `Task.get_param_names`, `task_id`/`task_family`; `external` and
`persist` semantics. (d6tflow2 will swap its `from luigi.parameter import BoolParameter` →
`d6tflow.BoolParameter` and update its `FlowImport` metaclass check itself.)

## Design: vendor a "mini-luigi" in two new modules

### 1. `d6tflow/parameter.py` (new)
Self-contained trimmed port of `luigi/parameter.py`. No `configuration`, `cmdline_parser`,
`date_interval`, `freezing`, or `jsonschema`.
- `_no_value = object()`.
- `class Parameter`: `__init__(default=_no_value, significant=True, description=None,
  positional=True)`, a class-level `_counter` for ordering, `parse`, `serialize` (→`str`),
  `normalize`, `has_task_value` (= default present), `task_value` (= `normalize(default)`).
  Value resolution collapses to **default only** (drop cmdline/config `_value_iterator`).
- Kept subclasses: `IntParameter`, `FloatParameter`, `BoolParameter` (implicit default `False`,
  true/false parse), `DateParameter` (`%Y-%m-%d` parse/serialize, normalize to `date`; drop
  interval/start machinery), `DictParameter` (`serialize=json.dumps(x,sort_keys=True)`,
  `parse`/`normalize` identity — no freezing), `ListParameter` (json), `EnumParameter`
  (`enum=` kwarg, name-based). Since the engine is sequential and identity is by `task_id`
  string, param values need not be hashable → dict/list stored raw.

### 2. `d6tflow/core.py` (new) — the mini-engine
- `flatten(struct)` and `getpaths(struct)` — small ports from `luigi/task.py`.
- `task_id_str(family, params_dict)` — simplified deterministic id
  `"{family}_{param_summary}_{md5(sorted_json)[:10]}"`; keep the `family_…` prefix so
  `split('_')[0]` yields the family. Module-level `TASK_ID_INCLUDE_PARAMS`/
  `TASK_ID_TRUNCATE_PARAMS` so `settings.set_parameter_len` can tune them.
- `class Register(type)` — minimal metaclass providing a **class-level** `task_family`
  property (needed by `WorkflowMulti.__init__` reading `task.task_family` on a class,
  `__init__.py:589`). No instance cache, no global registry. *(Note: changing the metaclass
  away from luigi's `Register` is what will require d6tflow2's FlowImport one-liner later.)*
- `class Task(metaclass=Register)` — simplified port of `luigi.Task`:
  `get_params`/`get_param_names`/`get_param_values` (positional+kwargs+defaults; drop
  config/unconsumed/visibility), `__init__` (set params, `param_kwargs`, `task_id`, hash),
  `to_str_params(only_significant=False)` (check only `.significant`, no visibility), `clone`,
  instance `task_family` property + `get_task_family` classmethod, `__eq__`/`__hash__` by
  `task_id`, default `complete`/`output`/`requires`/`input`/`run`.
- `class Target` — minimal abstract base (just an `exists()` stub) to serve as a **drop-in
  replacement for `luigi.target.Target`**, re-exported as `d6tflow.targets.Target` so
  d6tflow2's `SQLPandasTableTarget(luigi.target.Target)` becomes a one-line base swap.
- `class LocalTarget(Target)` — tiny base: `__init__(self, path)` → `self.path = path`
  **stored as-is, NOT coerced to `str`**; default `exists()`. (`CacheTarget`/`_LocalPathTarget`
  override the rest.) NB: luigi's `LocalTarget` coerced `self.path` to an OS-native `str`, which
  made `CacheTarget` key the in-memory cache by `str` while `DataTarget` keyed by `pathlib.Path`
  — an inconsistency already hot-fixed in the working tree by giving `CacheTarget.__init__` an
  explicit `self.path = pathlib.Path(path)`. Our non-coercing base removes the root cause; the
  existing `CacheTarget`/`_LocalPathTarget` `super().__init__(path)` + `self.path = Path(path)`
  overrides keep working and now key the cache by `Path` uniformly.
- `inherits`/`requires` decorator classes — ported from `luigi/util.py` (copy params + add
  `clone_parent`/`clone_parents`/`requires`). No `Register`/`parameter` dependency.
- `find_deps(task, upstream_family)` + `dfs_paths` — small port from `luigi/tools/deps.py`.
- `build(tasks, workers=1, detailed_summary=False, **ignored)` — **sequential executor**:
  - Recursive `process(task)`: if `task.complete()` → already done; else process each dep in
    `flatten(task.requires())` first (abort task if a dep failed), then execute.
  - **External tasks** (`getattr(task,'external',False)` truthy, or `run` is `None`): never
    executed; if still not complete after deps, mark failed (output must come from elsewhere).
  - **Generator `run()`**: iterate it, `flatten` each yielded value into tasks, `process`
    them, then resume (covers `TaskAggregator` dynamic deps and `yield self.clone(...)`).
  - Wrap `run()` in try/except: on error print the traceback, mark `success=False`, skip
    downstream. Memoize processed `task_id`s to avoid rework/cycles.
  - **Re-entrant:** all state (visited/failed memo, result accumulation) is local to each
    `build()` call — never module-global — so a task's `run()` may itself call
    `flow.run()`/`d6tflow.run()` (the "flow-within-a-flow" fan-out pattern used in real
    projects, e.g. ubs-re `tasks_llm.py` and smartscreener `devops_run_all.py`). Nested builds
    must not corrupt the outer build's state.
  - Return a small `LuigiRunResult`-shaped object with `.scheduling_succeeded` (True iff no
    failures) and `.summary_text` (short ran/complete/failed summary) — matches what
    `d6tflow.run` consumes at `__init__.py:161-172`.

## Files to modify
- **`d6tflow/__init__.py`**: replace luigi imports (2-13) with `from d6tflow import core`,
  `from d6tflow.core import flatten`, and the kept params from `d6tflow.parameter`. In `run()`
  delete the `luigi.worker…` line (122) and the `luigi.__version__` gate (158); use
  `core.build(...)`. `taskflow_downstream` → `core.find_deps` (199). `inherits`/`requires`
  (353-364) call `core.inherits`/`core.requires` (keep `dict_inherits`/`dict_requires`).
  FlowImport (949): `isinstance(obj, type) and issubclass(obj, d6tflow.tasks.TaskData)`.
  FlowExport template (848-871): generated file imports `d6tflow` only (params render as
  `d6tflow.parameter.*`).
- **`d6tflow/tasks/__init__.py`**: `from d6tflow import core`; `class TaskData(core.Task)`,
  `class TaskAggregator(core.Task)`; `complete()` uses `core.flatten`.
- **`d6tflow/targets/__init__.py`**: `from d6tflow import core`;
  `CacheTarget(core.LocalTarget)`, `_LocalPathTarget(core.LocalTarget)`.
- **`d6tflow/settings.py`**: `set_parameter_len` sets `d6tflow.core.TASK_ID_INCLUDE_PARAMS`/
  `TASK_ID_TRUNCATE_PARAMS` instead of `luigi.task.*`.
- **`d6tflow/utils.py`**: `from d6tflow.core import flatten`; drop the `bcolors` import, add a
  tiny local `bcolors` (ANSI `OKGREEN`/`OKBLUE`/`ENDC`).
- **`setup.py`**: remove `'luigi>=3.0.1'` from `install_requires`.
- **Tests/examples importing luigi**: `tests/test_main.py`, `tests/test_workflow.py`,
  `tests/for_import.py` (and `docs/example-*.py`) → switch `luigi.IntParameter`/`BoolParameter`/
  `Parameter` to the `d6tflow.*` equivalents; drop `import luigi`.

## Making the d6tflow2 update trivial (separate follow-up)
d6tflow2's coupling to luigi is small and mechanical. Because this rewrite preserves every
d6tflow name d6tflow2 imports (`d6tflow.tasks.TaskData`/`TaskPqPandas`/`TaskAggregator`/
`TaskCache`, `d6tflow.targets.DataTarget`/`_LocalPathTarget`, `d6tflow.Workflow`/`requires`/
`run`/`preview`/`Parameter`/`BoolParameter`/`FlowExport`) **and** adds a drop-in
`d6tflow.targets.Target`, the entire d6tflow2 update is import swaps. Exact edits (dev source
`D:\OneDrive\dev\d6tlib\d6tflow2`):
- `d6tflow2/tasks/athena.py:4`, `tasks/mongo.py:3`, `tasks/sql.py:4`:
  `from luigi.parameter import BoolParameter` → `from d6tflow import BoolParameter`.
- `d6tflow2/tasks/spark.py:1`: `import d6tflow, luigi` → `import d6tflow` (**luigi unused**).
- `d6tflow2/targets/athena.py:1`: `import luigi, warnings` → `import warnings` (**luigi unused**).
- `d6tflow2/targets/sql.py:1,7`: drop `import luigi`; `SQLPandasTableTarget(luigi.target.Target)`
  → `SQLPandasTableTarget(d6tflow.targets.Target)`.
- `d6tflow2/__init__.py:7,49`: drop `import luigi`; replace
  `type(obj) is luigi.task_register.Register` with
  `isinstance(obj, type) and issubclass(obj, d6tflow.tasks.TaskData)` (same fix as d6tflow's
  own FlowImport).
- `d6tflow2/tests/for_import.py` and `setup.py` keyword: migrate `luigi.*` params / drop the
  `luigi` keyword.
- Its `TaskData`/`DataTarget`/`_LocalPathTarget` subclasses and `to_str_params`/`external`/
  `persist` usage need **no change** — those interfaces are preserved by the stable contract.

## Out of scope (confirmed)
- **Fan-out / concat / "rerun-all-on-code-change" patterns** are user-side and need **no
  library design changes** — preserving re-entrancy + dynamic dict-`requires` is enough for
  existing flow-within-a-flow code and the `WorkflowMulti`-based patterns to keep working. Any
  ergonomic helper (concat task, version-based invalidation) is a separate future effort.
- **Primary user API is `flow.run()`** (`Workflow`/`WorkflowMulti`), not `d6tflow.run(...)`.
  `d6tflow.run` remains the internal executor that `Workflow.run()` calls into; it stays
  working but is not the recommended entry point in examples.

## Caveats / follow-ups (not done in this change)
- **User projects with direct `import luigi`** — audited; the surface is negligible:
  - `smartscreener` (reco-20200803-bak): 5 files have a bare `import luigi` but **no `luigi.X`
    usage at all** (vestigial). Only impact: a bare import errors if luigi is absent → delete
    the unused line (e.g. `import d6tflow, luigi` → `import d6tflow`).
  - `flow-crs`: 2 files (`tasks_d6tpipe.py:9`, `tasks_export.py:9`) use exactly one symbol,
    `luigi.parameter.Parameter(default=…)` → swap to `d6tflow.Parameter`. (This project uses
    d6tflow2, so luigi stays installed there anyway — not even breaking.)
  - Note: uninstalling luigi from existing envs is not required by this change; new clean
    installs simply won't pull luigi transitively.

## Verification
1. With luigi uninstalled from the env, `python -c "import d6tflow"` imports cleanly (banner prints).
2. `grep -rn "import luigi\|luigi\." d6tflow/` returns nothing (functional.py excluded — already luigi-free).
3. `pytest tests/test_main.py tests/test_workflow.py tests/test_workflowMulti.py
   tests/test_workflowMulti2.py -q` — covers param defaults/`significant`, `complete(cascade=)`,
   single/list/dict `requires`, every target format, `run`/`preview`, force/invalidate up/down,
   `@requires`/`@inherits`, `Workflow`/`WorkflowMulti`, `TaskAggregator`, FlowExport/FlowImport.
4. Smoke-test a 3-task chain + a `TaskAggregator` (run → complete → outputLoad → invalidate
   upstream → re-run) to confirm DAG ordering and dynamic-yield execution.
5. Confirm `d6tflow.run` still raises `RuntimeError` when a task's `run()` throws
   (abort path at `__init__.py:165`).
6. Confirm `to_str_params(only_significant=True)` returns the same shape d6tflow2 expects
   (dict of name→serialized significant params), and that `external=True` tasks are treated as
   non-runnable.
