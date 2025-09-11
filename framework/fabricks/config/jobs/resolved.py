from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, TypeVar

from pydantic import computed_field

from fabricks.config.base import ModelBase
from fabricks.config.runtime import RuntimeConfig
from fabricks.config.steps.base import BaseStepConfig
from fabricks.config.steps.bronze import BronzeStepConfig
from fabricks.config.steps.silver import SilverStepConfig
from fabricks.config.steps.gold import GoldStepConfig
from fabricks.config.jobs.base import BaseJobConfig
from fabricks.config.jobs.bronze import BronzeJobConfig
from fabricks.config.jobs.silver import SilverJobConfig
from fabricks.config.jobs.gold import GoldJobConfig


TJob = TypeVar("TJob", bound=BaseJobConfig)
TStep = TypeVar("TStep", bound=BaseStepConfig)


class _Resolver(ModelBase):
    """
    Mixin providing helpers to resolve layered configuration values with precedence:
    job > step > runtime.
    """

    @staticmethod
    def _first_not_none(*values: Tuple[Optional[Any], ...]) -> Optional[Any]:
        for v in values:
            if v is not None:
                return v
        return None

    @staticmethod
    def _merge_dicts(*dicts: Tuple[Optional[Dict[str, Any]], ...]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        for d in dicts:
            if d:
                merged.update(d)
        return merged


class ResolvedConfigBase(_Resolver):
    """
    Base composite model that encapsulates a concrete Job, its parent Step configuration,
    the global Runtime configuration, and optional raw step extras (untyped fields present
    in step YAML but not captured by the typed step model).
    """

    runtime: RuntimeConfig
    step: BaseStepConfig
    job: BaseJobConfig

    # Extra/untyped fields that may appear in step YAML (e.g. spark_options)
    step_extras: Optional[Dict[str, Any]] = None

    # ---------- Identity ----------

    @computed_field
    @property
    def qualified_name(self) -> str:
        # BaseJobConfig computes job string as f"{step}.{topic}_{item}"
        return self.job.job

    # ---------- Timeouts (job/step/runtime layered) ----------

    @computed_field
    @property
    def timeout_job(self) -> int:
        # job.options.timeout (single int) > step.options.timeouts.job > runtime.options.timeouts.job
        job_timeout: Optional[int] = getattr(self.job.options, "timeout", None)
        step_timeouts = getattr(self.step.options, "timeouts", None)
        step_job_timeout: Optional[int] = getattr(step_timeouts, "job", None) if step_timeouts else None
        runtime_job_timeout: Optional[int] = self.runtime.options.timeouts.job if self.runtime.options and self.runtime.options.timeouts else None

        val = self._first_not_none(job_timeout, step_job_timeout, runtime_job_timeout)
        if val is None:
            raise ValueError("Unable to resolve timeout_job from job/step/runtime")
        return int(val)

    @computed_field
    @property
    def timeout_step(self) -> Optional[int]:
        # step.options.timeouts.step > runtime.options.timeouts.step
        step_timeouts = getattr(self.step.options, "timeouts", None)
        step_val: Optional[int] = getattr(step_timeouts, "step", None) if step_timeouts else None
        runtime_val: Optional[int] = self.runtime.options.timeouts.step if self.runtime.options and self.runtime.options.timeouts else None
        val = self._first_not_none(step_val, runtime_val)
        return int(val) if val is not None else None

    @computed_field
    @property
    def timeout_pre_run(self) -> Optional[int]:
        step_timeouts = getattr(self.step.options, "timeouts", None)
        step_val: Optional[int] = getattr(step_timeouts, "pre_run", None) if step_timeouts else None
        runtime_val: Optional[int] = self.runtime.options.timeouts.pre_run if self.runtime.options and self.runtime.options.timeouts else None
        val = self._first_not_none(step_val, runtime_val)
        return int(val) if val is not None else None

    @computed_field
    @property
    def timeout_post_run(self) -> Optional[int]:
        step_timeouts = getattr(self.step.options, "timeouts", None)
        step_val: Optional[int] = getattr(step_timeouts, "post_run", None) if step_timeouts else None
        runtime_val: Optional[int] = self.runtime.options.timeouts.post_run if self.runtime.options and self.runtime.options.timeouts else None
        val = self._first_not_none(step_val, runtime_val)
        return int(val) if val is not None else None

    # ---------- Workers (step/runtime layered) ----------

    @computed_field
    @property
    def workers(self) -> Optional[int]:
        # step.options.workers > runtime.options.workers
        step_workers: Optional[int] = getattr(self.step.options, "workers", None)
        runtime_workers: Optional[int] = getattr(self.runtime.options, "workers", None) if self.runtime.options else None
        val = self._first_not_none(step_workers, runtime_workers)
        return int(val) if val is not None else None

    # ---------- Table options (job/step/runtime layered) ----------

    @computed_field
    @property
    def retention_days(self) -> Optional[int]:
        # job.table_options.retention_days > step.table_options.retention_days > runtime.options.retention_days
        job_table = getattr(self.job, "table_options", None)
        step_table = getattr(self.step, "table_options", None)
        job_val: Optional[int] = getattr(job_table, "retention_days", None) if job_table else None
        step_val: Optional[int] = getattr(step_table, "retention_days", None) if step_table else None
        runtime_val: Optional[int] = getattr(self.runtime.options, "retention_days", None) if self.runtime.options else None
        val = self._first_not_none(job_val, step_val, runtime_val)
        return int(val) if val is not None else None

    # ---------- Spark options (merged across runtime/step/job) ----------

    def _step_spark_extras(self) -> Dict[str, Any]:
        return (self.step_extras or {}).get("spark_options", {}) or {}

    @computed_field
    @property
    def spark_sql_options(self) -> Dict[str, Any]:
        runtime_sql = (self.runtime.spark_options.sql if self.runtime.spark_options else None)
        step_sql = self._step_spark_extras().get("sql")
        job_sql = (self.job.spark_options.sql if self.job.spark_options else None)
        # Precedence: runtime -> step -> job (later overrides earlier)
        return self._merge_dicts(runtime_sql, step_sql, job_sql)

    @computed_field
    @property
    def spark_conf_options(self) -> Dict[str, Any]:
        runtime_conf = (self.runtime.spark_options.conf if self.runtime.spark_options else None)
        step_conf = self._step_spark_extras().get("conf")
        job_conf = (self.job.spark_options.conf if self.job.spark_options else None)
        # Precedence: runtime -> step -> job (later overrides earlier)
        return self._merge_dicts(runtime_conf, step_conf, job_conf)

    # ---------- Paths (step/runtime layered) ----------

    @computed_field
    @property
    def storage_path(self) -> str:
        # step.path_options.storage (required) with fallback to runtime.path_options.storage
        step_storage: Optional[str] = getattr(self.step.path_options, "storage", None)
        runtime_storage: Optional[str] = getattr(self.runtime.path_options, "storage", None)
        val = self._first_not_none(step_storage, runtime_storage)
        if val is None:
            raise ValueError("Unable to resolve storage_path from step/runtime")
        return str(val)

    @computed_field
    @property
    def runtime_step_path(self) -> str:
        # step.path_options.runtime (required on steps)
        runtime_path: Optional[str] = getattr(self.step.path_options, "runtime", None)
        if runtime_path is None:
            raise ValueError("Unable to resolve runtime_step_path from step")
        return str(runtime_path)

    # ---------- Extenders (job/step/runtime layered) ----------

    @computed_field
    @property
    def extenders(self) -> List[Dict[str, Any]]:
        """
        Resolve extenders with precedence:
        - job.extender_options: List[{extender, arguments}]
        - step.options.extenders: List[str]  (names only, converted to dicts)
        - runtime.extender_options: single {extender, arguments}
        Returns a uniform list of dicts with keys {extender, arguments?}.
        """
        # Job-level extenders (already structured)
        job_extenders = getattr(self.job, "extender_options", None) or []
        if job_extenders:
            return [{"extender": e.extender, "arguments": getattr(e, "arguments", None)} for e in job_extenders]

        # Step-level extenders (names only)
        step_names: List[str] = getattr(self.step.options, "extenders", None) or []
        if step_names:
            return [{"extender": name} for name in step_names]

        # Runtime-level extender (single)
        runtime_ext = getattr(self.runtime, "extender_options", None)
        if runtime_ext:
            return [{"extender": runtime_ext.extender, "arguments": getattr(runtime_ext, "arguments", None)}]

        return []

    # ---------- Misc ----------

    @computed_field
    @property
    def tags(self) -> List[str]:
        return list(self.job.tags or [])

    @computed_field
    @property
    def comment(self) -> Optional[str]:
        return self.job.comment


class BronzeResolvedConfig(ResolvedConfigBase):
    runtime: RuntimeConfig
    step: BronzeStepConfig
    job: BronzeJobConfig


class SilverResolvedConfig(ResolvedConfigBase):
    runtime: RuntimeConfig
    step: SilverStepConfig
    job: SilverJobConfig


class GoldResolvedConfig(ResolvedConfigBase):
    runtime: RuntimeConfig
    step: GoldStepConfig
    job: GoldJobConfig
