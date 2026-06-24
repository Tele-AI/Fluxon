from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class _Suite:
    run_mode: str
    run_selectors: "_RunSelectors"
    scenes: Dict[str, Dict[str, Any]]
    scales: Dict[str, Dict[str, Any]]
    artifact_sets: Dict[str, Dict[str, Any]]
    profiles: Dict[str, Dict[str, Any]]


@dataclass(frozen=True)
class _RunSelectors:
    case_ids: Optional[Tuple[str, ...]]
    profile_ids: Tuple[str, ...]
    command_ids: Optional[Tuple[str, ...]]
    test_ids: Optional[Tuple[str, ...]]


@dataclass(frozen=True)
class _ResolvedCase:
    scene_id: str
    scale_id: str
    profile_id: str
    case_id: str
    case_key: str


@dataclass
class _RunSlot:
    case_key: str
    case_id: str
    run_index: int
    rec: Dict[str, Any]


@dataclass(frozen=True)
class _PlannedCase:
    case: _ResolvedCase
    ci_commands: Optional[List[Dict[str, Any]]]
    ci_prepare_steps: Optional[List[Dict[str, Any]]]
    label: str
    command_id: Optional[str]
    test_id: Optional[str]
    counted: bool


@dataclass(frozen=True)
class _ObservedFileState:
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class _RemoteRunDirStage:
    archive_prefix: str
    stage_prefix: str
    verify_relpaths: Tuple[str, ...]
    ctx: str
    sync_mode: str
    include_relpaths: Optional[Tuple[str, ...]] = None


@dataclass(frozen=True)
class _RuntimePhase:
    phase_id: str
    layer: str
    instance_ids: Tuple[str, ...]
    write_ctx: str
    stage_run_dir: Optional[_RemoteRunDirStage] = None


class _RetryableControllerStatusError(RuntimeError):
    pass


@dataclass(frozen=True)
class _CasePlan:
    case_family: str
    prepare_phases: Tuple[_RuntimePhase, ...]
    execute_phases: Tuple[_RuntimePhase, ...]
    collect_phases: Tuple[_RuntimePhase, ...]


@dataclass(frozen=True)
class _PreparedCase:
    plan: _CasePlan
    ci_runner_exit_code_baseline: Optional[_ObservedFileState] = None
    test_stack_result_path: Optional[Path] = None
    test_stack_coordinator_addr: Optional[str] = None
    test_stack_result_timeout_s: Optional[int] = None


@dataclass(frozen=True)
class _ExecutedCase:
    outcome: str
    summary: Dict[str, Any]


@dataclass
class _CaseRuntimeTracking:
    ci_lock_fp: Optional[Any] = None
    controller_lock_fp: Optional[Any] = None
    ci_attempted_instance_ids: List[str] = field(default_factory=list)
    ci_apply_ids: Dict[str, str] = field(default_factory=dict)
    ts_coord_deploy_attempted: bool = False
    ts_nodes_deploy_attempted: bool = False
    ts_coord_apply_id: Optional[str] = None
    ts_nodes_apply_id: Optional[str] = None
