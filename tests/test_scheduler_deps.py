"""调度器依赖触发测试（M8-1 Task 3）：``after`` 依赖声明与单轮拓扑触发。

运行：``python -m unittest discover -s tests -k test_scheduler_deps -v``

覆盖：
- 上游本轮 success → 同轮按拓扑序触发下游；无依赖条目按声明序先行；
- 上游本轮失败 → 下游跳过（info 日志），下一轮上游成功后下游恢复触发；
- 依赖本身是 after 条目且本轮被跳过 → 依赖它的条目连锁跳过；
- 纯 str 条目（无 after）每轮照常触发，行为与 v0.8.0 一致；
- ``_normalize_pipelines`` 接受 str/Path/dict 条目；CLI ``--after`` 解析
  （可重复、逗号分隔、缺 ``=`` 报错）与合并进条目（追加在自身声明之后）；
- 未知依赖名启动期 warning 不 fail；``after`` 成环抛 ``ConfigError`` 含环链路；
- 端到端：真实 JSON 配置 + RunStore——上游文件缺失失败，补上文件后下游恢复触发。

两类测试手法：
1. 逻辑测试用脚本化 ``_run_once`` 替身 + 执行配额自动置位 ``stop_event``：
   单线程同步跑满指定次数即退出，不睡眠、零轮询，可断言精确的触发顺序；
2. 端到端测试沿用既有调度器测试风格：后台线程 + RunStore 轮询等待，
   不用固定 sleep 判断业务结果。

遵循现有测试风格：仅文件顶部做路径准备，业务 import 放在测试方法内。
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# 小型流水线配置：单个常量节点，毫秒级执行；逻辑测试中配置内容不会被真正加载，
# 写成真实文件仅为沿用 CLI/归一化按 stem 取名的构造方式。
MINI_CFG = {
    "name": "deps_mini",
    "version": "1.0",
    "nodes": [
        {"id": "seed", "op": "control.const", "params": {"value": {"x": 1}}},
    ],
    "outputs": {"out": "${seed.value}"},
}

# 端到端上游配置：读取尚不存在的文件 → 本轮失败；补上文件后恢复成功。
UP_CFG = {
    "name": "dep_up",
    "version": "1.0",
    "nodes": [
        {"id": "load", "op": "io.read_text", "params": {"path": "dep_input.txt"}},
    ],
    "outputs": {},
}

# 端到端下游配置：单个常量节点，恒成功。
DOWN_CFG = {
    "name": "dep_down",
    "version": "1.0",
    "nodes": [
        {"id": "seed", "op": "control.const", "params": {"value": {"ok": 1}}},
    ],
    "outputs": {"out": "${seed.value}"},
}


def _write_cfg(directory: Path, name: str, cfg: dict) -> Path:
    path = directory / f"{name}.json"
    path.write_text(json.dumps(cfg, ensure_ascii=False), encoding="utf-8")
    return path


def _wait(predicate, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


class _ScriptedRunner:
    """脚本化 ``_run_once`` 替身：按 pipeline 名出队状态脚本并记录触发顺序。

    状态脚本（按配置文件 stem 匹配）：列表按执行次序出队，出队到最后一项后
    一直重复该项；未配置的名字默认恒 success。累计执行次数达到 ``quota`` 时
    自动置位 ``stop_event``，让调度循环在当前轮结束（无间隔等待）后退出。
    """

    def __init__(
        self,
        scripts: dict[str, list[str]],
        stop: threading.Event,
        quota: int,
    ) -> None:
        self.scripts = {name: list(seq) for name, seq in scripts.items()}
        self.stop = stop
        self.quota = quota
        self.executed: list[str] = []

    def __call__(self, pipeline, **kwargs) -> tuple[bool, dict[str, str]]:
        name = Path(pipeline).stem
        script = self.scripts.get(name, ["success"])
        status = script.pop(0) if len(script) > 1 else script[0]
        self.executed.append(name)
        if len(self.executed) >= self.quota:
            self.stop.set()
        ok = status == "success"
        return ok, {
            "run_id": f"fake-{len(self.executed)}",
            "status": status,
            "error": "" if ok else f"{name} 脚本失败",
        }


def _run_rounds(pipeline, *, scripts: dict[str, list[str]], quota: int, after=None):
    """同步跑调度器：替身凑满 quota 次执行后自动置位 stop。

    返回 ``(累计触发次数, 触发顺序的 pipeline 名列表, 替身)``；
    ``after`` 透传给 :func:`run_scheduler`（模拟 CLI ``--after`` 解析结果）。
    """
    from scheduler_app import run_scheduler

    stop = threading.Event()
    fake = _ScriptedRunner(scripts, stop, quota)
    with mock.patch("scheduler_app.scheduler._run_once", fake):
        count = run_scheduler(
            pipeline, every=0.001, after=after, stop_event=stop,
        )
    return count, fake.executed, fake


class TopoTriggerTest(unittest.TestCase):
    """Task 3：上游成功同轮触发下游 + 单轮拓扑顺序（脚本化替身，零睡眠）。"""

    def test_upstream_success_triggers_downstream_same_round(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_a = _write_cfg(d, "a", MINI_CFG)
            cfg_b = _write_cfg(d, "b", {**MINI_CFG, "name": "deps_b"})
            count, executed, _ = _run_rounds(
                [str(cfg_a), {"path": str(cfg_b), "after": ["a"]}],
                scripts={"a": ["success"], "b": ["success"]},
                quota=2,
            )
        self.assertEqual(executed, ["a", "b"], "上游成功后同轮应触发下游")
        self.assertEqual(count, 2)

    def test_execution_order_plain_first_then_topological(self):
        """声明序 [b(after a), a, c(after b)]：无依赖 a 先行，再按拓扑跑 b、c。"""
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_a = _write_cfg(d, "a", MINI_CFG)
            cfg_b = _write_cfg(d, "b", {**MINI_CFG, "name": "deps_b"})
            cfg_c = _write_cfg(d, "c", {**MINI_CFG, "name": "deps_c"})
            count, executed, _ = _run_rounds(
                [
                    {"path": str(cfg_b), "after": ["a"]},
                    str(cfg_a),
                    {"path": str(cfg_c), "after": ["b"]},
                ],
                scripts={},
                quota=3,
            )
        self.assertEqual(executed, ["a", "b", "c"], "应先跑无依赖条目再按拓扑跑依赖条目")
        self.assertEqual(count, 3)

    def test_run_scheduler_after_param_attaches_deps(self):
        """模拟 CLI ``--after b=a``：str 条目 b 经 after 参数获得依赖并同轮后置触发。"""
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_a = _write_cfg(d, "a", MINI_CFG)
            cfg_b = _write_cfg(d, "b", {**MINI_CFG, "name": "deps_b"})
            count, executed, _ = _run_rounds(
                [str(cfg_a), str(cfg_b)], scripts={}, quota=2, after={"b": ["a"]},
            )
        self.assertEqual(executed, ["a", "b"], "CLI after 应使 b 在 a 成功后才触发")
        self.assertEqual(count, 2)


class SkipAndResumeTest(unittest.TestCase):
    """Task 3：依赖未满足跳过（含连锁跳过）+ 下一轮恢复触发。"""

    def test_upstream_failure_skips_downstream_then_recovers(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_a = _write_cfg(d, "a", MINI_CFG)
            cfg_b = _write_cfg(d, "b", {**MINI_CFG, "name": "deps_b"})
            with self.assertLogs("scheduler_app.scheduler", level="INFO") as captured:
                count, executed, _ = _run_rounds(
                    [str(cfg_a), {"path": str(cfg_b), "after": ["a"]}],
                    scripts={"a": ["failed", "success"], "b": ["success"]},
                    quota=3,
                )
        self.assertEqual(
            executed, ["a", "a", "b"], "第 1 轮下游应跳过，第 2 轮上游恢复后触发",
        )
        self.assertEqual(count, 3)
        skip_logs = [msg for msg in captured.output if "跳过" in msg]
        self.assertTrue(skip_logs, "下游跳过应记 info 日志")
        self.assertIn("failed", skip_logs[0], "info 日志应说明依赖状态")

    def test_skipped_dependency_chains_skip(self):
        """a 失败 → b（after a）跳过 → c（after b）连锁跳过；下一轮依序恢复。"""
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_a = _write_cfg(d, "a", MINI_CFG)
            cfg_b = _write_cfg(d, "b", {**MINI_CFG, "name": "deps_b"})
            cfg_c = _write_cfg(d, "c", {**MINI_CFG, "name": "deps_c"})
            count, executed, _ = _run_rounds(
                [
                    str(cfg_a),
                    {"path": str(cfg_b), "after": ["a"]},
                    {"path": str(cfg_c), "after": ["b"]},
                ],
                scripts={"a": ["failed", "success"]},
                quota=4,
            )
        self.assertEqual(
            executed, ["a", "a", "b", "c"],
            "被跳过的依赖应连锁跳过下游，恢复后按拓扑序触发",
        )
        self.assertEqual(count, 4)

    def test_plain_str_entries_trigger_every_round(self):
        """纯 str 条目无 after：每轮照常触发，行为与 v0.8.0 一致（两轮 × 两条）。"""
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_a = _write_cfg(d, "a", MINI_CFG)
            cfg_b = _write_cfg(d, "b", {**MINI_CFG, "name": "deps_b"})
            count, executed, _ = _run_rounds(
                [str(cfg_a), str(cfg_b)], scripts={}, quota=4,
            )
        self.assertEqual(executed, ["a", "b", "a", "b"], "无依赖条目每轮按声明序触发")
        self.assertEqual(count, 4)


class ValidateTest(unittest.TestCase):
    """Task 3：启动期校验——未知依赖 warning 不 fail；成环抛 ConfigError。"""

    def test_unknown_dependency_warns_but_does_not_fail(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_a = _write_cfg(d, "a", MINI_CFG)
            cfg_b = _write_cfg(d, "b", {**MINI_CFG, "name": "deps_b"})
            entries = [str(cfg_a), {"path": str(cfg_b), "after": ["ghost"]}]
            with self.assertLogs("scheduler_app.scheduler", level="WARNING") as captured:
                count, executed, _ = _run_rounds(entries, scripts={}, quota=2)
        warnings = " ".join(captured.output)
        self.assertIn("ghost", warnings, "未知依赖名应记 warning")
        self.assertEqual(executed, ["a", "a"], "依赖永远无法满足，b 每轮跳过、从不执行")
        self.assertEqual(count, 2, "warning 不应中断调度")

    def test_after_cycle_raises_config_error_with_chain(self):
        from op_pipeline import ConfigError

        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_x = _write_cfg(d, "x", MINI_CFG)
            cfg_y = _write_cfg(d, "y", {**MINI_CFG, "name": "deps_y"})
            entries = [
                {"path": str(cfg_x), "after": ["y"]},
                {"path": str(cfg_y), "after": ["x"]},
            ]
            with self.assertRaises(ConfigError) as cm:
                _run_rounds(entries, scripts={}, quota=99)
        message = str(cm.exception)
        self.assertIn("循环引用", message)
        self.assertIn("x -> y -> x", message, "错误信息应含环链路")


class NormalizeAndCliTest(unittest.TestCase):
    """Task 3：条目归一化 + CLI ``--after`` 解析与合并。"""

    def test_normalize_accepts_str_path_and_dict_entries(self):
        from scheduler_app.scheduler import _normalize_pipelines

        entries = _normalize_pipelines(["a.json", {"path": "b.json", "after": ["a"]}])
        self.assertEqual([entry.name for entry in entries], ["a", "b"])
        self.assertEqual(entries[0].after, (), "str 条目等价于无依赖")
        self.assertEqual(entries[1].after, ("a",))

    def test_normalize_rejects_malformed_entries(self):
        from scheduler_app.scheduler import _normalize_pipelines

        with self.assertRaises(ValueError):
            _normalize_pipelines([{"after": ["a"]}])
        with self.assertRaises(ValueError):
            _normalize_pipelines([{"path": "b.json", "after": "a"}])
        with self.assertRaises(ValueError):
            _normalize_pipelines([42])

    def test_cli_after_specs_parse_repeatable_and_comma_separated(self):
        from scheduler_app.scheduler import _parse_after_specs

        parsed = _parse_after_specs(["b=a", "c=a,b", "b=x"])
        self.assertEqual(parsed, {"b": ["a", "x"], "c": ["a", "b"]},
                         "同名重复声明按顺序追加、逗号分隔拆分")
        with self.assertRaises(ValueError):
            _parse_after_specs(["noequals"])

    def test_cli_after_merges_into_dict_entry_declared_deps(self):
        from scheduler_app.scheduler import _normalize_pipelines

        entries = _normalize_pipelines(
            [{"path": "b.json", "after": ["a"]}, "c.json"],
            after={"b": ["x"]},
        )
        self.assertEqual(
            entries[0].after, ("a", "x"), "CLI 追加应排在条目自身声明之后",
        )
        self.assertEqual(entries[1].after, (), "未匹配到的条目不受影响")

    def test_parse_args_after_repeatable_with_default_empty(self):
        from scheduler_app.scheduler import parse_args

        args = parse_args([
            "--pipeline", "a.json", "--every", "5",
            "--after", "b=a", "--after", "c=a,b",
        ])
        self.assertEqual(args.after, ["b=a", "c=a,b"])
        args = parse_args(["--pipeline", "a.json", "--every", "1"])
        self.assertEqual(args.after, [], "未传 --after 时默认空列表")

    def test_cli_help_lists_after(self):
        proc = subprocess.run(
            [sys.executable, "-m", "scheduler_app.scheduler", "--help"],
            cwd=str(ROOT), capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("--after", proc.stdout, "--help 应列出 --after 及说明")


class EndToEndTest(unittest.TestCase):
    """Task 3：真实 JSON 配置 + RunStore 的端到端依赖触发。"""

    def test_downstream_runs_after_upstream_recovers(self):
        from op_pipeline import RunStore
        from scheduler_app import run_scheduler

        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            cfg_up = _write_cfg(d, "up", UP_CFG)
            cfg_down = _write_cfg(d, "down", DOWN_CFG)
            db = d / "runs.db"
            stop = threading.Event()
            results: list[int] = []
            errors: list[BaseException] = []

            def worker() -> None:
                try:
                    results.append(run_scheduler(
                        [str(cfg_up), {"path": str(cfg_down), "after": ["up"]}],
                        every=0.05, db=str(db), cache_dir=None, stop_event=stop,
                    ))
                except BaseException as exc:  # noqa: BLE001 - 测试需捕获所有异常
                    errors.append(exc)

            thread = threading.Thread(target=worker)
            thread.start()

            def _count(name: str, status: str | None = None) -> int:
                with RunStore(db) as store:
                    if status is None:
                        return store.query(
                            "SELECT COUNT(*) FROM runs WHERE pipeline = ?", (name,),
                        )[0][0]
                    return store.query(
                        "SELECT COUNT(*) FROM runs WHERE pipeline = ? AND status = ?",
                        (name, status),
                    )[0][0]

            try:
                self.assertTrue(
                    _wait(lambda: _count("dep_up") >= 1), "上游应先跑出（失败）记录",
                )
                self.assertEqual(
                    _count("dep_down"), 0, "上游未成功前下游不应触发",
                )
                (d / "dep_input.txt").write_text("hello", encoding="utf-8")
                self.assertTrue(
                    _wait(lambda: _count("dep_down") >= 1),
                    "上游恢复成功后下游应同轮触发",
                )
                self.assertGreaterEqual(
                    _count("dep_up", "success"), 1, "上游应有成功运行",
                )
            finally:
                stop.set()
                thread.join(timeout=10)
            self.assertFalse(thread.is_alive(), "stop_event 置位后调度线程应退出")
            self.assertEqual(errors, [])
            self.assertGreaterEqual(
                results[0], 2, "累计触发应含上游失败与成功两轮 + 下游 1 次",
            )


if __name__ == "__main__":
    unittest.main()
