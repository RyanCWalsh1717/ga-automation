"""
GA Automation — Health Check
=============================
Runs a suite of checks against the codebase and feedback log, then returns
a structured HealthReport.  Designed to be called by an on-demand agent session
or interactively from the command line.

Usage (standalone):
    python pipeline/health_check.py

Usage (from agent):
    from health_check import run_health_check
    report = run_health_check(repo_root='/path/to/ga-automation')
    print(report.summary())
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional


# ── Data structures ───────────────────────────────────────────

@dataclass
class CheckResult:
    name: str
    status: str          # 'pass' | 'warn' | 'fail'
    message: str
    detail: str = ''     # multi-line detail (stack trace, diff, etc.)


@dataclass
class FeedbackItem:
    submitted_at: str
    reporter: str
    property_code: str
    period: str
    severity: str        # 'low' | 'medium' | 'high' | 'critical'
    description: str
    status: str = 'open' # 'open' | 'acknowledged' | 'resolved'


@dataclass
class HealthReport:
    generated_at: str
    repo_root: str
    checks: List[CheckResult] = field(default_factory=list)
    feedback: List[FeedbackItem] = field(default_factory=list)

    # ── Derived counts ────────────────────────────────────────
    @property
    def n_pass(self)  -> int: return sum(1 for c in self.checks if c.status == 'pass')
    @property
    def n_warn(self)  -> int: return sum(1 for c in self.checks if c.status == 'warn')
    @property
    def n_fail(self)  -> int: return sum(1 for c in self.checks if c.status == 'fail')
    @property
    def n_open(self)  -> int: return sum(1 for f in self.feedback if f.status == 'open')
    @property
    def overall(self) -> str:
        if self.n_fail:   return 'FAIL'
        if self.n_warn:   return 'WARN'
        return 'PASS'

    def summary(self) -> str:
        lines = [
            f"=== GA Automation Health Check ===",
            f"Generated : {self.generated_at}",
            f"Repo      : {self.repo_root}",
            f"Overall   : {self.overall}",
            f"Checks    : {self.n_pass} pass  |  {self.n_warn} warn  |  {self.n_fail} fail",
            f"Feedback  : {self.n_open} open item(s)",
            "",
        ]
        for c in self.checks:
            icon = {'pass': '✅', 'warn': '⚠️', 'fail': '❌'}.get(c.status, '?')
            lines.append(f"  {icon}  {c.name}")
            lines.append(f"       {c.message}")
            if c.detail:
                for dl in c.detail.strip().splitlines():
                    lines.append(f"       {dl}")
            lines.append("")

        if self.feedback:
            lines.append("── Open Feedback Items ──────────────")
            for fb in self.feedback:
                if fb.status != 'open':
                    continue
                lines.append(
                    f"  [{fb.severity.upper()}] {fb.submitted_at}  "
                    f"{fb.reporter} ({fb.property_code} / {fb.period})"
                )
                lines.append(f"    {fb.description}")
                lines.append("")

        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            'generated_at': self.generated_at,
            'repo_root':    self.repo_root,
            'overall':      self.overall,
            'checks': [
                {'name': c.name, 'status': c.status,
                 'message': c.message, 'detail': c.detail}
                for c in self.checks
            ],
            'feedback': [
                {'submitted_at': f.submitted_at, 'reporter': f.reporter,
                 'property_code': f.property_code, 'period': f.period,
                 'severity': f.severity, 'description': f.description,
                 'status': f.status}
                for f in self.feedback
            ],
        }


# ── Individual checks ─────────────────────────────────────────

def _check_syntax(repo_root: str) -> CheckResult:
    """Parse every .py file under pipeline/ and app.py for syntax errors."""
    pipeline_dir = Path(repo_root) / 'pipeline'
    targets = list(pipeline_dir.rglob('*.py')) + [Path(repo_root) / 'app.py']
    errors = []
    for path in sorted(targets):
        try:
            src = path.read_text(encoding='utf-8')
            ast.parse(src)
        except SyntaxError as e:
            errors.append(f"{path.relative_to(repo_root)}  line {e.lineno}: {e.msg}")
        except Exception as e:
            errors.append(f"{path.relative_to(repo_root)}: {e}")

    if errors:
        return CheckResult(
            name='Syntax check',
            status='fail',
            message=f"{len(errors)} file(s) have syntax errors",
            detail='\n'.join(errors),
        )
    return CheckResult(
        name='Syntax check',
        status='pass',
        message=f"All {len(targets)} .py files parse cleanly",
    )


def _check_tests(repo_root: str) -> CheckResult:
    """Run test_may.py and capture pass/fail counts."""
    test_file = Path(repo_root) / 'test_may.py'
    if not test_file.exists():
        return CheckResult(
            name='Test suite',
            status='warn',
            message='test_may.py not found — skipped',
        )

    try:
        result = subprocess.run(
            [sys.executable, str(test_file)],
            capture_output=True, text=True,
            cwd=repo_root, timeout=120,
        )
        output = result.stdout + result.stderr

        # Parse summary line: "RESULTS:  22 passed  |  0 failed"
        import re
        m = re.search(r'(\d+)\s+passed.*?(\d+)\s+failed', output, re.IGNORECASE)
        if m:
            n_pass, n_fail = int(m.group(1)), int(m.group(2))
            if n_fail == 0:
                return CheckResult(
                    name='Test suite',
                    status='pass',
                    message=f"{n_pass} tests passed, 0 failed",
                )
            else:
                # Extract failed test lines for detail
                failed_lines = [
                    ln for ln in output.splitlines()
                    if 'FAIL' in ln and not ln.strip().startswith('RESULTS')
                ]
                return CheckResult(
                    name='Test suite',
                    status='fail',
                    message=f"{n_fail} test(s) failed ({n_pass} passed)",
                    detail='\n'.join(failed_lines[:20]),
                )
        else:
            # Couldn't parse — return raw output tail
            status = 'fail' if result.returncode != 0 else 'warn'
            return CheckResult(
                name='Test suite',
                status=status,
                message='Could not parse test output',
                detail=output[-1000:],
            )
    except subprocess.TimeoutExpired:
        return CheckResult(
            name='Test suite',
            status='warn',
            message='Test suite timed out after 120s',
        )
    except Exception as e:
        return CheckResult(
            name='Test suite',
            status='fail',
            message=f"Test runner error: {e}",
            detail=traceback.format_exc(),
        )


def _check_imports(repo_root: str) -> CheckResult:
    """Verify that key pipeline modules import without errors."""
    pipeline_dir = str(Path(repo_root) / 'pipeline')
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)

    modules = [
        'accrual_entry_generator',
        'management_fee',
        'prepaid_ledger',
        'bs_workpaper_generator',
        'qc_engine',
        'variance_comments',
        'je_verifier',
        'session_snapshot',
        'health_check',
    ]
    errors = []
    for mod in modules:
        try:
            __import__(mod)
        except Exception as e:
            errors.append(f"{mod}: {e}")

    if errors:
        return CheckResult(
            name='Module imports',
            status='fail',
            message=f"{len(errors)} module(s) failed to import",
            detail='\n'.join(errors),
        )
    return CheckResult(
        name='Module imports',
        status='pass',
        message=f"All {len(modules)} pipeline modules import cleanly",
    )


def _check_data_dir(repo_root: str) -> CheckResult:
    """Confirm data/ structure is intact for each configured property."""
    data_dir = Path(repo_root) / 'data'
    if not data_dir.exists():
        return CheckResult(
            name='Data directory',
            status='warn',
            message='data/ directory not found',
        )

    issues = []
    props = [d for d in data_dir.iterdir() if d.is_dir()]
    for prop in props:
        cfg = prop / 'config.yaml'
        if not cfg.exists():
            issues.append(f"{prop.name}: missing config.yaml")

    if issues:
        return CheckResult(
            name='Data directory',
            status='warn',
            message=f"{len(issues)} property folder(s) missing config.yaml",
            detail='\n'.join(issues),
        )
    return CheckResult(
        name='Data directory',
        status='pass',
        message=f"{len(props)} property folder(s) found, all have config.yaml",
    )


def _check_feedback_log(repo_root: str) -> tuple[CheckResult, list[FeedbackItem]]:
    """Read the feedback log and surface open items."""
    log_path = Path(repo_root) / 'data' / 'feedback_log.jsonl'
    items: list[FeedbackItem] = []

    if not log_path.exists():
        return (
            CheckResult(
                name='Feedback log',
                status='pass',
                message='No feedback log found — no issues reported yet',
            ),
            items,
        )

    try:
        for line in log_path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            items.append(FeedbackItem(
                submitted_at=d.get('submitted_at', ''),
                reporter=d.get('reporter', 'Unknown'),
                property_code=d.get('property_code', ''),
                period=d.get('period', ''),
                severity=d.get('severity', 'medium'),
                description=d.get('description', ''),
                status=d.get('status', 'open'),
            ))
    except Exception as e:
        return (
            CheckResult(
                name='Feedback log',
                status='warn',
                message=f"Could not parse feedback log: {e}",
            ),
            items,
        )

    open_items  = [i for i in items if i.status == 'open']
    critical    = [i for i in open_items if i.severity == 'critical']
    high        = [i for i in open_items if i.severity == 'high']

    if critical:
        status = 'fail'
        msg = f"{len(critical)} CRITICAL + {len(open_items) - len(critical)} other open item(s)"
    elif high:
        status = 'warn'
        msg = f"{len(high)} high-severity + {len(open_items) - len(high)} other open item(s)"
    elif open_items:
        status = 'warn'
        msg = f"{len(open_items)} open feedback item(s)"
    else:
        status = 'pass'
        msg = f"{len(items)} total item(s), none open"

    return (
        CheckResult(name='Feedback log', status=status, message=msg),
        items,
    )


# ── Main entry point ──────────────────────────────────────────

def run_health_check(repo_root: Optional[str] = None) -> HealthReport:
    """
    Run all checks and return a HealthReport.

    Args:
        repo_root: Path to the ga-automation repo root.
                   Defaults to two levels up from this file (pipeline/../).
    """
    if repo_root is None:
        repo_root = str(Path(__file__).parent.parent)

    report = HealthReport(
        generated_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        repo_root=repo_root,
    )

    report.checks.append(_check_syntax(repo_root))
    report.checks.append(_check_tests(repo_root))
    report.checks.append(_check_imports(repo_root))
    report.checks.append(_check_data_dir(repo_root))

    feedback_check, feedback_items = _check_feedback_log(repo_root)
    report.checks.append(feedback_check)
    report.feedback = feedback_items

    return report


def save_report(report: HealthReport, repo_root: str) -> str:
    """Write the report as JSON to data/health_reports/ and return the path."""
    out_dir = Path(repo_root) / 'data' / 'health_reports'
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = out_dir / f'health_{ts}.json'
    out_path.write_text(
        json.dumps(report.to_dict(), indent=2),
        encoding='utf-8',
    )
    return str(out_path)


# ── CLI ───────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='GA Automation health check')
    parser.add_argument('--root', default=None, help='Repo root path')
    parser.add_argument('--save', action='store_true', help='Save report JSON to data/health_reports/')
    args = parser.parse_args()

    report = run_health_check(repo_root=args.root)
    # Print safely — Windows terminals may not support emoji; fall back to ASCII
    try:
        print(report.summary())
    except UnicodeEncodeError:
        print(report.summary()
              .replace('✅', '[PASS]')
              .replace('⚠️', '[WARN]')
              .replace('❌', '[FAIL]'))

    if args.save:
        path = save_report(report, args.root or str(Path(__file__).parent.parent))
        print(f"\nReport saved → {path}")

    sys.exit(0 if report.overall == 'PASS' else 1)
