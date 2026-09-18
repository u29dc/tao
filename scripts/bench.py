#!/usr/bin/env python3
"""Validated direct-process benchmarks. No shell calibration or implied cache modes."""

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from support import CARGO, ROOT, existing_vault, repo_output, script_arguments, sha256

from fixtures import generate, validate

NOTE = 'notes/projects/project-1.md'
BASE = 'views/projects.base'
SDK_SUITES = (
    'sdk',
    'core',
    'all',
    'parse',
    'resolve',
    'search',
    'graph-walk',
    'unified-query',
    'bridge',
    'startup',
)


def cases():
    """Argument vectors keep paths/expressions literal, including spaces and metacharacters."""
    return {
        'vault-preflight': ['vault', 'preflight'],
        'health': ['health'],
        'doc-read': ['doc', 'read', '--path', NOTE],
        'doc-list': ['doc', 'list'],
        'base-list': ['base', 'list'],
        'base-schema': ['base', 'schema', '--path-or-id', BASE],
        'base-view': [
            'base',
            'view',
            '--path-or-id',
            BASE,
            '--view-name',
            'Projects',
            '--page',
            '1',
            '--page-size',
            '50',
        ],
        'graph-links': ['graph', 'links', '--path', NOTE],
        'graph-links-outgoing': ['graph', 'links', '--direction', 'outgoing', '--path', NOTE],
        'graph-links-incoming': ['graph', 'links', '--direction', 'incoming', '--path', NOTE],
        'graph-audit-unresolved': ['graph', 'audit', '--kind', 'unresolved', '--limit', '50'],
        'graph-audit-deadends': ['graph', 'audit', '--kind', 'deadends', '--limit', '50'],
        'graph-audit-orphans': ['graph', 'audit', '--kind', 'orphans', '--limit', '50'],
        'graph-audit-components': ['graph', 'audit', '--kind', 'components', '--limit', '50'],
        'graph-audit-components-strong': [
            'graph',
            'audit',
            '--kind',
            'components',
            '--mode',
            'strong',
            '--limit',
            '50',
        ],
        'graph-audit-floating': ['graph', 'audit', '--kind', 'floating', '--limit', '50'],
        'graph-audit-inbound-scope': [
            'graph',
            'audit',
            '--kind',
            'inbound-scope',
            '--scope',
            'notes/projects',
            '--include-markdown',
            '--limit',
            '50',
        ],
        'graph-path': [
            'graph',
            'path',
            '--from',
            NOTE,
            '--to',
            'notes/projects/project-2.md',
            '--max-depth',
            '8',
            '--max-nodes',
            '10000',
        ],
        'graph-walk': ['graph', 'walk', '--path', NOTE, '--depth', '2', '--limit', '200'],
        'graph-walk-folders': [
            'graph',
            'walk',
            '--path',
            NOTE,
            '--depth',
            '2',
            '--limit',
            '200',
            '--include-folders',
        ],
        'meta-properties': ['meta', 'properties', '--limit', '100'],
        'meta-tags': ['meta', 'tags', '--limit', '100'],
        'meta-aliases': ['meta', 'aliases', '--limit', '100'],
        'task-list': ['task', 'list', '--limit', '100'],
        'search-project': ['search', 'project', '--limit', '50'],
        'search-project-context': [
            'search',
            'project',
            '--context',
            '--depth',
            '2',
            '--limit',
            '20',
        ],
        'search-project-path-context': [
            'search',
            '--path',
            NOTE,
            '--context',
            '--depth',
            '2',
            '--limit',
            '20',
        ],
        'search-no-result': ['search', 'zqxnonexistentfixturetokenqzx', '--limit', '20'],
        'search-files': ['search', 'project', '--kind', 'files', '--ext', 'md', '--limit', '20'],
        'query-docs': ['query', '--from', 'docs', '--query', 'project', '--limit', '50'],
        'query-docs-where': [
            'query',
            '--from',
            'docs',
            '--query',
            'project',
            '--where',
            "matched_in contains 'content' or matched_in contains 'title' or matched_in contains 'path'",
            '--limit',
            '50',
        ],
        'query-docs-sort': [
            'query',
            '--from',
            'docs',
            '--query',
            'project',
            '--sort',
            'title:asc',
            '--select',
            'path',
            '--limit',
            '50',
        ],
        'query-docs-stream': [
            '--json-stream',
            'query',
            '--from',
            'docs',
            '--query',
            'project',
            '--select',
            'path',
            '--limit',
            '50',
        ],
        'query-graph': ['query', '--from', 'graph', '--limit', '50'],
        'query-graph-path': ['query', '--from', 'graph', '--path', NOTE, '--limit', '50'],
        'query-task': ['query', '--from', 'task', '--query', 'Follow', '--limit', '50'],
        'query-meta-tags': ['query', '--from', 'meta:tags', '--limit', '50'],
        'query-meta-aliases': ['query', '--from', 'meta:aliases', '--limit', '50'],
        'query-meta-properties': ['query', '--from', 'meta:properties', '--limit', '50'],
        'query-base': [
            'query',
            '--from',
            'base:' + BASE,
            '--view-name',
            'Projects',
            '--limit',
            '50',
        ],
    }


BUDGET_CASES = (
    'query-docs',
    'query-base',
    'query-graph-path',
    'graph-links',
    'graph-path',
    'graph-walk',
    'search-project-context',
    'meta-tags',
)


def finite_positive(value, label):
    if (
        isinstance(value, bool)
        or not isinstance(value, (float, int))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f'{label} must be a finite positive number')
    return float(value)


def load_budgets(path, override=None, profile='5k', mode='daemon-hit', variant='tiny'):
    default = 10.0
    configured = {}
    if path and not path.is_file():
        raise ValueError(f'budget configuration does not exist: {path}')
    if path:
        raw = json.loads(path.read_text())
        if not isinstance(raw, dict):
            raise ValueError('budget configuration must be an object')
        if raw.get('schema_version') == 2:
            allowed = {'schema_version', 'profile', 'mode', 'variant', 'baseline', 'profiles'}
            if set(raw) - allowed or raw.get('variant') != variant:
                raise ValueError('unknown versioned budget keys or unmeasured fixture variant')
            if raw.get('profile') not in ('1k', '5k') or raw.get('mode') not in (
                'direct',
                'daemon-hit',
                'daemon-miss',
            ):
                raise ValueError('invalid default budget workload')
            if not isinstance(raw.get('baseline'), dict) or not raw['baseline']:
                raise ValueError('versioned budgets require documented baseline provenance')
            profiles = raw.get('profiles')
            if not isinstance(profiles, dict) or not profiles or set(profiles) - {'1k', '5k'}:
                raise ValueError('invalid budget profiles')
            for profile_name, modes in profiles.items():
                if (
                    not isinstance(modes, dict)
                    or not modes
                    or set(modes) - {'direct', 'daemon-hit', 'daemon-miss'}
                ):
                    raise ValueError(f'invalid budget modes for {profile_name}')
                for mode_name, workload in modes.items():
                    if not isinstance(workload, dict) or set(workload) != {
                        'baseline_p50_ms',
                        'case_budgets_p50_ms',
                    }:
                        raise ValueError(f'invalid budget workload {profile_name}/{mode_name}')
                    for field in ('baseline_p50_ms', 'case_budgets_p50_ms'):
                        if not isinstance(workload[field], dict) or set(workload[field]) != set(
                            BUDGET_CASES
                        ):
                            raise ValueError('versioned budgets require every exact case ID')
                        for key, value in workload[field].items():
                            finite_positive(value, f'{profile_name}/{mode_name}/{field}/{key}')
                    if any(
                        workload['case_budgets_p50_ms'][key] < workload['baseline_p50_ms'][key]
                        for key in BUDGET_CASES
                    ):
                        raise ValueError(
                            'a regression budget cannot be below its recorded baseline'
                        )
            if profile not in profiles or mode not in profiles[profile]:
                raise ValueError(f'no measured budget for {profile}/{mode}')
            selected = profiles[profile][mode]['case_budgets_p50_ms']
            if override is not None:
                selected = {name: finite_positive(override, '--budget-ms') for name in BUDGET_CASES}
            return selected
        allowed = {
            'profile',
            'warm_read_p50_ms',
            'graph_read_p50_ms',
            'base_read_p50_ms',
            'query_read_p50_ms',
            'case_budgets_p50_ms',
        }
        if set(raw) - allowed:
            raise ValueError(f'unknown budget keys: {sorted(set(raw) - allowed)}')
        if 'profile' in raw and raw['profile'] not in ('1k', '5k'):
            raise ValueError('budget profile must be 1k or 5k')
        default = finite_positive(raw.get('warm_read_p50_ms', default), 'warm_read_p50_ms')
        for name in BUDGET_CASES:
            family = (
                'base'
                if name == 'query-base'
                else ('graph' if 'graph' in name else 'query' if name == 'query-docs' else 'warm')
            )
            configured[name] = finite_positive(raw.get(f'{family}_read_p50_ms', default), name)
        individual = raw.get('case_budgets_p50_ms', {})
        if not isinstance(individual, dict) or set(individual) - set(BUDGET_CASES):
            raise ValueError('case budgets must name the exact supported budget cases')
        for name, value in individual.items():
            configured[name] = finite_positive(value, name)
    if override is not None:
        default = finite_positive(override, '--budget-ms')
        configured = {}
    return {name: configured.get(name, default) for name in BUDGET_CASES}


def summarize(samples):
    if not samples:
        raise ValueError('benchmark has no samples')
    values = sorted(finite_positive(value, 'latency sample') for value in samples)

    def percentile(p):
        return values[max(0, math.ceil(len(values) * p) - 1)]

    return {
        'mean_ms': statistics.mean(values),
        'p50_ms': statistics.median(values),
        'p95_ms': percentile(0.95) if len(values) >= 20 else None,
        'p95_sample_sufficient': len(values) >= 20,
        'min_ms': values[0],
        'max_ms': values[-1],
        'stddev_ms': statistics.stdev(values) if len(values) > 1 else 0.0,
        'samples_ms': samples,
    }


def check_budgets(results, budgets):
    ids = [row['id'] for row in results]
    if len(ids) != len(set(ids)) or set(ids) != set(budgets):
        raise ValueError('budget result cases missing, unexpected or duplicated')
    checked = []
    for row in results:
        summary = summarize(row['samples_ms'])
        checked.append(
            {
                **row,
                **summary,
                'budget_p50_ms': budgets[row['id']],
                'pass': summary['p50_ms'] <= budgets[row['id']],
            }
        )
    return checked


def validate_budget_fixture(configured, fixture):
    if configured.get('schema_version') == 2:
        baseline = configured['baseline']
        expected_hash = baseline.get('fixture_sha256', {}).get(fixture['profile'])
        if baseline.get('fixture_seed') != fixture['seed'] or expected_hash != fixture['sha256']:
            raise ValueError(
                'budget baseline does not match the fixture seed/content hash; collect a new baseline'
            )


def envelope(raw):
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get('ok') is not True or 'data' not in value:
        raise ValueError('command did not return a successful data envelope')
    return value


def paths_in(value):
    found = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key in (
                'path',
                'file_path',
                'source_path',
                'target_path',
                'resolved_path',
            ) and isinstance(child, str):
                found.append(child)
            else:
                found.extend(paths_in(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(paths_in(child))
    return found


def validate_project_view(value, fixture):
    data = value['data']
    expected = sorted(
        (row for row in fixture['expected_projects'] if row['status'] != 'archived'),
        key=lambda row: (-row['priority'], row['path']),
    )
    if data.get('total') != len(expected):
        raise ValueError('Projects base total disagrees with independently expected membership')
    rows = data.get('rows', [])
    if [row.get('file_path') for row in rows] != [row['path'] for row in expected[:50]]:
        raise ValueError('Projects base selected rows or priority ordering disagree with fixture')
    for actual, planned in zip(rows, expected):
        if any(actual.get('values', {}).get(key) != planned[key] for key in ('status', 'priority')):
            raise ValueError('Projects base projected values disagree with fixture')


def validate_answer(case_id, value, fixture):
    data = value['data']
    if not isinstance(data, (dict, list)):
        raise ValueError('expected structured result data')
    if case_id in ('search-project', 'query-docs', 'search-project-context') and fixture:
        if not paths_in(data):
            raise ValueError(f'{case_id}: expected fixture matches, received no paths')
    if case_id == 'search-no-result' and (data.get('total') != 0 or data.get('candidates')):
        raise ValueError('negative search returned fixture paths')
    if case_id == 'doc-read' and fixture and NOTE not in paths_in(data):
        raise ValueError('exact document read did not identify the expected source')
    if case_id == 'base-view' and fixture:
        validate_project_view(value, fixture)
    if case_id == 'graph-path' and fixture:
        if data.get('found') is not True or data.get('path') != [
            NOTE,
            'notes/projects/project-2.md',
        ]:
            raise ValueError(
                'graph path did not preserve the independently expected direct relationship'
            )
    if (
        case_id
        in (
            'graph-walk',
            'graph-walk-folders',
            'graph-links',
            'graph-links-outgoing',
            'graph-links-incoming',
            'query-graph-path',
        )
        and fixture
    ):
        if not paths_in(data):
            raise ValueError(f'{case_id}: expected graph relationships, received no paths')
    if (
        case_id == 'task-list'
        and fixture
        and data.get('total') != sum(fixture['expected_tasks_by_state'].values())
    ):
        raise ValueError('task list total differs from independent fixture expectation')
    result_paths = (
        paths_in(data.get('candidates'))
        if isinstance(data, dict) and 'candidates' in data
        else paths_in(data)
    )
    return hashlib.sha256(json.dumps(result_paths, separators=(',', ':')).encode()).hexdigest()


def run_json(argv, env):
    result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, check=True, timeout=180)
    return envelope(result.stdout)


def benchmark_environment(report, temporary):
    # Ambient application state must not redirect test writes or change the workload.
    env = {key: value for key, value in os.environ.items() if not key.startswith('TAO_')}
    env.update(
        TMPDIR=str(temporary),
        TAO_CONFIG_PATH=str(report / 'absent-global-config.toml'),
        TAO_DATA_DIR=str(report / 'runtime-state'),
        TAO_CASE_POLICY='sensitive',
    )
    return env


def command(binary, operation, vault, database, mode, socket):
    args = [str(binary), '--execution-mode', 'direct' if mode == 'direct' else 'required-daemon']
    if mode != 'direct':
        args += ['--daemon-socket', str(socket)]
    if mode == 'daemon-miss':
        args += ['--no-result-cache']
    return args + operation + ['--vault-root', str(vault), '--db-path', str(database)]


def metadata():
    def output(argv):
        try:
            return subprocess.check_output(
                argv, cwd=ROOT, text=True, stderr=subprocess.DEVNULL
            ).strip()
        except (OSError, subprocess.SubprocessError):
            return None

    diff = subprocess.check_output(['git', 'diff', 'HEAD', '--binary'], cwd=ROOT)
    return {
        'commit': output(['git', 'rev-parse', 'HEAD']),
        'tracked_diff_sha256': hashlib.sha256(diff).hexdigest(),
        'git_status': output(['git', 'status', '--porcelain']),
        'rustc': output(['rustc', '--version']),
        'os': platform.platform(),
        'machine': platform.machine(),
        'cpu_count': os.cpu_count(),
        'python': platform.python_version(),
        'python_sqlite': sqlite3.sqlite_version,
        'clock': 'perf_counter_ns, process wall time, no shell subtraction',
    }


def validate_runtime(value, mode):
    runtime = value.get('meta', {}).get('runtime')
    expected_backend = 'direct' if mode == 'direct' else 'daemon'
    expected_cache = {'direct': 'bypass', 'daemon-hit': 'hit', 'daemon-miss': 'bypass'}[mode]
    if not isinstance(runtime, dict) or runtime.get('backend') != expected_backend:
        raise ValueError(f'benchmark backend was not confirmed as {expected_backend}')
    if runtime.get('resultCache') != expected_cache:
        raise ValueError(
            f'benchmark cache state was {runtime.get("resultCache")}, expected {expected_cache}'
        )
    return runtime


def resource_probe(argv, env, timeout, daemon_pid=None):
    """Separate resource probe avoids including instrumentation in latency samples."""
    timer = Path('/usr/bin/time')
    if not timer.exists():
        return {
            'client_peak_rss_bytes': None,
            'daemon_rss_after_probe_bytes': None,
            'reason': 'platform resource probe unavailable',
        }
    options = ['-l'] if sys.platform == 'darwin' else ['-f', 'TAO_MAX_RSS_KIB=%M']
    result = subprocess.run(
        [str(timer), *options, *argv],
        cwd=ROOT,
        env=env,
        capture_output=True,
        check=True,
        timeout=timeout,
    )
    envelope(result.stdout)
    diagnostic = result.stderr.decode('utf-8', errors='replace')
    pattern = (
        r'(\d+)\s+maximum resident set size'
        if sys.platform == 'darwin'
        else r'TAO_MAX_RSS_KIB=(\d+)'
    )
    matched = re.search(pattern, diagnostic)
    if not matched:
        raise ValueError('resource probe completed without a readable RSS measurement')
    client = int(matched.group(1)) * (1 if sys.platform == 'darwin' else 1024)
    daemon = None
    if daemon_pid:
        measured = subprocess.run(
            ['ps', '-o', 'rss=', '-p', str(daemon_pid)],
            text=True,
            capture_output=True,
            check=True,
            timeout=10,
        )
        daemon = int(measured.stdout.strip()) * 1024
    return {
        'client_peak_rss_bytes': client,
        'daemon_rss_after_probe_bytes': daemon,
        'daemon_measurement': 'snapshot after separate probe; not peak RSS',
    }


def measure(case_id, argv, args, env, fixture, prepare=None):
    # Validate each invocation, not just the final exit in a shell loop.
    samples, answer_hashes, output_bytes = [], set(), []
    runtime = []
    for index in range(args.warmup + args.runs):
        if prepare:
            prepare()
        start = time.perf_counter_ns()
        result = subprocess.run(
            argv, cwd=ROOT, env=env, capture_output=True, check=True, timeout=args.timeout_seconds
        )
        elapsed = (time.perf_counter_ns() - start) / 1_000_000
        value = envelope(result.stdout)
        identity = validate_answer(case_id, value, fixture)
        if index >= args.warmup:
            samples.append(elapsed)
            answer_hashes.add(identity)
            output_bytes.append(len(result.stdout))
            runtime.append(validate_runtime(value, args.mode))
            if index == args.warmup:
                answer_path = args.report_directory / f'{case_id}-{args.mode}-answer.json'
                answer_path.write_bytes(result.stdout)
    if len(answer_hashes) != 1:
        raise ValueError(f'{case_id}: result source set changed between measured iterations')
    if prepare:
        prepare()
    return {
        'id': case_id,
        'argv': argv,
        'mode': args.mode,
        'warmup': args.warmup,
        'runs': args.runs,
        'result_source_sha256': next(iter(answer_hashes)),
        'result_validated': True,
        'validated_answer_file': answer_path.name,
        'output_bytes': output_bytes,
        'runtime_observations': runtime,
        'memory': resource_probe(
            argv,
            env,
            args.timeout_seconds,
            getattr(args, 'daemon_pid', None) if args.mode != 'direct' else None,
        ),
        **summarize(samples),
    }


def sdk_scenarios(binary, vault, database, report, selected, env):
    scenarios = (
        ['parse', 'resolve', 'search', 'graph-walk', 'unified-query', 'bridge', 'startup']
        if selected in ('all', 'sdk', 'core')
        else [selected]
    )
    for name in scenarios:
        argv = [
            str(binary),
            '--scenario',
            name,
            '--iterations',
            '100',
            '--json-out',
            str(report / f'{name}-sdk.json'),
        ]
        if name in ('parse', 'search', 'graph-walk', 'unified-query'):
            argv += ['--vault-root', str(vault), '--db-path', str(database)]
        if name in ('bridge', 'startup', 'resolve'):
            argv += ['--bridge-notes', '1000']
        subprocess.run(argv, cwd=ROOT, env=env, check=True, timeout=600)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--suite',
        choices=(
            'all',
            'sdk',
            'core',
            'cli',
            'live',
            'drift',
            'fixtures',
            'daemon',
            'parse',
            'resolve',
            'search',
            'bridge',
            'startup',
            'graph-walk',
            'unified-query',
            'budget',
            'relevance',
        ),
        default='all',
    )
    parser.add_argument('--profile', choices=('1k', '5k'))
    parser.add_argument('--variant', choices=('tiny', 'representative'))
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--runs', type=int, default=25)
    parser.add_argument('--warmup', type=int, default=5)
    parser.add_argument('--timeout-seconds', type=int, default=180)
    parser.add_argument('--output', type=Path, default=ROOT / '.benchmarks/reports')
    parser.add_argument('--mode', choices=('direct', 'daemon-hit', 'daemon-miss'))
    parser.add_argument('--skip-generate', action='store_true')
    parser.add_argument('--fixture-vault', type=Path)
    parser.add_argument('--skip-build', action='store_true')
    parser.add_argument('--live-vault', type=Path, default=os.environ.get('TAO_BENCH_LIVE_VAULT'))
    parser.add_argument('--live-commands', type=Path)
    parser.add_argument('--budget-config', type=Path)
    parser.add_argument('--budget-ms', type=float)
    args = parser.parse_args(script_arguments())
    if args.fixture_vault and not args.skip_generate:
        parser.error('--fixture-vault requires --skip-generate')
    if args.live_commands and args.suite != 'live':
        parser.error('--live-commands requires --suite live')
    if (args.budget_config or args.budget_ms is not None) and args.suite != 'budget':
        parser.error('budget options require the budget suite')
    if args.runs < 1 or args.warmup < 0 or args.seed < 0 or args.timeout_seconds <= 0:
        parser.error('runs/timeout must be positive; warmup/seed nonnegative')
    if args.suite == 'budget' and args.runs < 20:
        parser.error('budget gates require at least 20 measured samples per case')
    default_budget_config = ROOT / '.benchmarks/budgets.json'
    if args.budget_config is None and default_budget_config.is_file():
        args.budget_config = default_budget_config
    configured = (
        json.loads(args.budget_config.read_text())
        if args.suite == 'budget' and args.budget_config and args.budget_config.is_file()
        else {}
    )
    if not isinstance(configured, dict):
        raise ValueError('budget configuration must be an object')
    args.profile = args.profile or configured.get('profile', '5k')
    args.mode = args.mode or configured.get('mode', 'daemon-hit')
    args.variant = args.variant or configured.get('variant', 'tiny')
    budgets = (
        load_budgets(args.budget_config, args.budget_ms, args.profile, args.mode, args.variant)
        if args.suite == 'budget'
        else None
    )
    if (
        args.suite == 'budget'
        and not args.budget_config
        and args.budget_ms is None
        and args.mode != 'daemon-hit'
    ):
        parser.error('uncached budgets require measured configuration or explicit --budget-ms')
    report = repo_output(args.output) / (
        datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
    )
    report.mkdir(parents=True)
    args.report_directory = report
    target = repo_output(ROOT / 'target')
    target.mkdir(exist_ok=True)
    binary, bench = target / 'release/tao', target / 'release/tao-bench'
    needs_cli = args.suite != 'fixtures'
    needs_sdk = args.suite in SDK_SUITES
    if not args.skip_build and needs_cli:
        build = [str(CARGO), 'build', '--locked', '--release', '-p', 'tao-cli']
        if needs_sdk:
            build += ['-p', 'tao-bench']
        subprocess.run([*build, '--target-dir', str(target)], cwd=ROOT, check=True)
    # A concurrent developer build cannot swap binaries between initialization and reads.
    artifacts = report / 'bin'
    artifacts.mkdir()
    sources = ([binary] if needs_cli else []) + ([bench] if needs_sdk else [])
    for source in sources:
        shutil.copy2(source, artifacts / source.name)
    binary, bench = artifacts / binary.name, artifacts / bench.name
    executable_identity = {path.name: sha256(artifacts / path.name) for path in sources}
    environment_identity = metadata()
    temp = tempfile.TemporaryDirectory(prefix='tao-bench-', dir=target)
    env = benchmark_environment(report, temp.name)
    socket = Path(temp.name) / 'daemon.sock'
    started = False
    rows = []
    initialization = {}
    daemon_before = None
    daemon_after = None
    try:
        if args.suite == 'relevance':
            from relevance import run_relevance

            run_relevance(binary, report, env)
            return
        if args.suite == 'fixtures':
            for profile in ('1k', '5k'):
                times = []
                for _ in range(args.runs):
                    start = time.perf_counter_ns()
                    generate(
                        report / 'fixtures',
                        profile,
                        int(profile[:-1]) * 1000,
                        args.seed,
                        args.variant,
                    )
                    times.append((time.perf_counter_ns() - start) / 1_000_000)
                rows.append({'id': f'fixture-{profile}', **summarize(times)})
            fixture = None
        else:
            if args.suite == 'live':
                if not args.live_vault:
                    parser.error(
                        'live suite requires explicit --live-vault or TAO_BENCH_LIVE_VAULT'
                    )
                vault = existing_vault(args.live_vault)
                fixture = None
                database = report / 'live-index.sqlite'
            else:
                if args.skip_generate:
                    vault = repo_output(
                        args.fixture_vault
                        or ROOT
                        / 'target/fixtures'
                        / (
                            f'vault-{args.profile}'
                            + ('-representative' if args.variant == 'representative' else '')
                        )
                    )
                    fixture = validate(vault)
                    if (fixture.get('seed'), fixture.get('variant'), fixture.get('profile')) != (
                        args.seed,
                        args.variant,
                        args.profile,
                    ):
                        raise ValueError(
                            'reused fixture seed/profile/variant differs from requested workload'
                        )
                else:
                    vault = generate(
                        report / 'fixtures',
                        args.profile,
                        int(args.profile[:-1]) * 1000,
                        args.seed,
                        args.variant,
                    )
                    fixture = validate(vault)
                # Independent index and socket avoid colliding with another run's state.
                database = report / 'index.sqlite'
                if budgets:
                    validate_budget_fixture(configured, fixture)
            refresh = ['vault', 'reindex']
            if fixture:
                refresh += ['--wait-content-ms', '60000']
            for name, operation in (('vault.open', ['vault', 'open']), ('vault.reindex', refresh)):
                initialization[name] = run_json(
                    command(binary, operation, vault, database, 'direct', socket), env
                )
            if fixture:
                validate(vault, database)
                value = run_json(
                    command(binary, cases()['base-view'], vault, database, 'direct', socket), env
                )
                validate_project_view(value, fixture)
            if needs_sdk:
                sdk_scenarios(bench, vault, database, report, args.suite, env)
            if args.suite in ('cli', 'all', 'daemon', 'drift', 'budget', 'live'):
                if args.mode != 'direct' or args.suite == 'daemon':
                    start_result = run_json(
                        [str(binary), 'vault', 'daemon', 'start', '--socket', str(socket)], env
                    )
                    args.daemon_pid = start_result['data'].get('pid')
                    started = True
                    # Establish the daemon's watcher/publication boundary before
                    # priming cache hits. Otherwise its first background refresh
                    # can invalidate a measured sample on larger fixtures.
                    initialization['daemon.reindex'] = run_json(
                        command(binary, refresh, vault, database, 'daemon-miss', socket), env
                    )
                    daemon_before = run_json(
                        [str(binary), 'vault', 'daemon', 'status', '--socket', str(socket)], env
                    )
                matrix = cases()
                if args.suite == 'budget':
                    matrix = {name: matrix[name] for name in BUDGET_CASES}
                elif args.suite == 'daemon':
                    matrix = {
                        'query-docs': matrix['query-docs'],
                        'search-project': matrix['search-project'],
                    }
                elif args.suite == 'live':
                    matrix = {
                        'health': ['health'],
                        'vault-preflight': ['vault', 'preflight'],
                        'vault-reindex-dry-run': ['vault', 'reindex', '--dry-run'],
                        'search-project': ['search', 'project', '--limit', '20'],
                        'graph-audit-unresolved': matrix['graph-audit-unresolved'],
                    }
                    if args.live_commands:
                        for line in args.live_commands.read_text().splitlines():
                            if not line.strip() or line.startswith('#'):
                                continue
                            entry = json.loads(line)
                            if not isinstance(entry.get('args'), list) or not all(
                                isinstance(v, str) for v in entry['args']
                            ):
                                raise ValueError(
                                    'live command JSONL requires id and string args array'
                                )
                            name = entry['id']
                            if (
                                name in matrix
                                or not name.replace('-', '').replace('_', '').isalnum()
                            ):
                                raise ValueError('invalid or duplicate live case ID')
                            matrix[name] = entry['args']
                elif args.suite == 'drift':
                    matrix = {'single-file-drift': ['vault', 'reindex']}
                original_mode = args.mode
                modes = (
                    ('direct', 'daemon-hit', 'daemon-miss')
                    if args.suite == 'daemon'
                    else (args.mode,)
                )
                for mode in modes:
                    args.mode = mode
                    for name, operation in matrix.items():
                        case_mode = (
                            'daemon-miss'
                            if mode == 'daemon-hit'
                            and (
                                operation[0] in ('health', 'validate', 'vault')
                                or '--json-stream' in operation
                            )
                            else mode
                        )
                        args.mode = case_mode
                        print(f'Benchmark {name} ({case_mode})', flush=True)
                        argv = command(binary, operation, vault, database, case_mode, socket)
                        if args.suite == 'drift':
                            note = vault / NOTE
                            original = note.read_bytes()

                            def prepare():
                                note.write_bytes(original)
                                run_json(
                                    command(
                                        binary,
                                        ['vault', 'reindex'],
                                        vault,
                                        database,
                                        'direct',
                                        socket,
                                    ),
                                    env,
                                )
                                note.write_bytes(original + b'\nDrift benchmark evidence.\n')

                            try:
                                row = measure(name, argv, args, env, fixture, prepare)
                            finally:
                                note.write_bytes(original)
                                run_json(
                                    command(
                                        binary,
                                        ['vault', 'reindex'],
                                        vault,
                                        database,
                                        'direct',
                                        socket,
                                    ),
                                    env,
                                )
                        else:
                            # A cache-hit workload explicitly primes before warmup/timing.
                            if case_mode == 'daemon-hit':
                                run_json(argv, env)
                            row = measure(name, argv, args, env, fixture)
                        if args.suite == 'daemon':
                            row['id'] += '-' + mode
                        rows.append(row)
                        (report / (row['id'] + '.json')).write_text(
                            json.dumps(row, indent=2) + '\n'
                        )
                args.mode = original_mode
        if args.suite == 'daemon':
            compared = {}
            for row in rows:
                case_id = row['id'].removesuffix('-' + row['mode'])
                compared.setdefault(case_id, set()).add(row['result_source_sha256'])
            if any(len(values) != 1 for values in compared.values()):
                raise ValueError(
                    'direct and daemon modes returned different result source ordering'
                )
        status = 'pass'
        if started:
            daemon_after = run_json(
                [str(binary), 'vault', 'daemon', 'status', '--socket', str(socket)], env
            )
        if budgets:
            rows = check_budgets(rows, budgets)
            if not all(row['pass'] for row in rows):
                status = 'fail'
        summary = {
            'schema_version': 2,
            'status': status,
            'suite': args.suite,
            'mode': args.mode,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'environment': environment_identity,
            'executable_sha256': executable_identity,
            'initialization': initialization,
            'daemon_status_before': daemon_before,
            'daemon_status_after': daemon_after,
            'configuration': {key: value for key, value in env.items() if key.startswith('TAO_')},
            'budget_configuration_sha256': sha256(args.budget_config)
            if budgets and args.budget_config
            else None,
            'fixture': fixture,
            'commands': rows,
            'memory_scope': 'separate client peak probe and daemon snapshot, in bytes',
        }
        if args.suite != 'fixtures':
            summary['storage_bytes_before_shutdown'] = {
                suffix or 'database': Path(str(database) + suffix).stat().st_size
                if Path(str(database) + suffix).exists()
                else 0
                for suffix in ('', '-wal', '-shm')
            }
        (report / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(f'Reports: {report}')
        if status != 'pass':
            raise ValueError('one or more validated cases exceeded their configured p50 budget')
    finally:
        if started:
            try:
                subprocess.run(
                    [str(binary), 'vault', 'daemon', 'stop', '--socket', str(socket)],
                    cwd=ROOT,
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=20,
                    check=False,
                )
            except (OSError, subprocess.SubprocessError) as error:
                print(f'daemon cleanup failed: {error}', file=sys.stderr)
        temp.cleanup()


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, sqlite3.Error, subprocess.SubprocessError) as error:
        print(f'benchmark failed: {error}', file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError):
            for captured in (error.stdout, error.stderr):
                if captured:
                    print(
                        captured.decode('utf-8', errors='replace')[-4000:]
                        if isinstance(captured, bytes)
                        else captured[-4000:],
                        file=sys.stderr,
                    )
        sys.exit(1)
