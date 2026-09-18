#!/usr/bin/env python3
"""Fixed, independently judged mixed-format search acceptance corpus."""

import hashlib
import json
import math
import subprocess
from pathlib import Path

# Judgments describe the authored source evidence, never the observed search output.
# Grade 3 is an exact requested entity; grade 2 is supporting topic evidence.
JUDGMENTS = [
    {
        'id': 'exact-markdown-entity',
        'query': 'aurora',
        'relevance': {
            'notes/aurora.md': 3,
            'notes/repetition.md': 2,
            'texts/field-log.txt': 2,
            'reports/field-evidence.pdf': 2,
            'assets/aurora-rig.bin': 2,
        },
    },
    {
        'id': 'exact-alias',
        'query': 'Northstar Launch',
        'relevance': {'notes/decision-record.md': 3, 'notes/northstar-launch-mention.md': 2},
    },
    {'id': 'punctuated-alias', 'query': 'Atlas, Program', 'relevance': {'notes/programme.md': 3}},
    {
        'id': 'normalized-unicode-title',
        'query': 'café navigation',
        'relevance': {'notes/Café_Navigation.md': 3},
    },
    {
        'id': 'title-over-body',
        'query': 'Lighthouse Registry',
        'relevance': {'notes/Lighthouse_Registry.md': 3, 'texts/station-log.txt': 2},
    },
    {'id': 'txt-body', 'query': 'cobaltquartz', 'relevance': {'texts/field-log.txt': 3}},
    {'id': 'pdf-body', 'query': 'embercircuit', 'relevance': {'reports/field-evidence.pdf': 3}},
    {
        'id': 'inventory-exact',
        'query': 'spectrometer blueprint',
        'relevance': {'assets/spectrometer-blueprint.bin': 3},
    },
    {'id': 'txt-name', 'query': 'field log', 'relevance': {'texts/field-log.txt': 3}},
    {'id': 'pdf-name', 'query': 'field evidence', 'relevance': {'reports/field-evidence.pdf': 3}},
    {
        'id': 'mixed-docs',
        'query': 'aurora',
        'options': ['--kind', 'docs'],
        'relevance': {
            'notes/aurora.md': 3,
            'notes/repetition.md': 2,
            'texts/field-log.txt': 2,
            'reports/field-evidence.pdf': 2,
        },
    },
    {
        'id': 'mixed-inventory',
        'query': 'aurora',
        'options': ['--kind', 'files'],
        'relevance': {'notes/aurora.md': 3, 'assets/aurora-rig.bin': 2},
    },
    {
        'id': 'txt-scope',
        'query': 'aurora',
        'options': ['--scope', 'texts'],
        'relevance': {'texts/field-log.txt': 3},
    },
    {
        'id': 'pdf-scope',
        'query': 'embercircuit',
        'options': ['--scope', 'reports'],
        'relevance': {'reports/field-evidence.pdf': 3},
    },
    {
        'id': 'markdown-scope',
        'query': 'aurora',
        'options': ['--scope', 'notes'],
        'relevance': {'notes/aurora.md': 3, 'notes/repetition.md': 2},
    },
    {
        'id': 'txt-extension',
        'query': 'aurora',
        'options': ['--ext', 'txt'],
        'relevance': {'texts/field-log.txt': 3},
    },
    {
        'id': 'pdf-extension',
        'query': 'aurora',
        'options': ['--ext', 'pdf'],
        'relevance': {'reports/field-evidence.pdf': 3},
    },
    {'id': 'empty-result', 'query': 'qzvneverappears84721', 'relevance': {}},
]


def pdf_pages(texts):
    objects = [b'<< /Type /Catalog /Pages 2 0 R >>', b'']
    page_ids = []
    for text in texts:
        page_id = len(objects) + 1
        page_ids.append(page_id)
        escaped = text.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
        content = f'BT /F1 12 Tf 40 740 Td ({escaped}) Tj ET'.encode('ascii')
        objects.extend(
            [
                f'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 {page_id + 1} 0 R >> >> /Contents {page_id + 2} 0 R >>'.encode(),
                b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>',
                b'<< /Length '
                + str(len(content)).encode()
                + b' >>\nstream\n'
                + content
                + b'\nendstream',
            ]
        )
    kids = ' '.join(f'{page_id} 0 R' for page_id in page_ids)
    objects[1] = f'<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>'.encode()
    result = bytearray(b'%PDF-1.4\n')
    offsets = []
    for index, obj in enumerate(objects, 1):
        offsets.append(len(result))
        result += f'{index} 0 obj\n'.encode() + obj + b'\nendobj\n'
    start = len(result)
    result += f'xref\n0 {len(objects) + 1}\n0000000000 65535 f \n'.encode()
    for offset in offsets:
        result += f'{offset:010d} 00000 n \n'.encode()
    result += (
        f'trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{start}\n%%EOF\n'.encode()
    )
    return result


def sources():
    payload = {
        'notes/aurora.md': '# Aurora\n\nThe named Aurora research programme studies polar sensor measurements.\n',
        'notes/repetition.md': '# Repeated observations\n\n'
        + 'aurora readings are listed again.\n' * 8000,
        'notes/decision-record.md': '---\naliases: ["Northstar Launch"]\n---\n# Decision record\n\nThe authoritative launch decision is recorded here.\n',
        'notes/northstar-launch-mention.md': '# Historical reference\n\nNorthstar Launch was mentioned in an earlier discussion.\n',
        'notes/programme.md': '---\naliases: ["Atlas, Program"]\n---\n# Programme register\n',
        'notes/Café_Navigation.md': '# Café Navigation\n\nNavigation observations use a shared vocabulary.\n',
        'notes/Lighthouse_Registry.md': '# Lighthouse Registry\n\nThe authoritative register of named coastal stations.\n',
        'texts/field-log.txt': 'Field log\nAurora measurements were captured at the remote station.\nThe unique cobaltquartz marker identifies this plain text evidence.\n',
        'texts/station-log.txt': 'Station log\nA historical note refers to the Lighthouse Registry.\n',
    }
    for index in range(24):
        payload[f'notes/unrelated-{index:02}.md'] = (
            f'# Unrelated {index}\n\nOrdinary unrelated gardening observations, specimen {index}.\n'
        )
    # Zero-grade lexical distractors: mentioning an entity in test boilerplate is
    # not evidence about it. Their count crosses the fixed retrieval page boundary.
    for index in range(120):
        payload[f'notes/noise/filler-{index:03}.md'] = (
            f'# Scratchpad {index}\n\nThis unrelated boilerplate contains the arbitrary '
            'test string Lighthouse Registry without information about the register.\n'
        )
    encoded = {path: content.encode('utf-8') for path, content in payload.items()}
    encoded['reports/field-evidence.pdf'] = pdf_pages(
        [f'Aurora evidence page {index + 1}. Unique embercircuit marker.' for index in range(24)]
    )
    encoded['assets/aurora-rig.bin'] = bytes(range(256))
    encoded['assets/spectrometer-blueprint.bin'] = (
        b'Inventory-only fabricated engineering binary.\x00\x01'
    )
    return encoded


def score(paths, relevance, k=10):
    if len(paths) != len(set(paths)):
        raise ValueError('canonical search candidates were duplicated')
    if not relevance:
        return {
            'empty_correct': not paths,
            'top1': None,
            'recall_at_10': None,
            'mrr': None,
            'ndcg_at_10': None,
        }
    highest = max(relevance.values())
    top1 = bool(paths) and relevance.get(paths[0], 0) == highest
    found = set(paths[:k]) & set(relevance)
    reciprocal_rank = next(
        (1 / (index + 1) for index, path in enumerate(paths) if path in relevance), 0.0
    )

    def dcg(grades):
        return sum((2**grade - 1) / math.log2(index + 2) for index, grade in enumerate(grades))

    ideal = dcg(sorted(relevance.values(), reverse=True)[:k])
    return {
        'top1': top1,
        'recall_at_10': len(found) / len(relevance),
        'mrr': reciprocal_rank,
        'ndcg_at_10': dcg([relevance.get(path, 0) for path in paths[:k]]) / ideal,
    }


def stable_limits(windows):
    largest = windows[max(windows)]
    return all(paths == largest[:limit] for limit, paths in windows.items())


def validate_coverage(coverage, paths, options=()):
    selected = list(paths)
    for option, value in zip(options[::2], options[1::2]):
        if option == '--scope':
            selected = [
                path
                for path in selected
                if path == value or path.startswith(value.rstrip('/') + '/')
            ]
        elif option == '--ext':
            extensions = set(value.split(','))
            selected = [path for path in selected if Path(path).suffix.lstrip('.') in extensions]
    markdown = sum(Path(path).suffix == '.md' for path in selected)
    extracted = sum(Path(path).suffix in ('.txt', '.pdf') for path in selected)
    expected = {
        'total_files': len(selected),
        'searchable_files': markdown + extracted,
        'markdown_files': markdown,
        'extracted_files': extracted,
        'unsupported_files': len(selected) - markdown - extracted,
        'pending_files': 0,
        'failed_files': 0,
        'partial_files': 0,
        'stale_files': 0,
        'diagnostic_files': 0,
    }
    if not isinstance(coverage, dict) or coverage.get('complete') is not True:
        raise ValueError('relevance results lack completed current content coverage')
    if any(
        type(coverage.get(key)) is not int or coverage[key] != count
        for key, count in expected.items()
    ):
        raise ValueError('relevance content coverage differs from the authored corpus')


def corpus_hash(payload):
    digest = hashlib.sha256()
    for path, content in sorted(payload.items()):
        digest.update(path.encode() + b'\0' + hashlib.sha256(content).digest())
    return digest.hexdigest()


def run_relevance(binary, report, env):
    import bench

    payload = sources()
    for judgment in JUDGMENTS:
        if not set(judgment['relevance']).issubset(payload):
            raise ValueError('judgment references an absent authored source')
    vault = report / 'vault'
    vault.mkdir()
    for relative, content in payload.items():
        path = vault / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    source_mtimes = {path: (vault / path).stat().st_mtime_ns for path in payload}
    # Freeze source identity and judgments before running the tool.
    manifest = {
        'schema_version': 1,
        'corpus_sha256': corpus_hash(payload),
        'files': len(payload),
        'pdf_pages': 24,
        'judgments': JUDGMENTS,
        'judgment_basis': 'Authored exact entities and explicit supporting text; grade 3 exact entity, grade 2 supporting topic evidence, grade 0 unrelated lexical test boilerplate. No judgments learned from returned results.',
    }
    (report / 'judgments.json').write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + '\n'
    )
    database = report / 'index.sqlite'
    socket = Path(env['TMPDIR']) / 'relevance.sock'
    reindex = bench.run_json(
        bench.command(
            binary,
            ['vault', 'reindex', '--wait-content-ms', '60000'],
            vault,
            database,
            'direct',
            socket,
        ),
        env,
    )
    (report / 'reindex.json').write_text(json.dumps(reindex, indent=2) + '\n')
    if reindex['data'].get('content_complete') is not True:
        raise ValueError('relevance corpus content extraction did not complete')
    rows = []
    violations = []
    started = False
    try:
        bench.run_json([str(binary), 'vault', 'daemon', 'start', '--socket', str(socket)], env)
        started = True
        for mode in ('direct', 'daemon-hit', 'daemon-miss'):
            for judgment in JUDGMENTS:
                windows = {}
                coverage = None
                for limit in (1, 3, 10, 100):
                    operation = [
                        'search',
                        judgment['query'],
                        *judgment.get('options', []),
                        '--limit',
                        str(limit),
                    ]
                    argv = bench.command(binary, operation, vault, database, mode, socket)
                    if mode == 'daemon-hit':
                        bench.run_json(argv, env)
                    value = bench.run_json(argv, env)
                    bench.validate_runtime(value, mode)
                    data = value['data']
                    coverage = data.get('content_coverage')
                    validate_coverage(coverage, payload, judgment.get('options', []))
                    if judgment['id'] in ('txt-body', 'pdf-body'):
                        expected_path = next(iter(judgment['relevance']))
                        evidence = next(
                            (row for row in data['docs'] if row.get('path') == expected_path), None
                        )
                        locator_kind = 'page' if judgment['id'] == 'pdf-body' else 'line'
                        if (
                            not evidence
                            or evidence.get('locator', {}).get('kind') != locator_kind
                            or not evidence.get('revision')
                            or evidence.get('coverage') != 'complete'
                            or evidence.get('stale') is not False
                        ):
                            raise ValueError(
                                f'{judgment["id"]}: source revision/locator/coverage evidence missing or stale'
                            )
                    windows[limit] = [item['path'] for item in data['candidates']]
                    (report / f'{judgment["id"]}-{mode}-{limit}.json').write_text(
                        json.dumps(value, indent=2, ensure_ascii=False) + '\n'
                    )
                metrics = score(windows[100], judgment['relevance'])
                stable = stable_limits(windows)
                row = {
                    'id': judgment['id'],
                    'mode': mode,
                    'metrics': metrics,
                    'limit_stable': stable,
                    'limits': windows,
                    'coverage': coverage,
                }
                rows.append(row)
                if (
                    not stable
                    or (not judgment['relevance'] and not metrics['empty_correct'])
                    or (
                        judgment['relevance']
                        and (
                            not metrics['top1']
                            or metrics['recall_at_10'] != 1.0
                            or metrics['ndcg_at_10'] < 0.95
                        )
                    )
                ):
                    violations.append(
                        {
                            'id': judgment['id'],
                            'mode': mode,
                            'metrics': metrics,
                            'limit_stable': stable,
                        }
                    )
        for judgment in JUDGMENTS:
            answers = [row['limits'] for row in rows if row['id'] == judgment['id']]
            if any(answer != answers[0] for answer in answers[1:]):
                violations.append({'id': judgment['id'], 'reason': 'backend result order differs'})
    finally:
        if started:
            subprocess.run(
                [str(binary), 'vault', 'daemon', 'stop', '--socket', str(socket)],
                env=env,
                capture_output=True,
                timeout=20,
                check=False,
            )
    observed = {
        path.relative_to(vault).as_posix(): path.read_bytes()
        for path in vault.rglob('*')
        if path.is_file()
    }
    if observed != payload or any(
        (vault / path).stat().st_mtime_ns != modified for path, modified in source_mtimes.items()
    ):
        raise ValueError('read-only relevance run changed an original source')
    aggregates = {}
    for mode in ('direct', 'daemon-hit', 'daemon-miss'):
        selected = [
            row for row in rows if row['mode'] == mode and row['metrics']['top1'] is not None
        ]
        aggregates[mode] = {
            key: sum(float(row['metrics'][key]) for row in selected) / len(selected)
            for key in ('top1', 'recall_at_10', 'mrr', 'ndcg_at_10')
        }
        aggregates[mode]['limit_stability'] = all(
            row['limit_stable'] for row in rows if row['mode'] == mode
        )
    summary = {
        'schema_version': 1,
        'status': 'fail' if violations else 'pass',
        'judgments': len(JUDGMENTS),
        'queries_per_mode': len(JUDGMENTS),
        'limits': [1, 3, 10, 100],
        'aggregate': aggregates,
        'corpus_sha256': manifest['corpus_sha256'],
        'executable_sha256': bench.sha256(binary),
        'original_sources_unchanged': True,
        'environment': bench.metadata(),
        'violations': violations,
        'results': rows,
        'limitation': 'Small fixed synthetic semantic regression set; not a representative estimate of personal-vault search quality or a latency bench.',
    }
    (report / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n')
    print(f'Relevance reports: {report}', flush=True)
    print(json.dumps(aggregates, indent=2), flush=True)
    if violations:
        raise ValueError(
            f'{len(violations)} fixed relevance/limit-stability checks failed; judgments were preserved'
        )
