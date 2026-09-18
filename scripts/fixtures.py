#!/usr/bin/env python3
"""Deterministic, repository-local benchmark fixtures and semantic validation."""

import argparse
import hashlib
import json
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

from support import ROOT, repo_output, script_arguments

MANIFEST = '.fixture-manifest.json'
VERSION = 2


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


def pdf(text):
    content = f'BT /F1 12 Tf 40 740 Td ({text}) Tj ET'.encode('ascii')
    objects = [
        b'<< /Type /Catalog /Pages 2 0 R >>',
        b'<< /Type /Pages /Kids [3 0 R] /Count 1 >>',
        b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>',
        b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>',
        b'<< /Length ' + str(len(content)).encode() + b' >>\nstream\n' + content + b'\nendstream',
    ]
    output = bytearray(b'%PDF-1.4\n')
    offsets = [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(output))
        output += f'{index} 0 obj\n'.encode() + obj + b'\nendobj\n'
    start = len(output)
    output += f'xref\n0 {len(objects) + 1}\n0000000000 65535 f \n'.encode()
    for offset in offsets[1:]:
        output += f'{offset:010d} 00000 n \n'.encode()
    output += (
        f'trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{start}\n%%EOF\n'.encode()
    )
    return output


def files_for_manifest(root):
    files = []
    for path in root.rglob('*'):
        if '.tao' in path.relative_to(root).parts or path.name == MANIFEST:
            continue
        if path.is_symlink():
            raise ValueError(f'fixture contains a symlink: {path}')
        if path.is_file():
            files.append(path)
    return sorted(files)


def facts(root):
    digest = hashlib.sha256()
    by_type = {}
    md_sizes = []
    degrees = []
    properties = Counter()
    for path in files_for_manifest(root):
        relative = path.relative_to(root).as_posix()
        # Inventory samples are small; no enormous content is allocated or hashed here.
        if path.is_symlink():
            raise ValueError(f'fixture contains a symlink: {path}')
        content = path.read_bytes()
        digest.update(relative.encode() + b'\0' + hashlib.sha256(content).digest())
        extension = path.suffix.lower() or '<none>'
        row = by_type.setdefault(extension, {'files': 0, 'bytes': 0})
        row['files'] += 1
        row['bytes'] += len(content)
        if extension == '.md':
            md_sizes.append(len(content))
            text = content.decode('utf-8')
            degrees.append(text.count('[['))
            if text.startswith('---\n'):
                for line in text.split('---\n', 2)[1].splitlines():
                    if ':' in line and not line.startswith(' '):
                        properties[line.split(':', 1)[0]] += 1
    return {
        'sha256': digest.hexdigest(),
        'files_by_extension': by_type,
        'markdown_bytes': {
            'min': min(md_sizes, default=0),
            'max': max(md_sizes, default=0),
            'median': statistics.median(md_sizes) if md_sizes else 0,
            'total': sum(md_sizes),
        },
        'outgoing_link_occurrences': {'total': sum(degrees), 'max': max(degrees, default=0)},
        'property_counts': dict(sorted(properties.items())),
    }


def safe_generation_root(raw):
    root = repo_output(raw)
    tracked = subprocess.check_output(['git', 'ls-files', '--', str(root)], cwd=ROOT, text=True)
    if tracked.strip():
        raise ValueError(f'refusing to replace tracked fixture content: {root}')
    if root.exists() and any(root.iterdir()):
        marker = root / MANIFEST
        if marker.is_symlink() or not marker.is_file():
            raise ValueError(f'unmanaged fixture output; choose a new directory: {root}')
        existing = json.loads(marker.read_text())
        if (
            existing.get('owner') != 'tao-benchmark-fixture'
            or existing.get('generator_version') != VERSION
        ):
            raise ValueError(f'unrecognized fixture ownership marker: {root}')
    return root


def publish(stage, root):
    if root.exists():
        # Only generator-owned, repository-local content can be replaced.
        shutil.rmtree(root)
    stage.rename(root)


def generate(output, profile, notes, seed, variant):
    name = 'vault-custom' if profile == 'custom' else f'vault-{profile}'
    if variant == 'representative':
        name += '-representative'
    root = safe_generation_root(output / name)
    root.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.fixture-', dir=root.parent))
    try:
        counts = {
            'projects': max(2, notes // 6),
            'contacts': max(1, notes // 7),
            'companies': max(1, notes // 18),
            'meetings': max(1, notes // 4),
        }
        counts['daily'] = notes - sum(counts.values())
        if counts['daily'] < 1:
            raise ValueError('--notes must be at least 12')
        expected_projects = []
        paths = []
        for category, count in counts.items():
            singular = {'companies': 'company', 'daily': 'daily'}.get(category, category[:-1])
            for index in range(1, count + 1):
                path = (
                    f'daily/2026-02-{index % 28 + 1:02}-{index}.md'
                    if category == 'daily'
                    else f'notes/{category}/{singular}-{index}.md'
                )
                paths.append(path)
                project = (index + seed) % counts['projects'] + 1
                company = (index * 5 + seed) % counts['companies'] + 1
                contact = (index * 3 + seed) % counts['contacts'] + 1
                metadata = {
                    'seed': seed,
                    'category': singular,
                    'project': f'[[notes/projects/project-{project}.md]]',
                    'company': f'[[notes/companies/company-{company}.md]]',
                    'contacts': [f'[[notes/contacts/contact-{contact}.md]]'],
                    'related': [f'[[notes/meetings/meeting-{index % counts["meetings"] + 1}.md]]'],
                    'tags': [singular, 'benchmark'],
                }
                if (index + seed) % 19 == 0:
                    metadata['related'].append(f'[[missing-{singular}-{index}]]')
                if category == 'projects':
                    metadata.update(
                        status=['active', 'paused', 'planning', 'archived'][(index + seed) % 4],
                        priority=(index + seed) % 5 + 1,
                    )
                    expected_projects.append(
                        {
                            'path': path,
                            'status': metadata['status'],
                            'priority': metadata['priority'],
                        }
                    )
                elif category == 'contacts':
                    metadata['role'] = 'stakeholder'
                elif category == 'companies':
                    metadata.update(
                        sector='software',
                        stage='growth',
                        aliases=[f'Company {index}', f'Company, {index}'],
                    )
                elif category == 'meetings':
                    metadata.update(
                        date=f'2026-01-{index % 28 + 1:02}',
                        attendees=[f'[[notes/contacts/contact-{contact}.md]]'],
                    )
                if category == 'projects':
                    metadata['depends_on'] = f'[[notes/projects/project-{index % count + 1}.md]]'
                if variant == 'representative' and index % 17 == 0:
                    metadata['optional_value'] = None if index % 34 else ['mixed', 7]
                body = f'# {singular.title()} {index}\n\nProject benchmark evidence token-{category}-{index}.\n\n'
                body += f'Review [[notes/projects/project-{project}.md]].\n\n- [ ] Follow up {singular} {index}\n- [x] Completed evidence review\n'
                if variant == 'representative':
                    if category == 'projects' and index == 1:
                        body = body.replace(
                            'Follow up project 1\n',
                            'Follow up project 1 ' + 'extended evidence ' * 500 + '\n',
                        )
                    desired = (
                        750_000
                        if category == 'projects' and index == 1
                        else 1024 + ((index * 7919 + seed) % 20_000)
                    )
                    paragraph = f'\nSource section for {singular} {index}. Evidence, decisions and project context with Unicode café 日本語.\n'
                    body += paragraph * (desired // len(paragraph.encode()))
                    if category == 'projects' and index == 1:
                        body += '\n## Related records\n' + '\n'.join(
                            f'[[notes/projects/project-{i}.md]]'
                            for i in range(2, min(counts['projects'], 201) + 1)
                        )
                        body += '\n[Supporting PDF](../../assets/reference.pdf)\n'
                frontmatter = (
                    '---\n'
                    + ''.join(
                        f'{key}: {json.dumps(value, ensure_ascii=False)}\n'
                        for key, value in metadata.items()
                    )
                    + '---\n'
                )
                write(stage / path, frontmatter + body)
        write(
            stage / 'templates/daily-template.md',
            '---\ntemplate: "daily"\n---\n# Daily Template\n- [ ] Plan day\n',
        )
        for category in ('projects', 'contacts', 'companies', 'meetings'):
            base = f'views:\n  - name: {category.title()}\n    type: table\n    source: notes/{category}\n'
            if category == 'projects':
                base += '    filters:\n      - key: status\n        op: neq\n        value: archived\n    sorts:\n      - key: priority\n        direction: desc\n'
            base += (
                '    columns:\n      - title\n      - status\n      - priority\n      - company\n'
            )
            write(stage / f'views/{category}.base', base)
        if variant == 'representative':
            write(
                stage / 'texts/über-notebook.txt',
                'Plain text evidence.\nThe fixture lighthouse phrase is visible here.\n',
            )
            write(
                stage / 'notes/duplicates/project-1.md',
                '---\ncategory: "duplicate"\n---\n# Duplicate name\n',
            )
            (stage / 'assets').mkdir(exist_ok=True)
            (stage / 'assets/reference.pdf').write_bytes(pdf('Fixture PDF lighthouse evidence'))
            (stage / 'assets/unknown.bin').write_bytes(bytes(range(256)))
        manifest = {
            'owner': 'tao-benchmark-fixture',
            'generator_version': VERSION,
            'profile': profile,
            'variant': variant,
            'seed': seed,
            'requested_notes': notes,
            'expected_projects': expected_projects,
            'expected_tasks_by_state': {'open': notes + 1, 'done': notes},
            'expected_resolved_links': [
                {'source': 'notes/projects/project-1.md', 'target': 'notes/projects/project-2.md'}
            ],
            'expected_project_view_count': sum(
                p['status'] != 'archived' for p in expected_projects
            ),
            **facts(stage),
        }
        write(stage / MANIFEST, json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
        publish(stage, root)
        validate(root)
        return root
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def parity(output):
    for name in ('graph', 'base'):
        root = safe_generation_root(output / name)
        root.parent.mkdir(parents=True, exist_ok=True)
        stage = Path(tempfile.mkdtemp(prefix='.parity-', dir=root.parent))
        try:
            shutil.copytree(
                ROOT / 'fixtures' / name,
                stage,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns('.DS_Store', '.tao'),
            )
            write(
                stage / MANIFEST,
                json.dumps(
                    {
                        'owner': 'tao-benchmark-fixture',
                        'generator_version': VERSION,
                        'profile': name,
                        'variant': 'parity-copy',
                        'seed': None,
                        **facts(stage),
                    },
                    indent=2,
                )
                + '\n',
            )
            publish(stage, root)
        finally:
            if stage.exists():
                shutil.rmtree(stage)


def validate(root, database=None):
    root = repo_output(root)
    manifest = json.loads((root / MANIFEST).read_text())
    if (
        manifest.get('owner') != 'tao-benchmark-fixture'
        or manifest.get('generator_version') != VERSION
    ):
        raise ValueError('fixture generator version mismatch; regenerate fixture')
    observed = facts(root)
    for key, value in observed.items():
        if manifest.get(key) != value:
            raise ValueError(f'fixture manifest mismatch: {key}')
    if database:
        database = repo_output(database)
        connection = sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)
        try:
            for expected in manifest.get('expected_projects', []):
                rows = connection.execute(
                    "SELECT p.key,p.value_json FROM properties p JOIN files f ON f.file_id=p.file_id WHERE f.normalized_path=? AND p.key IN ('status','priority')",
                    (expected['path'],),
                ).fetchall()
                values = {key: json.loads(value) for key, value in rows}
                if (
                    values.get('status') != expected['status']
                    or values.get('priority') != expected['priority']
                ):
                    raise ValueError(f'indexed project semantics mismatch: {expected["path"]}')
            expected_markdown = manifest['files_by_extension'].get('.md', {}).get('files', 0)
            observed_markdown = connection.execute(
                'SELECT COUNT(*) FROM files WHERE is_markdown = 1'
            ).fetchone()[0]
            if observed_markdown != expected_markdown:
                raise ValueError('indexed Markdown count differs from fixture manifest')
            for key, expected_count in manifest['property_counts'].items():
                observed_count = connection.execute(
                    'SELECT COUNT(*) FROM properties WHERE key=?', (key,)
                ).fetchone()[0]
                if observed_count != expected_count:
                    raise ValueError(f'indexed property distribution mismatch: {key}')
            if 'expected_tasks_by_state' in manifest:
                task_counts = dict(
                    connection.execute('SELECT state,COUNT(*) FROM tasks GROUP BY state').fetchall()
                )
                if task_counts != manifest['expected_tasks_by_state']:
                    raise ValueError('indexed task state distribution differs from fixture')
            for relation in manifest.get('expected_resolved_links', []):
                count = connection.execute(
                    'SELECT COUNT(*) FROM links l JOIN files s ON s.file_id=l.source_file_id JOIN files t ON t.file_id=l.resolved_file_id WHERE s.normalized_path=? AND t.normalized_path=? AND l.is_unresolved=0',
                    (relation['source'], relation['target']),
                ).fetchone()[0]
                if count == 0:
                    raise ValueError('expected fixture relationship was not resolved')
            violations = connection.execute('PRAGMA foreign_key_check').fetchall()
            if violations:
                raise ValueError('fixture database foreign-key violations')
        finally:
            connection.close()
    return manifest


def main():
    arguments = script_arguments(('validate',))
    if arguments[:1] == ['validate']:
        parser = argparse.ArgumentParser(
            description='Validate fixture identity and actual indexed metadata'
        )
        parser.add_argument('root', type=Path)
        parser.add_argument('--database', type=Path)
        args = parser.parse_args(arguments[1:])
        validate(args.root, args.database)
        print(f'Fixture validated: {args.root}')
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', choices=('all', 'parity', '1k', '5k'), default='all')
    parser.add_argument('--notes', type=int)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--variant', choices=('tiny', 'representative'), default='tiny')
    parser.add_argument('--output', type=Path, default=ROOT / 'target/fixtures')
    parser.add_argument(
        '--skip-validate',
        action='store_true',
        help='compatibility option; manifest checks always run',
    )
    parser.add_argument('--validate-only', action='store_true')
    parser.add_argument('--database', type=Path)
    args = parser.parse_args(arguments)
    if args.seed < 0 or (args.notes is not None and not 12 <= args.notes <= 5000):
        parser.error('seed must be nonnegative and --notes between 12 and 5000')
    output = repo_output(args.output)
    output.mkdir(parents=True, exist_ok=True)
    if args.validate_only:
        validate(output, args.database)
    elif args.profile == 'parity':
        parity(output)
    else:
        profiles = (
            [('custom', args.notes)]
            if args.notes is not None
            else (
                [('1k', 1000), ('5k', 5000)]
                if args.profile == 'all'
                else [(args.profile, int(args.profile[:-1]) * 1000)]
            )
        )
        for profile, notes in profiles:
            print(generate(output, profile, notes, args.seed, args.variant))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, sqlite3.Error, subprocess.SubprocessError) as error:
        print(f'fixtures: {error}', file=sys.stderr)
        sys.exit(1)
