#!/usr/bin/env python3
"""Build/package locally; install and remove only explicitly managed artifacts."""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

from support import (
    CARGO,
    ROOT,
    canonical_destination,
    repo_output,
    script_arguments,
    sha256,
)

MARKER = '.tao-install.json'


def verify(binary):
    # Run away from the checkout, so successful validation cannot depend on cwd.
    with tempfile.TemporaryDirectory(prefix='verify-', dir=ROOT / 'target') as cwd:
        for option in ('--version', '--help'):
            subprocess.run(
                [str(binary), option], cwd=cwd, check=True, stdout=subprocess.DEVNULL, timeout=20
            )


def build():
    target = repo_output(ROOT / 'target')
    subprocess.run(
        [
            str(CARGO),
            'build',
            '--locked',
            '--release',
            '-p',
            'tao-cli',
            '--target-dir',
            str(target),
        ],
        cwd=ROOT,
        check=True,
    )
    return target / 'release' / ('tao.exe' if os.name == 'nt' else 'tao')


def package(binary, out):
    out = repo_output(out)
    out.mkdir(parents=True, exist_ok=True)
    for name in ('tao-cli-bundle.tar.gz', 'manifest.json', 'tao-cli-bundle.tar.gz.sha256'):
        if (out / name).is_symlink():
            raise ValueError(f'refusing symlink package output: {out / name}')
    with tempfile.TemporaryDirectory(prefix='package-', dir=ROOT / 'target') as raw:
        stage = Path(raw)
        staged = stage / 'tao'
        shutil.copy2(binary, staged)
        if shutil.which('codesign'):
            subprocess.run(['codesign', '--force', '--sign', '-', str(staged)], check=True)
        verify(staged)
        version = subprocess.check_output([str(staged), '--version'], text=True).strip()
        manifest = {
            'schema_version': 1,
            'version': version,
            'platform': platform.system(),
            'architecture': platform.machine(),
            'files': {'tao': sha256(staged)},
        }
        (stage / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        bundle = stage / 'tao-cli-bundle.tar.gz'
        with tarfile.open(bundle, 'w:gz') as archive:
            for name in ('tao', 'manifest.json'):
                archive.add(stage / name, arcname=name)
        extracted = stage / 'extracted'
        extracted.mkdir()
        with tarfile.open(bundle) as archive:
            # This archive was built above with two known regular files.
            for member in archive.getmembers():
                if member.name not in ('tao', 'manifest.json') or not member.isfile():
                    raise ValueError('unexpected packaged member')
                with archive.extractfile(member) as source:
                    destination = extracted / member.name
                    with destination.open('wb') as output:
                        shutil.copyfileobj(source, output)
                    destination.chmod(member.mode)
        if sha256(extracted / 'tao') != manifest['files']['tao']:
            raise ValueError('extracted executable checksum mismatch')
        verify(extracted / 'tao')
        destination = out / bundle.name
        os.replace(bundle, destination)
        (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        (out / (bundle.name + '.sha256')).write_text(
            sha256(destination) + '  ' + bundle.name + '\n'
        )
        print(destination)


def install(binary, raw_out):
    out = canonical_destination(raw_out)
    out.mkdir(parents=True, exist_ok=True)
    destination = out / 'tao'
    marker = out / MARKER
    if destination.is_symlink() or marker.is_symlink():
        raise ValueError('refusing symlink installation artifact')
    old_marker = marker.read_bytes() if marker.exists() else None
    # Replace just the explicit executable, preserving configuration and all state.
    with tempfile.TemporaryDirectory(prefix='.tao-install-', dir=out) as raw:
        stage = Path(raw)
        candidate = stage / 'tao'
        shutil.copy2(binary, candidate)
        candidate.chmod(candidate.stat().st_mode | 0o111)
        if shutil.which('codesign'):
            subprocess.run(['codesign', '--force', '--sign', '-', str(candidate)], check=True)
        verify(candidate)
        previous = stage / 'previous'
        if destination.exists():
            shutil.copy2(destination, previous)
        marker_candidate = stage / 'marker.json'
        marker_candidate.write_text(
            json.dumps(
                {'schema_version': 1, 'owner': 'tao', 'files': {'tao': sha256(candidate)}}, indent=2
            )
            + '\n'
        )
        os.replace(candidate, destination)
        try:
            verify(destination)
            os.replace(marker_candidate, marker)
        except BaseException:
            if previous.exists():
                os.replace(previous, destination)
            else:
                destination.unlink(missing_ok=True)
            if old_marker is not None:
                marker.write_bytes(old_marker)
            else:
                marker.unlink(missing_ok=True)
            raise
    print(f'Installed and verified {destination}; configuration and runtime state preserved.')


def uninstall(raw_out, dry_run=False):
    out = canonical_destination(raw_out)
    marker = out / MARKER
    if marker.is_symlink() or not marker.is_file():
        raise ValueError(f'not a managed Tao installation: {out}')
    manifest = json.loads(marker.read_text())
    if (
        manifest.get('owner') != 'tao'
        or manifest.get('schema_version') != 1
        or set(manifest.get('files', {})) != {'tao'}
    ):
        raise ValueError('invalid installation ownership marker')
    binary = out / 'tao'
    if binary.is_symlink() or not binary.is_file() or sha256(binary) != manifest['files']['tao']:
        raise ValueError('installed executable changed; refusing to remove unmanaged content')
    print(f'{"Would remove" if dry_run else "Removing"} managed executable {binary}')
    if not dry_run:
        binary.unlink()
        marker.unlink()
        # Never remove configuration, sockets, indexes, models, or other contents.
        if not any(out.iterdir()):
            out.rmdir()


def clean(dry_run=False):
    paths = [repo_output(ROOT / name) for name in ('target', 'dist')]
    for path in paths:
        print(f'{"Would remove" if dry_run else "Removing"} build output {path}')
        if not dry_run and path.exists():
            shutil.rmtree(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'operation',
        nargs='?',
        choices=('build', 'package', 'install', 'clean', 'uninstall'),
        default='package',
    )
    parser.add_argument(
        '--target',
        choices=('cli', 'all'),
        default='all',
        help='compatibility selector; both package the CLI',
    )
    parser.add_argument('--out', type=Path)
    parser.add_argument(
        '--artifact',
        type=Path,
        help='existing executable to verify/package/install without building',
    )
    parser.add_argument(
        '--dry-run', action='store_true', help='show cleanup/uninstall scope without removing files'
    )
    args = parser.parse_args(
        script_arguments(('build', 'package', 'install', 'clean', 'uninstall'))
    )
    if args.operation in ('clean', 'uninstall'):
        if args.operation == 'clean':
            if args.out or args.artifact:
                parser.error('clean accepts no output/artifact override')
            clean(args.dry_run)
        else:
            if args.out is None:
                parser.error('uninstall requires explicit --out')
            uninstall(args.out, args.dry_run)
        return
    if args.dry_run:
        parser.error('--dry-run applies only to clean/uninstall')
    repo_output(ROOT / 'target').mkdir(exist_ok=True)
    binary = args.artifact.resolve() if args.artifact else build()
    if not binary.is_file():
        parser.error(f'executable does not exist: {binary}')
    if args.operation == 'build':
        verify(binary)
    elif args.operation == 'package':
        package(binary, args.out or ROOT / 'dist')
    else:
        install(
            binary,
            args.out
            or Path(
                os.environ.get(
                    'TAO_HOME',
                    str(Path(os.environ.get('TOOLS_HOME', str(Path.home() / '.tools'))) / 'tao'),
                )
            ),
        )


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f'tao release: {error}', file=sys.stderr)
        sys.exit(1)
