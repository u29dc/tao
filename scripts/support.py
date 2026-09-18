"""Shared repository boundaries, command arguments and artifact identity."""

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CARGO = ROOT / 'scripts/cargo.sh'


def script_arguments(operations=()):
    """Accept Bun's conventional separator before options, including after a verb."""
    args = sys.argv[1:]
    if args[:1] == ['--']:
        args = args[1:]
    if len(args) > 1 and args[0] in operations and args[1] == '--':
        args = [args[0], *args[2:]]
    return args


def sha256(path):
    with path.open('rb') as stream:
        digest = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
        return digest.hexdigest()


def canonical_destination(raw):
    path = Path(raw).expanduser().absolute()
    # Reject symlinks anywhere in the destination, including a dangling leaf.
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise ValueError(f'refusing symlink destination: {path}')
    path = path.resolve()
    if path in (Path('/'), Path.home().resolve(), ROOT, ROOT.parent):
        raise ValueError(f'refusing broad destination: {path}')
    return path


def repo_output(raw):
    path = canonical_destination(raw)
    if not path.is_relative_to(ROOT) or path == ROOT:
        raise ValueError(f'output must remain inside repository: {path}')
    return path


def existing_vault(raw):
    """Accept an explicit read-only vault input without allowing broad roots."""
    path = Path(raw).expanduser().resolve()
    if not path.is_dir() or path in (Path('/'), Path.home().resolve()):
        raise ValueError(f'vault must be an existing non-broad directory: {path}')
    return path
