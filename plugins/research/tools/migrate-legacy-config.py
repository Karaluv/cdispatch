#!/usr/bin/env python3
"""Write a separate migrated site config; never edit inputs or contact Slurm."""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--profiles', required=True, type=Path)
    parser.add_argument('--python', required=True, type=Path)
    args = parser.parse_args()
    config = json.loads(args.source.read_text())
    if config.get('plugins'):
        parser.error('Input already declares plugins; merge it explicitly')
    options = dict(profile_library=str(args.profiles.resolve()),
                   application_profile_rules=config.pop('application_profile_rules', []),
                   application_lane_owners=config.pop('application_lane_owners', []),
                   displacement_stages=['ground', 'excited'])
    config['plugins'] = [dict(module='cdispatch_research.plugin', options=options)]
    config['controller_transfer_stages'] = ['geometry-controller']
    config['python'] = str(args.python.absolute())
    # Do not resolve the interpreter symlink: doing so would bypass its virtualenv.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as stream:
        json.dump(config, stream, indent=2); stream.write('\n')
    args.output.chmod(0o600)
    print(args.output)
    print('Prepared only. Review queued commands, broker roots and shared handlers before cutover.')

if __name__ == '__main__':
    main()
