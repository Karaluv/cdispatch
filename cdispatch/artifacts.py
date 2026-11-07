"""Bounded, declarative file checks for adopting existing scientific jobs."""
from pathlib import Path
import re


def validate_contract(contract):
    if not isinstance(contract,list) or not 1<=len(contract)<=32:raise ValueError('Expected 1..32 artifact checks')
    for rule in contract:
        if not isinstance(rule,dict) or not isinstance(rule.get('path'),str) or not Path(rule['path']).is_absolute():
            raise ValueError('Artifact paths must be absolute')
        if type(rule.get('min_bytes',0)) is not int or rule.get('min_bytes',0)<0:raise ValueError('Invalid artifact size')
        for key in ('contains','regex'):
            patterns=rule.get(key,[])
            if not isinstance(patterns,list) or len(patterns)>32 or any(not isinstance(x,str) or len(x)>1000 for x in patterns):raise ValueError('Invalid artifact patterns')
        for pattern in rule.get('regex',[]):re.compile(pattern)


def check(contract):
    validate_contract(contract);evidence=[]
    for rule in contract:
        path=Path(rule['path'])
        if not path.is_file() or path.stat().st_size<rule.get('min_bytes',0):raise ValueError('Missing/incomplete artifact: '+str(path))
        contains=set(rule.get('contains',[]));patterns={pattern:re.compile(pattern) for pattern in rule.get('regex',[])}
        if contains or patterns:
            with path.open(errors='replace') as stream:
                for line in stream:
                    contains={value for value in contains if value not in line}
                    patterns={value:pattern for value,pattern in patterns.items() if not pattern.search(line)}
                    if not contains and not patterns:break
            if contains or patterns:raise ValueError('Artifact validation failed: '+str(path))
        evidence.append(dict(path=str(path),bytes=path.stat().st_size))
    return evidence
