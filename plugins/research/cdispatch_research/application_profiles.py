"""Explicit administrator bindings of reviewed profiles to queued applications.

Keep original command, inputs, validators and submitted spec immutable. These
rules only enumerate user-authorized resource shapes for a future allocation.
"""
import copy
import hashlib
import json
from functools import lru_cache
from pathlib import Path

from cdispatch.paths import profile_root

def reviewed_profile(identifier,digest,library):
    if not identifier.startswith('cp2k/energy-force-excited/') or '..' in identifier.split('/'):
        raise ValueError('Application binding requires an explicit CP2K energy-force profile')
    from cdispatch.profile_library import entry_path
    raw=entry_path(library,identifier).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=digest:
        raise ValueError('Reviewed application profile changed: '+identifier)
    record=json.loads(raw)
    if record['id']!=identifier or record['kind']!='batch' or record['status'] not in ('candidate','validated'):
        raise ValueError('Application profile is not an eligible reviewed batch shape')
    if record['profile'].get('gpus_per_node',0):
        raise ValueError('O56 application bindings are CPU only')
    return record['profile']

def apply(spec,config):
    if spec.get('kind')!='batch':return spec
    for owner in config.get('application_lane_owners',[]):
        if (spec.get('parent')==owner['previous_parent'] and
                spec.get('labels',{}).get('project')=='glass' and
                spec.get('labels',{}).get('direction')==owner['lane']):
            result=copy.deepcopy(spec);result['profiles']=[]
            result['_application_lane_delegated']=owner['owner']
            return result
    if spec.get('_application_profile_rule'):return spec
    matches=[r for r in config.get('application_profile_rules',[]) if
             r.get('authorization') and r.get('labels') and
             all(spec.get('labels',{}).get(k)==v for k,v in r['labels'].items())]
    if not matches:return spec
    if len(matches)!=1:raise ValueError('Ambiguous application resource binding')
    rule=matches[0]
    if not rule.get('profiles'):raise ValueError('Empty application resource binding')
    result=copy.deepcopy(spec);template=result['profiles'][0];profiles=[]
    for ref in rule['profiles']:
        shape=copy.deepcopy(reviewed_profile(ref['id'],ref['sha256'],config.get('profile_library',profile_root())))
        p=copy.deepcopy(template)
        # Do not retain placement/memory constraints from a different shape.
        for field in ('allowed_nodes','memory_model','expected_seconds'):
            p.pop(field,None)
        binding_env=dict(p.get('env',{}));binding_env.update(shape.get('env',{}))
        shape.pop('command',None);shape.pop('env',None)
        p.update(shape);p['env']=binding_env;profiles.append(p)
    result['profiles']=profiles
    result.setdefault('placement',{}).pop('preferred_partitions',None)
    if rule.get('preferred_partitions'):
        result['placement']['preferred_partitions']=list(rule['preferred_partitions'])
    if 'preferred_cpu_counts' in rule:
        from cdispatch.common import validate_cpu_preference
        result['placement']['preferred_cpu_counts']=validate_cpu_preference(rule['preferred_cpu_counts'])
    result['placement']['allow_fallback']=True
    result['_application_profile_rule']=rule['name']
    return result
