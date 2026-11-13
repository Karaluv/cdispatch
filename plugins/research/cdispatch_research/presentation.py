import re
DIRECTIONS = {'torsion_plus':'торсия +', 'torsion_minus':'торсия −', 'bond_si3':'связь Si3', 'bond_si11':'связь Si11', 'bend_plus':'изгиб +', 'bend_minus':'изгиб −'}

def group(task):
    s=task.get('spec',{});labels=s.get('labels',{});name=s.get('name','Задание')
    if labels.get('stage')=='geometry-controller':return 'Управление сканами O56'
    direction=labels.get('direction') or labels.get('case')
    if direction in DIRECTIONS:return 'O56 · '+DIRECTIONS[direction]
    if 'O56 exact-angle PES' in labels.get('purpose',''):return 'O56 · угол'
    if name=='codex-node':return 'Codex'
    return name

def label(task):
    s=task.get('spec',{});name=group(task)
    if s.get('labels',{}).get('stage')=='geometry-controller':
        return 'O56 · управление '+('углом' if 'angle' in s.get('name','') else '4 сканами')
    if name.startswith('O56 · '):
        command=' '.join(str(x) for p in s.get('profiles',[]) for x in p.get('command',[]))
        point=re.search(r'(?:angle|point)_(\d+)',command)
        if point:name+=' · точка '+str(int(point[1]))
        stage=s.get('labels',{}).get('stage')
        if stage in ('ground','excited'):name+=' · '+stage
        lane=s.get('labels',{}).get('lane')
        if lane is not None:name+=' · вариант '+lane
    return name
