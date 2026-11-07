"""Human labels from advertised Slurm partition families and typed GRES."""
import re


def description(node):
    cpu=node.get('CPUModel') or node.get('CpuSpec') or 'CPU model not advertised'
    names={'a100':'NVIDIA A100','h200nvl':'NVIDIA H200 NVL','titan_x':'NVIDIA TITAN X',
           'titanx':'NVIDIA TITAN X','v100':'NVIDIA V100','rtx4500':'NVIDIA RTX 4500'}
    gpu=[]
    for kind,count in re.findall(r'gpu:([^:,()]+):(\d+)',node.get('Gres','')):
        gpu.append(count+'× '+names.get(kind.lower(),kind))
    return dict(cpu=cpu,gpu=', '.join(gpu) or ('GPU (type not advertised)' if 'gpu' in node.get('Gres','') else 'без GPU'),source='Slurm node annotation / GRES')


def has_gpu(node):
    return 'gpu' in node.get('Gres','') or bool(re.search(r'gres/gpu(?:[:=])',node.get('CfgTRES','')))
