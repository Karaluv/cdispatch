"""Portable initial configuration; site values are supplied explicitly."""
import sys


def initial(owner, account, partitions):
    if not owner or not account or not partitions or any(not p for p in partitions):
        raise ValueError('Owner, account and at least one partition are required')
    limits = dict(jobs=5, cpus=None, mem_mib=None, gpus=None, nodes=None)
    return dict(
        owner=owner, python=sys.executable,
        users={owner: dict(enabled=True, authorized=True, account=account,
                           partitions=list(partitions), limits=dict(limits))},
        pool_limits=dict(limits), node_cost={}, excluded_nodes=[],
        poll_interval=10, submit_interval=10, account_interval=300,
        max_pending=3, interactive_reserve_jobs=0,
        auto_preempt=False, auto_rebalance=False,
        plan_window=12, plan_beam=32, failure_burst_limit=3,
        respect_slurm_reservations=True, respect_pending_feedback=True,
        backfill_margin_seconds=120, cancel_retry_interval=30,
        service_partitions=list(partitions), service_preferred_nodes=[],
        interactive_partitions=list(partitions), service_wall_minutes=1440,
        shared_pool=dict(enabled=True, cpus=4, mem_mib=4096,
                         reserve_cpus=1, reserve_mem_mib=512,
                         max_cpus=8, max_mem_mib=16384, max_handlers=16,
                         auto_grow=False),
        task_history=dict(enabled=True), plugins=[])
