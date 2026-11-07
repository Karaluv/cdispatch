"""Compute-node supervisor for the dispatcher and its optional Telegram monitor."""
import argparse
import os
from pathlib import Path
import signal
import socket
import re
import subprocess
import sys
import tempfile
import time

from .common import atomic, read


class ProxyTunnel:
    def __init__(self, settings):
        self.settings=settings;self.process=None;self.directory=None;self.url=None

    def start(self):
        self.close()
        self.directory=tempfile.TemporaryDirectory(prefix='cdispatch-proxy-')
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
        cfg=self.settings
        argv=['ssh','-F','/dev/null','-N','-T','-i',cfg['identity'],'-o','IdentitiesOnly=yes',
              '-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','HostKeyAlias='+cfg.get('host_key_alias','localhost'),
              '-o','UserKnownHostsFile='+cfg['known_hosts'],'-o','ConnectTimeout=10','-o','ExitOnForwardFailure=yes',
              '-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3',
              '-L','127.0.0.1:%d:127.0.0.1:%d'%(port,cfg.get('port',10808)),cfg['destination']]
        self.process=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        self.url='socks5h://127.0.0.1:'+str(port)
        until=time.monotonic()+10
        while time.monotonic()<until and self.process.poll() is None:
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.2):return self.url
            except OSError:time.sleep(.1)
        # The supervisor restarts a failed tunnel; its failure never stops Slurm monitoring.
        return self.url

    def close(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
        if self.directory:self.directory.cleanup()
        self.process=None


def stop_process(proc, seconds=40):
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:proc.wait(timeout=seconds)
        except subprocess.TimeoutExpired:proc.kill();proc.wait()


def predecessor_active(root, config, job_id, now=None):
    """A newly released pool waits for the original service's authority.

    It may already host shared steps admitted by the original daemon, but
    must not start a competing daemon or Telegram poller while it waits.
    """
    handoff=config.get('shared_allocation_handoff',{})
    if str(job_id)!=handoff.get('target'):return False
    predecessor=handoff.get('predecessor','')
    if not re.fullmatch(r'\d+',predecessor):raise ValueError('Invalid service predecessor')
    status=read(Path(root)/'service-status.json',{})
    now=time.time() if now is None else now
    if status.get('job_id')==predecessor and now-status.get('time',0)<60:return True
    try:
        result=subprocess.run(['squeue','-h','-j',predecessor,'-o','%T'],
                              capture_output=True,text=True,timeout=10)
    except (OSError,subprocess.TimeoutExpired):return True
    return result.returncode!=0 or bool(result.stdout.strip())


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,required=True);parser.add_argument('--config',type=Path,required=True)
    args=parser.parse_args()
    if not os.environ.get('SLURM_JOB_ID'):raise RuntimeError('Service requires a Slurm compute allocation')
    cfg=read(args.config);stopping=False;controller=None;bot=None;tunnel=None;last_restart=0
    handoff_done=False;last_handoff_check=0
    settings_path=cfg.get('telegram_settings');settings=read(settings_path,{}) if settings_path else {}
    def stop(signum,frame):
        nonlocal stopping
        stopping=True
        if controller and controller.poll() is None:controller.send_signal(signal.SIGUSR1)
    for sig in (signal.SIGUSR1,signal.SIGTERM,signal.SIGINT):signal.signal(sig,stop)
    while not stopping and predecessor_active(args.root,cfg,os.environ['SLURM_JOB_ID']):
        atomic(args.root/('service-standby-'+os.environ['SLURM_JOB_ID']+'.json'),
               dict(time=time.time(),job_id=os.environ['SLURM_JOB_ID'],
                    predecessor=cfg['shared_allocation_handoff']['predecessor'],
                    state='standby; original service retains daemon and bot'))
        time.sleep(5)
    if stopping:return
    with (args.root/'telegram.log').open('ab',buffering=0) as log:
        try:
            controller=subprocess.Popen([sys.executable,'-m','cdispatch.cli','--root',str(args.root),'--config',str(args.config),'daemon'])
            controller_restarts=[]
            while not stopping:
                if controller.poll() is not None:
                    controller_restarts=[stamp for stamp in controller_restarts if time.time()-stamp<3600]
                    controller_restarts.append(time.time());time.sleep(min(60,3*2**min(len(controller_restarts)-1,5)))
                    if stopping:break
                    controller=subprocess.Popen([sys.executable,'-m','cdispatch.cli','--root',str(args.root),'--config',str(args.config),'daemon'])
                if settings and time.time()-last_restart>=30:
                    if tunnel and tunnel.process.poll() is not None:
                        stop_process(bot);bot=None;tunnel.close();tunnel=None
                    if bot is None or bot.poll() is not None:
                        last_restart=time.time();env=os.environ.copy()
                        if settings.get('proxy'):
                            if tunnel is None:
                                tunnel=ProxyTunnel(settings['proxy']);tunnel.start()
                            env['CDISPATCH_TELEGRAM_PROXY']=tunnel.url
                        bot=subprocess.Popen([sys.executable,'-m','cdispatch.telegram','--root',str(args.root),
                                              '--settings',str(settings_path)],env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=log)
                if cfg.get('service_handoff') and not handoff_done and time.time()-last_handoff_check>=10:
                    last_handoff_check=time.time()
                    try:
                        from .service_handoff import audit
                        handoff_done=audit(args.root,os.environ['SLURM_JOB_ID'],cfg['service_handoff'])
                    except Exception as error:
                        atomic(args.root/'service-handoff-error.json',dict(time=time.time(),error=str(error)))
                atomic(args.root/'service-status.json',dict(time=time.time(),host=socket.gethostname(),job_id=os.environ['SLURM_JOB_ID'],
                       controller_pid=controller.pid,telegram_pid=bot.pid if bot and bot.poll() is None else None,
                       telegram_exit=bot.returncode if bot else None,proxy_running=bool(tunnel and tunnel.process.poll() is None)))
                time.sleep(1)
        finally:
            stop_process(controller);stop_process(bot)
            if tunnel:tunnel.close()
    return controller.returncode or 0


if __name__=='__main__':raise SystemExit(main())
