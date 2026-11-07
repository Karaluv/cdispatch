"""Bounded streaming JSON parser shared by persistence and read-only clients."""
import json
from pathlib import Path


class JSONStream:
    def __init__(self, stream, chunk_size=65536, max_value_chars=16*1024*1024, pool_keys=False):
        self.stream=stream;self.chunk_size=chunk_size;self.limit=max_value_chars
        self.buffer='';self.position=0;self.eof=False
        self.keys={}
        self.decoder=json.JSONDecoder(object_pairs_hook=self.pooled_object if pool_keys else None)

    def pooled_object(self, pairs):
        # Standard whole-file decoding shares object keys within one scan.
        # Preserve that memory saving across independently decoded task values.
        return {self.keys.setdefault(key,key):value for key,value in pairs}

    def more(self):
        self.buffer=self.buffer[self.position:];self.position=0
        part=self.stream.read(self.chunk_size)
        if not part:self.eof=True
        self.buffer+=part
        if len(self.buffer)>self.limit:raise ValueError('Monitor cache value exceeds bounded reader limit')

    def peek(self):
        while True:
            while self.position<len(self.buffer) and self.buffer[self.position].isspace():self.position+=1
            if self.position<len(self.buffer):return self.buffer[self.position]
            if self.eof:return ''
            self.more()

    def consume(self, token):
        if self.peek()!=token:raise ValueError('Invalid monitor cache JSON structure')
        self.position+=1

    def value(self):
        self.peek()
        while True:
            try:
                value,end=self.decoder.raw_decode(self.buffer,self.position)
                # A number can span chunks: raw_decode accepts its prefix.
                if end==len(self.buffer) and not self.eof:self.more();continue
                if end<len(self.buffer) and self.buffer[end] not in ' \t\r\n,:}]':
                    if self.eof:raise ValueError('Invalid monitor cache JSON value')
                    self.more();continue
                self.position=end
                return value
            except json.JSONDecodeError:
                if self.eof:raise
                self.more()

    def object(self, transform=None):
        self.consume('{');result={}
        if self.peek()=='}':self.consume('}');return result
        while True:
            key=self.value()
            if not isinstance(key,str):raise ValueError('Invalid monitor cache key')
            self.consume(':');value=self.value()
            result[key]=transform(value) if transform else value
            if self.peek()=='}':self.consume('}');return result
            self.consume(',')

    def array(self):
        self.consume('[');result=[]
        if self.peek()==']':self.consume(']');return result
        while True:
            result.append(self.value())
            if self.peek()==']':self.consume(']');return result
            self.consume(',')

    def root(self, task_transform=None):
        self.consume('{');result={}
        if self.peek()=='}':
            self.consume('}')
            if self.peek():raise ValueError('Trailing data in monitor cache')
            return result
        while True:
            key=self.value()
            if not isinstance(key,str):raise ValueError('Invalid monitor cache key')
            self.consume(':')
            result[key]=(self.object(task_transform) if key=='tasks' else
                         self.array() if key=='accounting' else self.value())
            if self.peek()=='}':self.consume('}');break
            self.consume(',')
        if self.peek():raise ValueError('Trailing data in monitor cache')
        return result



def read_scheduler_state(path, default=None):
    """Keep every scheduler field while decoding one historical task at a time.

    Unlike the monitor view, scheduler state must preserve complete immutable
    submissions, commands, attempts and application contracts. Streaming avoids
    holding the entire serialized queue alongside its decoded objects at startup.
    """
    try:
        with Path(path).open(encoding='utf-8') as stream:
            return JSONStream(stream, pool_keys=True).root(task_transform=None)
    except FileNotFoundError:
        return default
