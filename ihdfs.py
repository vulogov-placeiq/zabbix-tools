#!/usr/bin/env python

import sys
import os
import time
import re
from multiprocessing import Process, Queue

try:
    import pyhdfs
except ImportError:
    print "HDFS module required for this tool is not installed"
    sys.exit(10)

try:
    import argparse
except ImportError:
    print "Module required for the parsing CMD arguments not present"
    sys.exit(12)

PARSER = argparse.ArgumentParser()
PARSER.add_argument("-V", "--verbose", help="Increase output verbosity",
                    action="store_true")
PARSER.add_argument("-n", "--nodemanager", help="Reference to a Nodemanager",
                    action="append")
# PARSER.add_argument("-N", "--nodemanagers",
#                     help="Reference to a pattern of Nodemanager names", action="append")
PARSER.add_argument("-W", "--workers", help="Number of processing workers",
                    type=int, default=10)
PARSER.add_argument("-T", "--timeout", help="Timeout for HADOOP",
                    type=float, default=30.0)
PARSER.add_argument("--tries", help="Maximum number of communication attempts",
                    type=int, default=100)
PARSER.add_argument("--delay", help="Re-try delay",
                    type=float, default=30.0)
PARSER.add_argument("-U", "--user", help="HADOOP User",
                    type=str, required=True)
PARSER.add_argument("--Execute", help="Force execution of the default actions. Default: NO-OP",
                    action="store_true")
PARSER.add_argument("-F", "--filter", help="add filtering for the files found",
                    type=str)
PARSER.add_argument("-R", "--run", help="Name of the action to execute for each element",
                    action="append")
PARSER.add_argument('PATH', action='append', nargs='+')
ARG = PARSER.parse_args()

class iHDFS_Filter:
    def __init__(self):
        self.make_filters()
    def make_filters(self):
        pass
    def run_filter(self, filter, _f, s):
        return True

class iHDFS_Display:
    def __init__(self):
        self.actions["display"] = (self.display, False)
    def display(self, _f, s):
        print _f

class iHDFS(iHDFS_Filter, iHDFS_Display):
    def __init__(self, **kw):
        self.actions = {}
        for c in self.__class__.__bases__:
            c.__init__(self)
        iHDFS_Display.__init__(self)
        self.arg = kw["arg"]
        self.fs = pyhdfs.HdfsClient(hosts=kw['hosts'], user_name=kw['user_name'])
        self.queue = Queue()
        self.arbiter()
        self.filter = None
        if self.arg.filter != None:
            self.filter = dict(re.findall(r'(\S+)=(".*?"|\S+)', self.arg.filter))
    def run(self, path=None):
        if not path:
            self.queue.put(path)
        else:
            for d, subd, files in self.fs.walk(path):
                for f in files:
                    _f = "%s/%s"%(d, f)
                    self.queue.put(_f)
    def arbiter(self):
        self.run_action = []
        for r in self.arg.run:
            r = r.lower()
            if r in self.actions.keys():
                self.run_action.append(self.actions[r])
    def worker(self, *args):
        while True:
            _f = self.queue.get()
            if _f is None:
                break
            try:
                s = self.fs.get_file_status(_f)
            except:
                continue
            if self.filter:
                for fi in self.filter.keys():
                    if not self.run_filter(fi, _f, s):
                        continue
            for a, isExec in self.run_action:
                if (isExec == True and self.arg.Execute) or isExec == False:
                    apply(a, (_f, s))
    def start(self):
        self.workers = [Process(target=self.worker, args=(i,))
                        for i in xrange(self.arg.workers)]
        for w in self.workers:
            w.start()
    def stop(self):
        for w in self.workers:
            self.queue.put(None)






def main():
    global ARG
    if not ARG.nodemanager or len(ARG.nodemanager) == 0:
        print "You must specify at least 1 Nodemanager"
        sys.exit(13)
    if not ARG.PATH or len(ARG.PATH) == 0 or (len(ARG.PATH) == 1 and len(ARG.PATH[0]) == 0):
        print "You must specify at least 1 HDFS PATH"
        sys.exit(13)
    HDFS_NM=",".join(ARG.nodemanager)
    HDFS_PATH=ARG.PATH[0]
    f = iHDFS(hosts=HDFS_NM, user_name=ARG.user, arg=ARG)
    f.start()
    for p in HDFS_PATH:
        try:
            s = f.fs.list_status(p)
        except pyhdfs.HdfsFileNotFoundException, msg:
            print "HDFS error:",msg
            continue
        f.run(p)
    f.stop()

main()