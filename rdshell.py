##
##
##

import sys
import os
import time
import requests

try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    urllib3.disable_warnings(urllib3.exceptions.SNIMissingWarning)
    urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)
    import requests
    import fnmatch
    import json
except ImportError:
    print "Basic modules required for the communicating with RunDeck not present"
    sys.exit(10)

try:
    from pyzabbix import ZabbixAPI
except ImportError:
    print "Module required for communicating with Zabbix API not present"
    sys.exit(11)

try:
    import argparse
except ImportError:
    print "Module required for the parsing CMD arguments not present"
    sys.exit(12)

PARSER = argparse.ArgumentParser()
PARSER.add_argument("-V", "--verbose", help="Increase output verbosity",
                    action="store_true")
PARSER.add_argument("-C", "--cfg", help="Configuration file",
                    type=str)
PARSER.add_argument("-T", "--token", help="RunDeck API token",
                    type=str)
PARSER.add_argument("-R", "--rundeck", help="RunDeck URL",
                    type=str)
PARSER.add_argument("-Z", "--zabbix", help="Zabbix URL",
                    type=str)
PARSER.add_argument("-U", "--username", help="Zabbix User",
                    type=str)
PARSER.add_argument("-P", "--password", help="Zabbix Password",
                    type=str)
PARSER.add_argument("--run", help="RunDeck task to run",
                    type=str, required=True)
PARSER.add_argument("-F", "--filter", help="Rundeck execution filter",
                    type=str, default="name:.*")
PARSER.add_argument("-O", "--opt", help="Rundeck execution optional arguments",
                    type=str, default="")
PARSER.add_argument("-E", "--eventid", help="Zabbix EventID",
                    type=str)
PARSER.add_argument("-A", "--ack", help="Acknowledge the event",
                    action="store_true")
PARSER.add_argument("-M", "--msg", help="Message for the event acknowledgement",
                    type=str)

ARG = PARSER.parse_args()

import logging
logging.basicConfig()

if ARG.verbose:
    try:
        import httplib as http_client
        http_client.HTTPConnection.debuglevel = 1
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
    except:
        print "You have an issues with verbose output"
        sys.exit(13)

def RDGET(self, url, cmd, params, headers):
    return requests.get(url, headers=headers, params=params).text
def RDRUN(self, url, cmd, params, headers):
    _, project, jobname = cmd.split("/")
    job = self("project/%s/jobs"%project, jobFilter=jobname)
    if job:
        job=job[0]
        params["_job"] = job
        url = "%s/api/14/job/%s"%(self.url, job["id"])
        return apply(self.__call__, ('run', url), params)
    return None
def RDACTUALRUN(self, url, cmd, params, headers):
    job = params["_job"]
    del params["_job"]
    res = requests.post(url,headers=headers, params={"format":"json"}, json=params).text
    return res

CMD_MAP={
    'system/*':RDGET,
    'scheduler/jobs':RDGET,
    'project/*/jobs':RDGET,
    'exec/*':RDRUN,
    'run':RDACTUALRUN
}

class RDShellConfig:
    def configure(self):
        if self.arg.cfg:
            import ConfigParser
            config = ConfigParser.ConfigParser()
            try:
                config.read([self.arg.cfg,])
            except:
                print "Can not read",self.arg.cfg
                sys.exit(14)
            try:
                self.token = config.get('rundeck','token')
                self.url = config.get('rundeck','url')
                self.zabbix = config.get('zabbix','url')
                self.zabbix_user = config.get('zabbix','username')
                self.zabbix_pass = config.get('zabbix','password')
            except:
                print "Config file",self.arg.cfg,"is incorrect"
                sys.exit(15)



class RDShell(RDShellConfig):
    def __init__(self, **kw):
        self.set_default(kw, "token", None)
        self.set_default(kw, "url", "https://rundeck.placeiq.net")
        self.set_default(kw, "arg", object())
        self.token = self.arg.token
        if os.environ.has_key("RD_TOKEN"):
            self.token = os.environ["RD_TOKEN"]
        self.url = self.arg.rundeck
        self.zabbix = self.arg.zabbix
        self.zabbix_user = self.arg.username
        self.zabbix_pass = self.arg.password
        self.configure()
        for c in [self.token,self.url,self.zabbix,self.zabbix_user,self.zabbix_pass]:
            if c == None:
                print "RDShell parameters in misconfigured"
                sys.exit(17)
    def mkUrl(self, cmd, url=None):
        if url == None:
            return "%s/api/20/%s"%(self.url, cmd)
        else:
            return "%s/%s"%(url, cmd)
    def __call__(self, cmd, url=None, **params):
        params["format"] = "json"
        headers =  {
            "Content-Type": "application/json",
            "X-Rundeck-Auth-Token": self.token
        }
        url = self.mkUrl(cmd,url)
        r = None
        for u in CMD_MAP.keys():
            if fnmatch.fnmatch(cmd, u):
                r = CMD_MAP[u](self, url, cmd, params,headers)
                break
        if r and type(r) in [type(''), type(u'')]:
            r = json.loads(r)
        return r
    def set_default(self, kw, var, default_value):
        if kw.has_key(var):
            setattr(self, var, kw[var])
        else:
            setattr(self, var, default_value)
    def ack(self, res):
        session = requests.Session()
        session.verify=False
        zapi = ZabbixAPI(self.zabbix, session)
        try:
            zapi.login(self.zabbix_user, self.zabbix_pass)
        except KeyboardInterrupt:
            print "Can't login to Zabbix"
            sys.exit(19)
        if self.arg.msg == None:
            self.arg.msg = "AUTO acknowledged at %s"%time.ctime(time.time())
        zapi.event.acknowledge(eventids=[self.arg.eventid],message=self.arg.msg)
        if res["status"] != "running":
            zapi.event.acknowledge(eventids=[self.arg.eventid],message="%s was failed"%self.arg.run)
        else:
            zapi.event.acknowledge(eventids=[self.arg.eventid],message="%s is running on %s"%(self.arg.run, self.arg.filter))
            zapi.event.acknowledge(eventids=[self.arg.eventid],message=res["job"]["permalink"])
    def run(self):
        res = self("exec/%s"%self.arg.run, argString=self.arg.opt, filter=self.arg.filter)
        if self.arg.eventid and self.arg.ack:
            ## Acknowledge the event
            self.ack(res)

def main():
    global ARG
    s = RDShell(arg=ARG)
    s.run()

main()
