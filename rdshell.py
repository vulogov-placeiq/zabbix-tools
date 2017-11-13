##
##
##

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.SNIMissingWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)
import requests
import fnmatch
import json
# import logging
# import httplib as http_client
# http_client.HTTPConnection.debuglevel = 1
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

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
    print repr(params)
    res = requests.post(url,headers=headers, params={"format":"json"}, json=params).text
    return res

CMD_MAP={
    'system/*':RDGET,
    'scheduler/jobs':RDGET,
    'project/*/jobs':RDGET,
    'exec/*':RDRUN,
    'run':RDACTUALRUN
}

class RDShell:
    def __init__(self, **kw):
        self.set_default(kw, "token", None)
        self.set_default(kw, "url", "https://rundeck.placeiq.net")
        if self.token == None:
            raise ValueError, "Authentication credentials not set"
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

if __name__ == '__main__':
    s = RDShell(token="AUoE4zT8Pnl2HakRkzMwfec0FaytO8GA")
    #print s("system/info")
    #print s('scheduler/jobs')
    print s("exec/test/test", argString="-opt value -answer 42", filter="name:delivery-ftp.nym1.placeiq.net")
