import json
import os
import subprocess
import urllib.request


################################################################
###
### Proxy Control
###
### Please create an environment variable called BCC_PROXY with the IP address of your proxy server for the EMR commands
###
################################################################

HTTP_PROXY='HTTP_PROXY'
HTTPS_PROXY='HTTPS_PROXY'
BCC_HTTP_PROXY  = 'BCC_HTTP_PROXY'
BCC_HTTPS_PROXY = 'BCC_HTTPS_PROXY'
NO_PROXY='NO_PROXY'
debug=False

def proxy_on(http=True,https=True):
    if http and (BCC_HTTP_PROXY in os.environ):
        os.environ[HTTP_PROXY]  = os.environ[BCC_HTTP_PROXY]
    if https and (BCC_HTTPS_PROXY in os.environ):
        os.environ[HTTPS_PROXY] = os.environ[BCC_HTTPS_PROXY]

def proxy_off():
    if HTTP_PROXY in os.environ:
        del os.environ[HTTP_PROXY]
    if HTTPS_PROXY in os.environ:
        del os.environ[HTTPS_PROXY]

class Proxy:    
    def __init__(self,http=True,https=True):
        self.http = http
        self.https = https

    def __enter__(self):
        proxy_on(http=self.http, https=self.https)
        return self

    def __exit__(self, *args):
        proxy_off()


def get_url(url, context=None, ignore_cert=False):
    if ignore_cert:
        import ssl
        context = ssl._create_unverified_context()

    import urllib.request
    with urllib.request.urlopen(url, context=context) as response:
        return response.read().decode('utf-8')

def get_url_json(url, **kwargs):
    return json.loads(get_url(url, **kwargs))

def user_data():
    return get_url_json("http://169.254.169.254/2016-09-02/user-data/")

def instance_identity():
    return get_url_json('http://169.254.169.254/latest/dynamic/instance-identity/document')

def ami_id():
    return get_url('http://169.254.169.254/latest/meta-data/ami-id')


def show_credentials():
    """This is mostly for debugging"""
    subprocess.call(['printenv'])
    subprocess.call(['aws','configure','list'])

def get_ipaddr():
    return get_url("http://169.254.169.254/latest/meta-data/local-ipv4")

def instanceId():
    return instance_identity()['instanceId']

if __name__=="__main__":
    print("AWS Info:")
    doc = instance_identity()
    for (k,v) in doc.items():
        print("{}: {}".format(k,v))
    print("AMI ID: {}".format(ami_id()))
