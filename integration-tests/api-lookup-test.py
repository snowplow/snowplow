#!/usr/bin/env python2

import BaseHTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
import base64
import json


class AuthHandler(SimpleHTTPRequestHandler):
    """ Mock HTTP Server for API Lookup Enrichment Integration Test suite"""

    post_request_counter = 0

    def do_AUTHHEAD(self):
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm=\"API Lookup Enrichment test. User: snowplower, password: supersecret\"')
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_POST(self):
        ''' Present frontpage with user authentication. '''
        self.post_request_counter += 1
        self.protocol_version='HTTP/1.1'
        auth = self.headers.getheader('Authorization')
        if self.path.startswith("/guest"):
            self.send_response(200)
            response = self.generate_response("POST")
            self.send_header('Content-length', len(response))
            self.end_headers()
            self.wfile.write(response)
        elif auth is None:
            self.do_AUTHHEAD()
            self.wfile.write('no auth header received')
        elif auth == 'Basic ' + base64.b64encode('snowplower:supersecret'):
            response = self.generate_response("POST", auth)
            self.send_response(200)
            self.send_header('Content-length', len(response))
            self.end_headers()
            self.wfile.write(response)
        else:
            self.do_AUTHHEAD()
            self.wfile.write(self.headers.getheader('Authorization'))
            self.wfile.write('not authenticated')

    def do_GET(self):
        if self.path.startswith("/guest"):
            self.send_response(200)
            response = self.generate_response("GET")
            self.end_headers()
            self.wfile.write(response)
        else:
            self.wfile.write('not authenticated')

    def generate_response(self, method, auth=None):
        if auth is not None:
            userpass = base64.decodestring(auth[6:])
            response = {
                "rootNull": None,
                "data": {
                    "firstKey": None,
                    "lookupArray": [
                        {
                            "path": self.path,
                            "auth_header": userpass,
                            "method": method,
                            "request": self.post_request_counter
                        }, {
                            "path": self.path,
                            "request": self.post_request_counter
                        }, {}
                    ]
                }
            }
        else:
            response = {
                "message": "unauthorized",
                "path": self.path,
                "method": method

            }
        return json.dumps(response)


if __name__ == '__main__':
    BaseHTTPServer.test(AuthHandler, BaseHTTPServer.HTTPServer)
