# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright 2026 The Kubernetes Authors.
# SPDX-License-Identifier: Apache-2.0
"""HTTP fixture with independent memory, rootfs, and process-start identities."""
import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

token = uuid.uuid4().hex
counter = 0
holds = 0
lock = threading.Lock()


class Probe(BaseHTTPRequestHandler):
    def do_GET(self):
        global holds
        path = urlsplit(self.path)
        if path.path == "/healthz":
            self.send_response(503 if os.path.exists("/probe-not-ready") else 200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if path.path == "/hold":
            duration = min(60, int(parse_qs(path.query).get("seconds", [20])[0]))
            with lock:
                holds += 1
            try:
                time.sleep(duration)
                self.respond()
            finally:
                with lock:
                    holds -= 1
            return
        self.respond()

    def do_POST(self):
        global counter
        path = urlsplit(self.path)
        if path.path == "/not-ready":
            duration = min(30, int(parse_qs(path.query).get("seconds", [8])[0]))
            with open("/probe-not-ready", "w") as out:
                out.write("temporary readiness transition")
            timer = threading.Timer(duration, lambda: os.unlink("/probe-not-ready"))
            timer.daemon = True
            timer.start()
            self.respond()
            return
        with lock:
            counter += 1
            disk = int(open("/probe-counter").read()) + 1 if os.path.exists("/probe-counter") else 1
            with open("/probe-counter", "w") as out:
                out.write(str(disk))
        self.respond()

    def respond(self):
        with lock:
            state = {"token": token, "memory": counter, "disk": int(open("/probe-counter").read()) if os.path.exists("/probe-counter") else 0, "holds": holds}
        data = json.dumps(state).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


ThreadingHTTPServer(("0.0.0.0", 8080), Probe).serve_forever()
