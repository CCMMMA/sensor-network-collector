import json
import threading
import urllib.error
import urllib.parse
import urllib.request


def ws_to_http_base(ws_url: str) -> str:
    parsed = urllib.parse.urlparse(ws_url)
    if parsed.scheme not in ("ws", "wss", "http", "https"):
        raise ValueError(f"Unsupported Signal K URL scheme: {parsed.scheme}")
    scheme = "https" if parsed.scheme in ("wss", "https") else "http"
    netloc = parsed.netloc
    if not netloc:
        raise ValueError(f"Invalid Signal K URL: {ws_url}")
    return f"{scheme}://{netloc}"


def normalize_href(base_http: str, href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if not href.startswith("/"):
        href = "/" + href
    return base_http.rstrip("/") + href


def http_json(url: str, method: str = "GET", payload=None, timeout: int = 10):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {}
        return resp.getcode(), body


def extract_state_token_href(body: dict):
    state = body.get("state")
    token = body.get("token")
    href = body.get("href")

    access = body.get("accessRequest")
    if isinstance(access, dict):
        state = state or access.get("permission") or access.get("state")
        token = token or access.get("token")
        href = href or access.get("href")

    status_code = body.get("statusCode")
    if not state and status_code == 202:
        state = "PENDING"
    return (str(state).upper() if state else ""), (str(token) if token else ""), (str(href) if href else "")


def persist_token(config_path: str, key: str, token: str):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if not isinstance(cfg, dict):
        raise RuntimeError("Config file must contain a JSON object")
    cfg[key] = token
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
        f.write("\n")


class SignalKAccessManager:
    def __init__(
        self,
        server_url: str,
        initial_token: str,
        config_path: str,
        config_token_key: str,
        client_id: str,
        description: str,
        poll_interval_sec: int,
        request_timeout_sec: int,
        client_factory,
        on_client_ready,
        on_client_unavailable,
        on_token_approved,
        logger,
    ):
        self.server_url = str(server_url).strip()
        self.base_http = ws_to_http_base(self.server_url)
        self.initial_token = str(initial_token or "").strip()
        self.config_path = config_path
        self.config_token_key = config_token_key
        self.client_id = str(client_id).strip()
        self.description = str(description or "sensor-network-collector")
        self.poll_interval_sec = int(poll_interval_sec)
        self.request_timeout_sec = int(request_timeout_sec)
        self.client_factory = client_factory
        self.on_client_ready = on_client_ready
        self.on_client_unavailable = on_client_unavailable
        self.on_token_approved = on_token_approved
        self.logger = logger

        self._stop_event = threading.Event()
        self._kick_event = threading.Event()
        self._thread = None
        self._lock = threading.Lock()
        self._state = "INIT"
        self._request_href = ""
        self._candidate_token = self.initial_token

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="signalk-access-manager", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._kick_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def notify_token_invalid(self):
        with self._lock:
            self._candidate_token = ""
            self._request_href = ""
            self._state = "TOKEN_INVALID"
        self.on_client_unavailable()
        self._kick_event.set()

    def _validate_token(self, token: str) -> bool:
        if not token:
            return False
        client = None
        try:
            client = self.client_factory(token)
            client.check_connection()
            self.on_client_ready(client)
            self.logger.info("Signal K token is valid; publishing enabled")
            return True
        except Exception as e:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            self.logger.warning("Signal K token validation failed: %s", e)
            return False

    def _start_request(self):
        url = self.base_http.rstrip("/") + "/signalk/v1/access/requests"
        payload = {"clientId": self.client_id, "description": self.description}
        code, body = http_json(url, method="POST", payload=payload, timeout=self.request_timeout_sec)
        if code == 501:
            raise RuntimeError("Signal K access requests API not implemented (501)")
        if code not in (200, 202):
            raise RuntimeError(f"Signal K access request failed with status {code}: {body}")

        state, token, href = extract_state_token_href(body if isinstance(body, dict) else {})
        if token:
            return "APPROVED", token, ""
        if not href:
            raise RuntimeError(f"Signal K access request missing href: {body}")
        return (state or "PENDING"), "", normalize_href(self.base_http, href)

    def _poll_request(self, href: str):
        code, body = http_json(href, method="GET", payload=None, timeout=self.request_timeout_sec)
        if code not in (200, 202):
            raise RuntimeError(f"Signal K access request poll failed with status {code}: {body}")
        state, token, _ = extract_state_token_href(body if isinstance(body, dict) else {})
        return state or "PENDING", token

    def _approve_token(self, token: str):
        persist_token(self.config_path, self.config_token_key, token)
        self.on_token_approved(token)
        with self._lock:
            self._candidate_token = token
            self._request_href = ""
            self._state = "APPROVED"
        self.logger.info("Signal K access approved; token saved to %s key=%s", self.config_path, self.config_token_key)

        if not self._validate_token(token):
            self.notify_token_invalid()

    def _run(self):
        self.logger.info("Signal K access manager started")
        while not self._stop_event.is_set():
            try:
                with self._lock:
                    token = self._candidate_token
                    href = self._request_href

                if token:
                    if self._validate_token(token):
                        # Token valid: keep current websocket client active until explicitly invalidated
                        while not self._stop_event.is_set():
                            if self._kick_event.wait(timeout=1.0):
                                self._kick_event.clear()
                                break
                        continue
                    with self._lock:
                        self._candidate_token = ""
                        self._request_href = ""
                    self.on_client_unavailable()

                if not href:
                    state, token, href = self._start_request()
                    with self._lock:
                        self._state = state or "PENDING"
                        self._request_href = href
                    if token:
                        self._approve_token(token)
                        continue
                    self.logger.info("Signal K access request pending (clientId=%s)", self.client_id)

                state, token = self._poll_request(href)
                state_up = state.upper()
                if token or state_up == "APPROVED":
                    if not token:
                        self.logger.info("Signal K access state APPROVED without token yet; waiting next poll")
                    else:
                        self._approve_token(token)
                        continue
                elif state_up in ("DENIED", "REJECTED"):
                    with self._lock:
                        self._state = state_up
                        self._request_href = href
                    self.on_client_unavailable()
                    self.logger.error("Signal K access request denied; retrying in %ss", self.poll_interval_sec)
                else:
                    with self._lock:
                        self._state = state_up or "PENDING"
                    self.logger.debug("Signal K access request state=%s", state_up or "PENDING")

                self._kick_event.wait(timeout=self.poll_interval_sec)
                self._kick_event.clear()

            except urllib.error.HTTPError as e:
                self.on_client_unavailable()
                if e.code == 501:
                    self.logger.error("Signal K access requests unsupported (501).")
                else:
                    self.logger.warning("Signal K access HTTP error: %s", e)
                self._kick_event.wait(timeout=self.poll_interval_sec)
                self._kick_event.clear()
            except Exception as e:
                self.on_client_unavailable()
                self.logger.warning("Signal K access manager retry after error: %s", e)
                self._kick_event.wait(timeout=self.poll_interval_sec)
                self._kick_event.clear()

        self.logger.info("Signal K access manager stopped")
