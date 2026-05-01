import datetime
import enum

from autoinject import injector
import zirconium as zr
import zrlog
import flask
import flask_login

import medsutil.secure
from medsutil.awaretime import AwareDateTime


class PolicyArea(enum.Enum):
    SCRIPT = 'script-src'
    STYLE = 'style-src'
    IMAGE = 'image-src'
    DEFAULT = 'default-src'


@injector.injectable
class CSPRegistry:

    cfg: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._csp_policies = {
            'default-src': [],
            'script-src': [],
            'style-src': [],
            'img-src': []
        }
        self._can_cache_page = True
        self._cache_time: int = self.cfg.as_int(("gcflask", "csp", "cache_time"), default=300)
        self._static_cache_time: int = self.cfg.as_int(("gcflask", "csp", "static_cache_time"), default=7200)
        self._allow_caching_default = self.cfg.as_bool(("gcflask", "csp", "allow_caching"), default=True)
        self._is_static_resource = False
        self._log = zrlog.get_logger("gcflask.csp")
        self._enabled = self.cfg.as_bool(("gcflask", "csp", "enabled"), default=False)
        self._report_enabled = self.cfg.as_bool(("gcflask", "csp", "report_enabled"), default=True)
        self._upstream_enabled = self.cfg.as_bool(("gcflask", "csp", "upstream_enabled"), default=False)

    def set_static(self, cache_time: int | None = None):
        self._cache_time = cache_time if cache_time is not None else self._static_cache_time
        self._is_static_resource = True

    def allow_caching(self, response: flask.Response = None) -> bool:
        return False
        # If caching is disabled, lets just not worry about it
        if not self._allow_caching_default:
            self._log.debug("Caching disabled")
            return False
        # No caching if we used a nonce or anything like it
        if not self._can_cache_page:
            self._log.debug("Caching disabled, nonce used")
            return False
        if response and response.status_code not in (200, 301):
            self._log.debug("Non-success error code, caching disabled")
            return False
        # No caching for methods other than GET or HEAD
        if flask.request.method not in ('GET', 'HEAD'):
            self._log.debug("Request method was not GET or HEAD, caching disabled")
            return False
        # Otherwise we can probably cache this resource
        return True

    def allow_shared_caching(self, response: flask.Response = None) -> bool:
        # We can cache static resources even if it is an authenticated request since
        # there is no extra information
        if self._is_static_resource:
            return True
        # No caching if we used an authorization header in the request
        if flask.request.headers.get("Authorization", default=None) is not None:
            self._log.debug("Authorization header present, shared cache disabled")
            return False
        # No caching if the user is authenticated
        if flask_login.current_user.is_authenticated:
            self._log.debug("Authenticated page, shared cache disabled")
            return False
        return True

    def set_cache_time(self, time: int):
        self._cache_time = time

    def no_cache(self):
        self._can_cache_page = False

    def reset_csp_policy(self, policy_area: PolicyArea | str):
        pa: str = policy_area.value if isinstance(policy_area, PolicyArea) else policy_area
        if pa in self._csp_policies:
            self._csp_policies[pa] = []

    def add_csp_policy(self, policy_area: PolicyArea | str, directive: str):
        pa: str = policy_area.value if isinstance(policy_area, PolicyArea) else policy_area
        if pa not in self._csp_policies:
            self._csp_policies[pa] = []
        if directive not in self._csp_policies[pa]:
            self._csp_policies[pa].append(directive)

    def add_headers(self, response: flask.Response):
        csp_headers = [
            f"{header} 'self' {' '.join(str(x) for x in self._csp_policies[header])}"
            for header in self._csp_policies
        ]
        if self._enabled:
            response.headers.set("Content-Security-Policy", ";".join(csp_headers))
        if self._report_enabled:
            response.headers.set("Content-Security-Policy-Report-Only", ";".join(csp_headers))
        if self._upstream_enabled:
            for policy_area in self._csp_policies:
                if policy_area == "default-src":
                    continue
                response.headers.set(
                    f"X-Upstream-CSP-{policy_area}",
                    " ".join(str(x) for x in self._csp_policies[policy_area])
                )
        if not self.allow_caching(response) or self._cache_time == 0:
            response.cache_control.max_age = 0
            response.cache_control.no_cache = True
            response.cache_control.no_store = True
            response.cache_control.must_revalidate = True
            response.cache_control.proxy_revalidate = True
        else:
            allow_share = self.allow_shared_caching(response)
            response.cache_control.no_cache = None
            response.cache_control.no_store = None
            response.cache_control.must_revalidate = None
            response.cache_control.proxy_revalidate = None
            response.cache_control.max_age = self._cache_time
            response.expires = (AwareDateTime.utcnow() + datetime.timedelta(seconds=self._cache_time))
            response.cache_control.private = (not allow_share) or None
            response.cache_control.public = allow_share or None

    def build_nonce(self) -> str:
        return medsutil.secure.generate_csp_nonce()


@injector.inject
def csp_nonce(policy_area: str, cspr: CSPRegistry = None):
    nonce = cspr.build_nonce()
    cspr.no_cache()
    cspr.add_csp_policy(policy_area, f"'nonce-{nonce}'")
    return nonce


@injector.inject
def csp_allow(policy_area: str, hostname: str, cspr: CSPRegistry = None):
    cspr.add_csp_policy(policy_area, hostname)
    return ""
