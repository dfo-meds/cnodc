import ipaddress

import zrlog
from werkzeug.middleware.proxy_fix import ProxyFix

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

class TrustedProxyFix:

    def __init__(self, app, trust_from_ips="*", **kwargs):
        self._app = app
        self._proxy = ProxyFix(app, **kwargs)
        self._trusted = trust_from_ips
        self._log = zrlog.get_logger("cnodc.trusted_proxy")
        self._cache = {}

    def _is_upstream_trustworthy(self, environ):
        _ip = environ.get("REMOTE_ADDR")
        try:
            upstream_ip = ipaddress.ip_address(_ip)
        except (ipaddress.AddressValueError, ValueError):
            self._log.warning(f"Upstream address could not be parsed: {_ip}")
            return False
        if self._trusted == "*" or self._trusted is True:
            return True
        elif self._trusted == "" or self._trusted is False or self._trusted is None:
            return False
        elif isinstance(self._trusted, str):
            return self._match_ip_address(upstream_ip, self._trusted)
        else:
            return any(self._match_ip_address(upstream_ip, x) for x in self._trusted)

    def _match_ip_address(self, actual: IPAddress, network_def):
        if network_def not in self._cache:
            try:
                self._cache[network_def] = ipaddress.ip_network(network_def, strict=True)
            except (ipaddress.AddressValueError, ipaddress.NetmaskValueError, ValueError):
                self._log.warning(f"Trusted IP or subnet could not be parsed: {network_def}")
                self._cache[network_def] = None
        if self._cache[network_def] is not None:
            return actual in self._cache[network_def]
        return False

    def __call__(self, environ, *args, **kwargs):
        """Applies proxy configuration only if the upstream IP is allowed."""
        if self._is_upstream_trustworthy(environ):
            self._log.debug("trusting upstream...")
            return self._proxy(environ, *args, **kwargs)
        else:
            self._log.debug("not trusting upstream...")
            return self._app(environ, *args, **kwargs)
