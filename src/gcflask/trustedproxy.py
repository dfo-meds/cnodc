""" Improved proxyfix class that only works when the IP address is trusted. """
import ipaddress
import typing as t

import zrlog
from werkzeug.middleware.proxy_fix import ProxyFix


IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


class TrustedProxyFix:
    """ Implements ProxyFix only if the upstream IP is trusted."""

    def __init__(self, app, trust_from_ips: str | bool | None | t.Sequence[str] = "*", **kwargs):
        self._app = app
        self._proxy = ProxyFix(app, **kwargs)
        self._trusted = trust_from_ips
        self._log = zrlog.get_logger("gcflask.trustedproxy")
        self._cache = {}

    def _is_upstream_trustworthy(self, environ: dict[str, str]) -> bool:
        """ Checks if the upstream is trustworthy. """

        _ip = environ.get("REMOTE_ADDR", '')
        try:
            upstream_ip = ipaddress.ip_address(_ip)

        # invalid upstream IP address, certainly invalid.
        except (ipaddress.AddressValueError, ValueError):
            self._log.warning("Upstream address could not be parsed: %s", _ip)
            return False

        # Trust all IP addresses (not recommended)
        if self._trusted == "*" or self._trusted is True:
            return True

        # Trust nothing
        elif self._trusted == "" or self._trusted is False or self._trusted is None:
            return False

        # Trust only one IP address or subnet
        elif isinstance(self._trusted, str):
            return self._match_ip_address(upstream_ip, self._trusted)

        # Trust a list of them
        else:
            return any(self._match_ip_address(upstream_ip, x) for x in t.cast(t.Sequence, self._trusted))

    def _match_ip_address(self, actual: IPAddress, network_def: str) -> bool:
        """ Checks if an IP address matches the network definition. """

        # cache the resolution, because this is slow.
        if network_def not in self._cache:

            try:
                self._cache[network_def] = ipaddress.ip_network(network_def, strict=True)

            except (ipaddress.AddressValueError, ipaddress.NetmaskValueError, ValueError):
                self._log.warning(f"Trusted IP or subnet could not be parsed: {network_def}")
                self._cache[network_def] = None

        # None indicates the parsing failed
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
