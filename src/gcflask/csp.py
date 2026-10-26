"""
Provides streamlined support for adding and managing content security policy identifiers.

This is very boring web security stuff. See https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CSP

But basically, CSP (Content Security Policy) controls which resources can be loaded by the browser to prevent
attacks.

A CSP is broken down into policy areas:
- default-src is the default if others aren't set
- img-src is for images
- style-src is for style tags
- script-src is for script tags
- object-src for object and embed
- base-uri for the base element

The policy area contains individual directives, which can be:
- 'nonce-X' for a nonce (X is a one-time use specific key that is allowed to be loaded and must be added on to the tag too)
- 'domain.com' for an entire domain (can use * as a wildcard)
- 'self' for content within the same HTML document
- 'none' for nothing at all
- 'sha256-X', 'sha385-X' or 'sha512-X' for hashes where X is the hash
- 'https:' to force HTTPS connections
- 'strict-dynamic' to allow a third-party script to load its own scripts

There are some additional options for some policy areas

"""
import enum

from autoinject import injector, auto
import zirconium as zr
import zrlog

import medsutil.secure


class PolicyArea(enum.Enum):
    """ Represents a CSP policy area"""

    SCRIPT = 'script-src'
    """ Javascript """

    STYLE = 'style-src'
    """ CSS """

    IMAGE = 'img-src'
    """ Images """

    OBJECT = 'object-src'
    """ Objects and embeds """

    BASE = 'base-uri'
    """ Base tag"""

    FRAME = 'frame-ancestors'
    """ Iframes """

    CHILD = 'child-src'
    """ Nested browsing and worker execution contexts """

    CONNECT = 'connect-src'
    """ XHR, fetch requests, web sockets """

    FONT = 'font-src'
    """ Fonts """

    MANIFEST = 'manifest-src'
    """ Application manifests """

    MEDIA = 'media-src'
    """ Media """

    SANDBOX = 'sandbox'
    """ Sandbox directives """

    FORM = "form-action"
    """ Forms """

    DEFAULT = 'default-src'
    """ Other, if not specified """


@injector.injectable
class CSPRegistry:
    """ Manages Content Security Policy headers and caching management. """

    cfg: zr.ApplicationConfig = auto()

    @injector.construct
    def __init__(self):
        self._csp_policies = {
            x.value: []
            for x in PolicyArea
        }
        self._log = zrlog.get_logger("gcflask.csp")
        self._enabled = self.cfg.as_bool(("gcflask", "csp", "enabled"), default=False)
        self._report_enabled = self.cfg.as_bool(("gcflask", "csp", "report_enabled"), default=True)
        self._upstream_enabled = self.cfg.as_bool(("gcflask", "csp", "upstream_enabled"), default=False)
        self._upgrade_insecure_requests = self.cfg.as_bool(("gcflask", "csp", "upgrade_insecure_requests"), default=False)
        self._report_uri: str | None = self.cfg.as_str(("gcflask", "csp", "report_to"), default=None)

    def reset_csp_policy(self, policy_area: PolicyArea | str):
        """ Clears all policies stored under the given area. """
        pa: str = policy_area.value if isinstance(policy_area, PolicyArea) else policy_area
        if pa in self._csp_policies:
            del self._csp_policies[pa]

    def add_csp_policy(self, policy_area: PolicyArea | str, directive: str):
        """ Adds a new content security policy under the given area

            :param policy_area: The policy area to add it under.
            :param directive: A CSP directive
         """
        pa: str = policy_area.value if isinstance(policy_area, PolicyArea) else policy_area
        if pa not in self._csp_policies:
            self._csp_policies[pa] = []
        if directive not in self._csp_policies[pa]:
            self._csp_policies[pa].append(directive)

    def get_headers(self) -> dict[str, str]:
        """Builds HTTP headers to add a response based on CSP and caching policies. """

        headers = {}

        # Build CSP headers from directives
        csp_headers = [
            ' '.join((header, *(f"'{x}'" for x in directives)))
            for header, directives in self._csp_policies.items()
            if directives
        ]

        # Upgrade insecure requests
        if self._upgrade_insecure_requests:
            csp_headers.append("upgrade-insecure-requests")

        # Report URI
        if self._report_uri:
            csp_headers.append(f"report-to {self._report_uri}")
            # this one is deprecated but still recommended
            csp_headers.append(f"report-uri {self._report_uri}")

        if self._enabled:
            headers["Content-Security-Policy"] = ";".join(csp_headers)

        if self._report_enabled:
            headers["Content-Security-Policy-Report-Only"] = ";".join(csp_headers)

        # Passes CSP policies to upstream server for reasons I forget (have to look at nginx configuration)
        if self._upstream_enabled:
            for policy_area, directives in self._csp_policies.items():
                if policy_area == "default-src" or not directives:
                    continue
                # TODO: review if this should have quotes
                headers[f"X-Upstream-CSP-{policy_area}"] = " ".join(str(x) for x in directives)

        return headers


@injector.inject
def csp_nonce(policy_area: str, cspr: CSPRegistry = auto()) -> str:
    """ Builds a Content-Security-Policy nonce, registers it, and returns the nonce."""
    nonce = medsutil.secure.generate_csp_nonce()
    cspr.add_csp_policy(policy_area, f"'nonce-{nonce}'")
    return nonce


@injector.inject
def csp_allow(policy_area: str, directive: str, cspr: CSPRegistry = auto()):
    """ Adds a CSP for a given directive. """
    cspr.add_csp_policy(policy_area, directive)
    return ""
