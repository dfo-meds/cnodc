
.. envvar:: GCAPP_CONFIG_DIRECTORIES

    A semi-colon delimited list of directories to search for configuration files.

.. envvar:: PROMETHEUS_MULTIPROC_DIR

    A directory to store prometheus metrics files between processes



.. confval:: gcapp.plugins
    :default: ``{}``

    A mapping of plugins to priorities (either integers or the string "none" to prevent them from loading)

.. confval:: gcflask.authentication.handlers
    :default: ``{}``

    A mapping of handler names to fully-qualified class names that implement :py:class:`gcflask.auth.BaseAuthenticationHandler`.

.. confval:: gcflask.authentication.login_required
    :default: auth.login

    The default Flask handler for when a login is required.

.. confval:: gcflask.authentication.login_success
    :default: base.home

.. confval:: gcflask.authentication.logout_success
    :default: base.home

.. confval:: gcflask.authentication.unauthorized
    :default: base.home

.. confval:: gcflask.authentication.referrer_failed
    :default: bash.splash

.. confval:: gcflask.authentication.login_for_handler
    :default: auth.login_for_handler

.. confval:: gcflask.gcp.enabled
    :default: ``False``

.. confval:: gcflask.csp.report_enabled
    :default: ``True``

.. confval:: gcflask.csp.upstream_enabled
    :default: ``False``

.. confval:: gcflask.csp.upgrade_insecure_requests
    :default: ``False``

.. confval:: gcflask.csp.report_to
    :default: ``None``

.. confval:: flask.PERMANENT_SESSION_LIFETIME
    :default: 44640

.. confval:: flask.SECRET_KEY

.. confval:: gcflask.proxy_fix.enabled
    :default: ``False``

.. confval:: gcflask.proxy_fix.trusted_upstreams
    :default:
    semi-colon delimited
* to accept all
blank to accept none
str or list of subnet masks, see ipaddress.ip_network


.. confval:: gcflask.proxy_fix.x_for
    :default: ``1``

.. confval:: gcflask.proxy_fix.x_proto
    :default: ``1``

.. confval:: gcflask.proxy_fix.x_host
    :default: ``1``

.. confval:: gcflask.proxy_fix.x_port
    :default: ``1``

.. confval:: gcflask.proxy_fix.x_prefix
    :default: ``1``

.. confval:: gcflask.path_prefix

.. confval:: gcflask.i18n_url_files
    :default: ``[]``

list of i18n url mapping files

.. confval:: gcflask.security.allowed_hosts
    :default: ``[]``

list of hosts that a request can be from

.. confval:: gcflask.security.require_https
    :default: ``False``


.. confval:: gcflask.security.verify_referrer_for_get
    :default:: ``False``


