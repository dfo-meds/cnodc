import flask
from autoinject import injector

from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.auth import AuthenticationManager
from medsid.oidc.server import OpenIDServer

auth = MultiLanguageBlueprint('auth', __name__)


@auth.route('/.well-known/openid-configuration')
@injector.inject
def oidc_configuration(server: OpenIDServer = None):
    return server.endpoint_config()

@auth.route('/authorization')
@injector.inject
def oidc(server: OpenIDServer = None):
    return server.endpoint_authorization()


@auth.route('/authorization/redirect')
@injector.inject
def oidc_redirect(server: OpenIDServer = None):
    return server.endpoint_authorization()


@auth.route('/token')
@injector.inject
def oidc_token(server: OpenIDServer = None):
    return server.endpoint_token()


@auth.route('/registration')
@injector.inject
def oidc_registration(server: OpenIDServer = None):
    return server.endpoint_client_registration()


@auth.route('/userinfo')
@injector.inject
def oidc_userinfo(server: OpenIDServer = None):
    return server.endpoint_userinfo()

@auth.route('/end_session')
@injector.inject
def oidc_end_session(server: OpenIDServer = None):
    return server.endpoint_user_logout()



@auth.route('/login')
@injector.inject
def login(am: AuthenticationManager = None):
    return am.endpoint_login()


@auth.route('/login/<handler>')
@injector.inject
def login_for_handler(handler: str, am: AuthenticationManager = None):
    return am.endpoint_login_for_handler(handler)


@auth.route('/authorization/redirect')
@injector.inject
def oidc_redirect(server: OpenIDServer = None):
    return server.endpoint_client_redirect()


@auth.route('/logout')
@injector.inject
def logout(am: AuthenticationManager = None):
    return am.endpoint_logout()

