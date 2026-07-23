from autoinject import injector

from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.auth import AuthenticationManager
from gcflask.security import security_check

auth = MultiLanguageBlueprint('auth', __name__, url_prefix="/medsid")


@auth.route('/login', methods=['GET', 'POST'])
@security_check(anonymous_only=True)
@injector.inject
def login(am: AuthenticationManager = None):
    return am.endpoint_login()


@auth.route('/login/<handler>', methods=['GET', 'POST'])
@security_check(anonymous_only=True)
@injector.inject
def login_for_handler(handler: str, am: AuthenticationManager = None):
    return am.endpoint_login_for_handler(handler)


@auth.route('/logout')
@security_check(authenticated_only=True)
@injector.inject
def logout(am: AuthenticationManager = None):
    return am.endpoint_logout()
