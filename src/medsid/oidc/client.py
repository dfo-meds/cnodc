import copy
from urllib.parse import urljoin, urlencode

import flask
import requests_oauth2client as oa2c
import zirconium as zr
from autoinject import injector
from requests_oauth2client import TokenSerializer, BearerToken
import typing as t

from gcflask.auth import AuthenticationHandler
from gcflask.user import AuthenticatedUser


@injector.injectable
class OAuthClient:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._base_domain = self.config.get(('medsid', 'base_domain'), '').rstrip('/') + '/'
        self._logout_endpoint = self._base_domain + '/end_session',
        self._client = oa2c.OAuth2Client(
            token_endpoint=self._base_domain + '/token',
            authorization_endpoint=self._base_domain + '/authorization',
            userinfo_endpoint=self._base_domain + '/userinfo',
            client_id=self.config.get(('medsid', 'client_id')),
            client_secret=self.config.get(('medsid', 'client_secret')),
            redirect_uri=flask.url_for(self.config.get(('medsid', 'redirect_endpoint'), default='auth.oidc_redirect')),
        )
        self._serializer = TokenSerializer()

    def endpoint_logout(self, token: bytes, logout_redirect: str = None):
        values = {
            'id_token_hint': self._serializer.loads(token).as_dict(),
            'client_id': self._client.client_id,
        }
        if logout_redirect is not None:
            values['post_logout_redirect_uri'] = logout_redirect
        return flask.redirect(
            f"{self._logout_endpoint}?{urlencode(values)}",
            303
        )

    def _with_oauth_error_handling[**P, Q](self, cb: t.Callable[P, Q], *args, **kwargs) -> Q:
        try:
            return cb(*args, **kwargs)
        except Exception as e:
            raise

    def endpoint_authenticate(self):
        return self._with_oauth_error_handling(self._endpoint_authenticate)

    def _endpoint_authenticate(self):
        request = self._client.authorization_request(
            scope='openid profile email urn:medsid:access_management'
        )
        return flask.redirect(request.uri)

    def endpoint_redirect(self) -> dict[str, t.Any]:
        return self._with_oauth_error_handling(self._endpoint_redirect)

    def _endpoint_redirect(self) -> dict[str, t.Any]:
        request = self._client.authorization_request(
            scope='openid profile email urn:medsid:access_management'
        )
        response = request.validate_callback(flask.request.url)
        token = self._client.authorization_code(response)
        userinfo = self._client.userinfo(token)
        data = {
            'bearer_token': self._serializer.dumps(token)
        }
        if userinfo:
            data.update(userinfo)
        return data


class MedsIDHandler(AuthenticationHandler):

    oath_client: OAuthClient = None
    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__('medsid_oidc', True, **kwargs)
        self._app_name = self.config.get(('medsid', 'app_name'), '')

    def login_page(self):
        return self.oath_client.endpoint_authenticate()

    def logout(self):
        return self.oath_client.endpoint_logout(
            flask.session['token'],
            self._auth_manager.logout_success_url()
        )

    def _attempt_login_from_redirect(self, userinfo: dict):
        flask.session['token'] = userinfo.pop('bearer_token')
        flask.session['user_id'] = userinfo['sub']
        flask.session['userinfo'] = userinfo
        return self._load_user(userinfo)

    def load_user(self, user_id):
        if flask.session['user_id'] == user_id:
            return self._load_user(flask.session['userinfo'])
        return None

    def _load_user(self, userinfo: dict):
        filtered = {}
        for x in userinfo:
            if x in ('sub', 'name', 'email', 'urn:medsid:permissions'):
                continue
            elif x.startswith('urn:medsid:'):
                filtered[x[11:]] = userinfo[x]
            else:
                filtered[x] = userinfo[x]
        return AuthenticatedUser(
            unique_id=userinfo.get('sub', ''),
            display_name=userinfo.get('name', ''),
            email=userinfo.get('email', ''),
            permissions=self._extract_permissions(userinfo.get('urn:medsid:permissions', {})),
            **filtered
        )

    def _extract_permissions(self, permissions: dict[str, list[str]]) -> list[str]:
        if permissions and self._app_name in permissions:
            return permissions[self._app_name]
        return []


