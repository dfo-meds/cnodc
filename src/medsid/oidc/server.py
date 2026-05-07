import json

import flask
import flask_login
import psycopg2
import zrlog

from autoinject import injector
import zirconium as zr
from jwkest.jwk import rsa_load, RSAKey
from oic.oic import TokenErrorResponse, EndSessionRequest
from pyop.exceptions import InvalidAuthenticationRequest, InvalidClientAuthentication, OAuthError, BearerTokenError, \
    InvalidAccessToken
from pyop.message import AuthorizationRequest
from pyop.provider import Provider
from pyop.authz_state import AuthorizationState
from pyop.storage import StorageBase
from pyop.subject_identifier import HashBasedSubjectIdentifierFactory
from pyop.userinfo import Userinfo
from pyop.util import should_fragment_encode

from gcflask.auth import AuthenticationManager
from medsutil.awaretime import AwareDateTime


@injector.injectable
class OpenIDServer:

    config: zr.ApplicationConfig = None
    auth: AuthenticationManager = None

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger("medsid.openidserver")
        self._dsn = self.config.get(("medsid", "dsn"), None)
        self._base_domain = self.config.get(('medsid', 'base_domain'), '').rstrip('/') + '/'
        self._db = psycopg2.connect(self._dsn)
        self._signing_key_file = self.config.get(("medsid", "signing_key_file"), None)
        key = RSAKey(key=rsa_load(self._signing_key_file), use='sig', alg='RS256')
        self._provider = Provider(
            key,
            {
                'issuer': self._base_domain,
                'authorization_endpoint': self._base_domain + '/authorization',
                'token_endpoint': self._base_domain + '/token',
                'userinfo_endpoint': self._base_domain + '/userinfo',
                'registration_endpoint': self._base_domain + '/registration',
                'end_session_endpoint': self._base_domain + '/',
                'response_types_supported': ['code', 'id_token token'],
                'id_token_signing_alg_values_supported': [key.alg],
                'response_modes_supported': ['fragment', 'query'],
                'subject_types_supported': ['public', 'pairwise'],
                'grant_types_supported': ['authorization_code', 'implicit'],
                'claim_types_supported': ['normal'],
                'claims_parameter_supported': True,
                'claims_supported': [
                    'sub', 'username', 'name', 'email', 'email_verified', 'locale',
                    'urn:medsid:permissions',
                    'urn:medsid:last_success',
                    'urn:medsid:last_error',
                    'urn:medsid:last_success_ip',
                    'urn:medsid:last_error_ip',
                    'urn:medsid:total_errors',
                ],
                'request_parameter_supported': False,
                'request_uri_parameter_supported': False,
                'scopes_supported': [
                    'openid', 'profile', 'email',
                    'urn:medsid:access_management'
                ],
            },
            AuthorizationState(
                HashBasedSubjectIdentifierFactory(
                    self.config.get(('medsid', 'secret_hash'), default='12345'),
                ),
                PostgresWrapper(self._db, 'medsid', 'auth_codes'),
                PostgresWrapper(self._db, 'medsid', 'access_tokens'),
                PostgresWrapper(self._db, 'medsid', 'refresh_tokens'),
                PostgresWrapper(self._db, 'medsid', 'subject_tokens'),
                authorization_code_lifetime=self.config.as_int(('medsid', 'auth_code_lifetime_seconds'), 300),
                access_token_lifetime=self.config.as_int(('medsid', 'access_token_lifetime_seconds'), 60*60*24),
                refresh_token_lifetime=self.config.as_int(('medsid', 'refresh_token_lifetime_seconds'), 60*60*24*265),
                refresh_token_threshold=self.config.as_int(('medsid', 'refresh_token_threshold_seconds'), 300),
            ),
            PostgresClientInfo(self._db),
            Userinfo(PostgresUserInfo(self._db)),
            extra_scopes={
                'urn:medsid:access_management': [
                    'urn:medsid:permissions',
                    'urn:medsid:last_success',
                    'urn:medsid:last_error',
                    'urn:medsid:last_success_ip',
                    'urn:medsid:last_error_ip',
                    'urn:medsid:total_errors',
                ]
            }
        )

    def _with_oauth_error_handling(self, cb, *args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except OAuthError as ex:
            headers = {}
            self._log.exception(ex)
            if isinstance(ex, InvalidAuthenticationRequest):
                error_url = ex.to_error_url()
                if error_url:
                    return flask.redirect(error_url, code=303)
            error_resp = TokenErrorResponse(error=ex.oauth_error, error_description=str(ex))
            error_code = 400
            if isinstance(ex, InvalidClientAuthentication):
                error_code = 401
                headers['WWW-Authenticate'] = 'Basic'
            elif isinstance(ex, (BearerTokenError, InvalidAccessToken)):
                error_code = 401
                headers['WWW-Authenticate'] = 'Bearer'
            response = flask.jsonify(
                error_resp.to_dict(1)
            )
            if headers:
                response.headers.update(headers)
            return response, error_code
        except Exception as ex:
            return flask.jsonify({'error': str(ex)}), 500

    def endpoint_config(self):
        return self._with_oauth_error_handling(self._endpoint_config)

    def _endpoint_config(self):
        return flask.jsonify(
            self._provider.provider_configuration.to_dict(1)
        )

    def endpoint_client_registration(self):
        return self._with_oauth_error_handling(self._endpoint_client_registration)

    def _endpoint_client_registration(self):
        response = self._provider.handle_client_registration_request(
            flask.request.get_data(as_text=True)
        )
        return flask.jsonify(response.to_dict(1)), 201

    def endpoint_userinfo(self):
        return self._with_oauth_error_handling(self._endpoint_userinfo)

    def _endpoint_userinfo(self):
        response = self._provider.handle_userinfo_request(
            flask.request.get_data(as_text=True),
            flask.request.headers
        )
        return flask.jsonify(response.to_dict(1))

    def endpoint_authorization(self):
        return self._with_oauth_error_handling(self._endpoint_authorization)

    def _endpoint_authorization(self):
        flask.session['_auth_request'] = self._provider.parse_authentication_request(
            flask.request.get_data(as_text=True)
        ).to_dict()
        return self.auth.endpoint_login(
            flask.url_for('auth.oidc_redirect')
        )

    def endpoint_client_redirect(self):
        if '_auth_request' not in flask.session:
            return flask.abort(400)
        return self._with_oauth_error_handling(self._endpoint_client_redirect)

    def _endpoint_client_redirect(self):
        auth_request = AuthorizationRequest().from_dict(flask.session['_auth_request'])
        auth_response = self._provider.authorize(
            auth_request,
            str(flask_login.current_user.get_id())
        )
        return flask.redirect(
            auth_response.request(auth_request['redirect_uri'], should_fragment_encode(auth_request)),
            303
        )

    def endpoint_token(self):
        return self._with_oauth_error_handling(self._endpoint_token)

    def _endpoint_token(self):
        return flask.jsonify(
            self._provider.handle_token_request(
                flask.request.get_data(as_text=True),
                flask.request.headers
            ).to_dict(1)
        )

    def endpoint_user_logout(self):
        return self._with_oauth_error_handling(self._endpoint_user_logout)

    def _endpoint_user_logout(self):
        end_request = EndSessionRequest().deserialize(flask.request.get_data(as_text=True))
        self._provider.logout_user(
            flask_login.current_user.get_id(),
            end_request
        )
        redirect = self._provider.do_post_logout_redirect(end_request)
        redirect2 = self.auth.endpoint_logout()
        if redirect:
            return flask.redirect(redirect, 303)
        return redirect2


class PostgresClientInfo(StorageBase):

    def __init__(self, db: psycopg2._psycopg.connection):
        self._db = db

    def __getitem__(self, item):
        with self._db.cursor() as cur:
            cur.execute("SELECT oauth_config FROM applications WHERE app_str_id = %s", [
                item
            ])
            res = cur.fetchone()
            if not res:
                raise KeyError(item)
            return json.loads(res[0]) if res[0] else {}

    def __setitem__(self, key, value):
        with self._db.cursor() as cur:
            cur.execute(f"INSERT INTO applications (app_str_id, oauth_config) VALUES (%s, %s, %s) ON CONFLICT (app_str_id) DO UPDATE oauth_config=EXCLUDED.oauth_config", [
                key, json.dumps(value)
            ])
            self._db.commit()

    def __contains__(self, item):
        with self._db.cursor() as cur:
            cur.execute("SELECT 1 FROM applications WHERE app_str_id = %s", [item])
            res = cur.fetchone()
            return res is not None

    def __delitem__(self, key):
        with self._db.cursor() as cur:
            cur.execute(f"DELETE FROM applications WHERE app_str_id = %s", [key])
            self._db.commit()

    def pack(self, value):
        raise NotImplementedError

    def items(self):
        with self._db.cursor() as cur:
            cur.execute(f"SELECT app_str_id, oauth_config FROM applications")
            while rows := cur.fetchmany(25):
                for r in rows:
                    yield r[0], json.loads(r[1])


class PostgresUserInfo:

    def __init__(self, db: psycopg2._psycopg.connection):
        self._db = db

    def __getitem__(self, item):
        with self._db.cursor() as cur:
            cur.execute("SELECT user_id, username, name, email, email_verified, language_pref, last_success, last_error, last_success_ip, last_error_ip, total_errors FROM users WHERE user_id = %s", [item])
            res = cur.fetchone()
            if not res:
                raise KeyError(item)
            permissions: dict[str, set[str]] = {}
            cur.execute("SELECT p.name, a.name FROM user_role ur JOIN roles_permissions rp ON rp.role_id = ur.role_id JOIN permissions p ON p.permission_id = rp.permission_id JOIN applications a ON a.app_id = p.app_id WHERE ur.user_id = %s", [item])
            while rows := cur.fetchmany(25):
                for row in rows:
                    if row[1] not in permissions:
                        permissions[row[1]] = set()
                    permissions[row[1]].add(row[0])
            cur.execute("SELECT p.name, a.name FROM user_role ur JOIN roles_app_roles rap ON rap.role_id = ur.role_id JOIN permissions_application_roles par ON par.app_role_id = rap.app_role_id JOIN permissions p ON p.permission_id = par.permission_id JOIN applications a ON a.app_id = p.app_id WHERE ur.user_id = %s", [item])
            while rows := cur.fetchmany(25):
                for row in rows:
                    if row[1] not in permissions:
                        permissions[row[1]] = set()
                    permissions[row[1]].add(row[0])
            return {
                'user_id': res[0],
                'username': res[1],
                'name': res[2],
                'email': res[3],
                'email_verified': res[4],
                'locale': res[5],
                'urn:medsid:last_success': AwareDateTime.from_datetime(res[6]).isoformat(),
                'urn:medsid:last_error': AwareDateTime.from_datetime(res[7]).isoformat(),
                'urn:medsid:last_success_ip': res[8],
                'urn:medsid:last_error_ip': res[9],
                'urn:medsid:total_errors': res[10],
                'urn:medsid:permissions': {
                    k: list(permissions[k])
                    for k in permissions
                }
            }

    def __contains__(self, item):
        with self._db.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE user_id = %s", [item])
            res = cur.fetchone()
            return res is not None

class PostgresWrapper(StorageBase):
    def __init__(self, conn: psycopg2._psycopg.connection, table_name: str, ttl=None):
        self._db = conn
        if ttl is None or (isinstance(ttl, int) and ttl >= 0):
            self._ttl = ttl
        else:
            raise ValueError("TTL must be a non-negative integer or None")

        self._table_name = table_name
        with self._db.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self._table_name} (key VARCHAR(256), data JSON, last_modified TIMESTAMP)")

    def __setitem__(self, key, value):
        with self._db.cursor() as cur:
            cur.execute(f"INSERT INTO {self._table_name} (key, data, last_modified) VALUES (%s, %s, %s) ON CONFLICT (key) DO UPDATE SET data=EXCLUDED.data, last_modified=EXCLUDED.last_modified", [
                key, json.dumps(value), AwareDateTime.utcnow()
            ])
            self._db.commit()

    def pack(self, value):
        raise NotImplementedError

    def __getitem__(self, key):
        with self._db.cursor() as cur:
            cur.execute(f"SELECT key, data, last_modified FROM {self._table_name} WHERE key=%s", [key])
            res = cur.fetchone()
            if res is None:
                raise KeyError(key)
            return json.loads(res[1])

    def __delitem__(self, key):
        with self._db.cursor() as cur:
            cur.execute(f"DELETE FROM {self._table_name} WHERE key=%s", [key])
            self._db.commit()

    def __contains__(self, key):
        with self._db.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {self._table_name} WHERE key=%s", [key])
            res = cur.fetchone()
            return res is not None

    def items(self):
        with self._db.cursor() as cur:
            cur.execute(f"SELECT key, data FROM {self._table_name}")
            while rows := cur.fetchmany(25):
                for r in rows:
                    yield r[0], json.loads(r[1])
