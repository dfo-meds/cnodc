import flask
import functools

from auth import attempt_login, verify_user_token, change_password

nodb = flask.Blueprint("nodb", __name__)


def require_login(cb: callable):

    @functools.wraps(cb)
    def check_user(*args, **kwargs):
        if not flask.request.is_json:
            return {'error': 'unrecognized content type, must be JSON'}, 400
        if 'token' not in flask.request.json:
            return {'error': 'missing token'}, 400
        if 'SECRET_KEY' not in flask.current_app.config:
            return {'error': 'no secret key configured'}, 500
        if verify_user_token(flask.request.json['token']):
            return cb(*args, **kwargs)
        else:
            return {'error': 'token validation failed'}, 403

    return check_user


@nodb.route("/login", methods=["POST"])
def login():
    if not flask.request.is_json:
        return {'error': 'unrecognized content type, must be JSON'}, 400
    data = flask.request.json
    if not (data and 'username' in data and 'password' in data):
        return {'error': 'missing username and/or password'}, 400
    if 'SECRET_KEY' not in flask.current_app.config:
        return {'error': 'no secret key configured'}, 500
    return attempt_login(
        data['username'],
        data['password'],
        data['shared'] if 'shared' in data else None
    )


@nodb.route('/change-password', methods=["POST"])
@require_login
def change_password():
    if 'new_password' not in flask.request.json:
        return {'error': 'missing new password'}, 400
    return change_password(flask.request.json['new_password'])


