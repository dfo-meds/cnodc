import flask

from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import require_permission

base = MultiLanguageBlueprint('base', __name__, url_prefix="/dmd")

@base.route('/')
@require_permission(anyone=True, check_referrer=False)
def splash():
    return flask.render_template('splash.html')


@base.route('/home')
@require_permission(anyone=True)
def home():
    return flask.render_template('home.html')
