import flask

from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import require_permission, web_error_handling

base = MultiLanguageBlueprint('base', __name__, url_prefix="/dmd")

@base.route('/')
@require_permission(anyone=True, check_referrer=False)
@web_error_handling
def splash():
    return flask.render_template('splash.html')


@base.route('/home')
@require_permission(anyone=True)
@web_error_handling
def home():
    return flask.render_template('home.html')
