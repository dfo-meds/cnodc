import flask

from gcflask.i18n_url import MultiLanguageBlueprint


base = MultiLanguageBlueprint('base', __name__, url_prefix="/dmd")

@base.route('/')
def splash():
    return flask.render_template('splash.html')


@base.route('/home')
def home():
    return flask.render_template('home.html')
