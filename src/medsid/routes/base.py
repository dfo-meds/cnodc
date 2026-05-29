from gcflask.i18n_url import MultiLanguageBlueprint


base = MultiLanguageBlueprint('base', __name__)

@base.route('/')
def splash():
    ...


@base.route('/home')
def home():
    ...
