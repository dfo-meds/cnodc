from gcflask.i18n_url import MultiLanguageBlueprint

user = MultiLanguageBlueprint('base', __name__)


@user.route('/me')
def me():
    ...

@user.route('/me/edit', methods=['GET', 'POST'])
def edit():
    ...

@user.route('/me/change-password', methods=['GET', 'POST'])
def change_password():
    ...

@user.route('/me/permissions')
def permissions():
    ...
