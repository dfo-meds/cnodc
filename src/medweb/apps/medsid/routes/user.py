from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import require_permission
user = MultiLanguageBlueprint('user', __name__, url_prefix="/medsid")


@user.route('/me')
@require_permission(authenticated_only=True)
def me():
    ...

@user.route('/me/edit', methods=['GET', 'POST'])
@require_permission(authenticated_only=True)
def edit():
    ...

@user.route('/me/change-password', methods=['GET', 'POST'])
@require_permission(authenticated_only=True)
def change_password():
    ...

@user.route('/me/permissions')
@require_permission(authenticated_only=True)
def permissions():
    ...

@user.route('/users')
@require_permission("medsid.user_management.view")
def list_users():
    ...

@user.route('/users/<username>')
@require_permission("medsid.user_management.view")
def view_user(username):
    ...

@user.route('/users/<username>/edit', methods=['GET', 'POST'])
@require_permission("medsid.user_management.edit")
def edit_user(username):
    ...

@user.route('/api/users')
@require_permission("medsid.user_management.view", is_api=True)
def api_list_users():
    ...
