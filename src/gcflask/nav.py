import typing as t

import flask
import flask_login
import markupsafe


class BaseNav:

    def __init__(self):
        self.children: dict[str, NavItem] = {}

    def __call__(self) -> markupsafe.Markup:
        return self.__html__()

    def __html__(self):
        raise NotImplementedError

    def append_at(self, position: list[str], item: NavItem):
        position_this_level = position.pop(0)
        if not position:
            self.children[position_this_level] = item
        else:
            self.children[position_this_level].append_at(position, item)

    def sublist_markup(self) -> str:
        if not self.children:
            return ''
        s = '<ul class="menu">'
        for child in sorted(self.children.values(), key=lambda x: x.order):
            s += child.__html__()
        s += '</ul>'
        return markupsafe.Markup(s)


class NavItem(BaseNav):

    def __init__(self, text: str, link: str, order: int | None = None, require_permissions: t.Sequence[str] | None = None):
        super().__init__()
        self.text = text
        self.link = link
        self.order = order
        self.permissions = require_permissions

    def check_access(self) -> bool:
        if flask.has_request_context():
            if not self.permissions:
                return True
            return flask_login.current_user.require_all(self.permissions)
        return False

    def __html__(self):
        s = ''
        if self.check_access():
            s = '<li>'
            s += '<a href="' + markupsafe.escape(flask.url_for(self.link)) + '">' + markupsafe.escape(self.text) + '</a>'
            s += self.sublist_markup()
            s += '</li>'
        return markupsafe.Markup(s)


class NavMenu(BaseNav):

    def __html__(self):
        return markupsafe.Markup(self.sublist_markup())