import typing as t

import flask
import flask_login
import markupsafe

from gcflask.i18n import BaseDString


class BaseNav:

    def __init__(self):
        self.children: dict[str, NavItem] = {}

    def __call__(self) -> markupsafe.Markup:
        return self.__html__()

    def __html__(self):
        print(self.markup())
        return markupsafe.Markup(self.markup())

    def append_at(self, position: list[str], item: NavItem):
        position_this_level = position.pop(0)
        if not position:
            if item.order is None:
                if not self.children:
                    item.order = 0
                else:
                    item.order = max([x.order or 0 for x in self.children.values()]) + 1
            self.children[position_this_level] = item
        else:
            self.children[position_this_level].append_at(position, item)

    def markup(self) -> str:
        raise NotImplementedError

    def sublist_markup(self) -> str:
        if not self.children:
            return ''
        s = '<ul class="menu">'
        for child in sorted(self.children.values(), key=lambda x: x.order):
            s += child.markup()
        s += '</ul>'
        return s


class NavItem(BaseNav):

    def __init__(self,
                 text: str | BaseDString,
                 link: str,
                 order: int | None = None,
                 require_permissions: t.Sequence[str] | None = None):
        super().__init__()
        self.text = text
        self.link = link
        self.order = order
        self.permissions = require_permissions

    def check_access(self) -> bool:
        if flask.has_request_context():
            if not self.permissions:
                return True
            return flask_login.current_user.require_permissions(self.permissions)
        return False

    def markup(self) -> str:
        s = ''
        if self.check_access():
            s = '<li>'
            s += f'<a href="{markupsafe.escape(flask.url_for(self.link))}">{markupsafe.escape(self.text)}</a>'
            s += self.sublist_markup()
            s += '</li>'
        return s


class NavMenu(BaseNav):

    def markup(self) -> str:
        return self.sublist_markup()