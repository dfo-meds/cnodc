from markupsafe import Markup, escape

from gcapp import i18n


class ActionList:

    def __init__(self):
        self._items: list[tuple[str, str, int]] = []

    def __bool__(self):
        return bool(self._items)

    def add_action(self, tr_key: str, url: str, order: int = -1):
        if order < 0:
            if self._items:
                order = max(x[2] for x in self._items) + 1
            else:
                order = 0
        self._items.append((
            url,
            tr_key,
            order
        ))

    def render(self, html_cls: str = "action_list") -> Markup:
        self._items.sort(key=lambda x: x[2])
        markup = f'<ul class="{html_cls}">'
        for path, txt, _ in self._items:
            markup += f"<li><a href=\"{escape(path)}\">{escape(i18n.tr(txt))}</a></li>"
        markup += '</ul>'
        return Markup(markup)

    def __html__(self) -> Markup:
        return self.render()
