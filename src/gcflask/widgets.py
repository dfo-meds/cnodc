import typing as t

from autoinject import injector
from markupsafe import Markup, escape
from wtforms.widgets import html_params

from gcflask.csp import csp_nonce
from gcapp.i18n import TranslationManager, TString, tr, LanguageDetector


class BetterTableWidget:
    """
    Renders a list of fields as a set of table rows with th/td pairs.

    If `with_table_tag` is True, then an enclosing <table> is placed around the
    rows.

    Hidden fields will not be displayed with a row, instead the field will be
    pushed into a subsequent table row to ensure XHTML validity. Hidden fields
    at the end of the field list will appear outside the table.
    """

    def __init__(self, with_table_tag: bool = True):
        self.with_table_tag = with_table_tag

    def __call__(self, field, **kwargs):
        html = []
        if self.with_table_tag:
            kwargs.setdefault("id", field.id)
            html.append("<table %s>\n" % html_params(**kwargs))
        hidden = ""
        for subfield in field:
            content = str(subfield() if not subfield.render_kw else subfield(**subfield.render_kw))
            if subfield.type in ("HiddenField", "CSRFTokenField"):
                hidden += str(subfield)
            elif subfield.type in ("BooleanField"):
                html.append(
                    "<tr><td colspan='2'>%s%s%s</td></tr>\n" % (content, str(subfield.label), hidden)
                )
                hidden = ""
            else:
                html.append(
                    "<tr><th>%s</th><td>%s%s</td></tr>\n"
                    % (str(subfield.label), hidden, content)
                )
                hidden = ""
        if self.with_table_tag:
            html.append("</table>\n")
        if hidden:
            html.append(hidden + "\n")
        return Markup("".join(html))


class TabbedFieldFormWidget:

    def __init__(self, no_tab_fields: t.Optional[list] = None, for_txt_input: bool = False, default_tab: int = None):
        self._no_tab_fields = no_tab_fields or []
        self._for_txt_input = for_txt_input
        self._default_tab: t.Optional[int] = default_tab

    def __call__(self, field, **kwargs):
        labels = []
        texts = []
        others = []
        for subfield in field:
            name = subfield.name
            if subfield.type in ("HiddenField", "CSRFTokenField") or any(name.endswith(x) for x in self._no_tab_fields):
                others.append(subfield)
            else:
                labels.append(subfield.label)
                sf = str(subfield())
                if subfield.description:
                    sf += f'<div class="form-description">{subfield.description}</div>'
                texts.append(sf)
        cls = "" if not self._for_txt_input else "tabs-for-multilingual"
        html = f'<div id="{field.id}" class="{cls}">\n'
        html += '<ul>\n'
        for idx, label in enumerate(labels, start=1):
            label = str(label)
            if '<a' in label:
                s1 = label.find("<a")
                e1 = label.find(">", s1)
                s2 = label.find("</a>")
                label = label[0:s1] + label[e1+1:s2] + label[s2+4:]
            html += f'<li><a href="#{field.id}-tab-{idx}">{label}</a></li>\n'
        html += '</ul>\n'
        for idx, text in enumerate(texts, start=1):
            html += f'<div id="{field.id}-tab-{idx}">{text}</div>\n'
        html += '</div>\n'
        html += f'<script language="javascript" type="text/javascript" nonce="{csp_nonce("script-src")}">\n'
        html += "$(document).ready(function() {"
        tabs = ''
        if self._default_tab is not None:
            tabs = '{active: ' + str(self._default_tab) + '}'
        html += f"$('#{field.id}').tabs({tabs}).addClass('ui-tabs-vertical ui-helper-clearfix');"
        html += f"$('#{field.id} li').removeClass('ui-corner-top').addClass('ui-corner-left');"
        html += "});"
        html += '</script>\n'
        if others:
            html += f"<table id='{field.id}-other-fields' class='cb'>\n<tbody>\n"
            hidden = ""
            for subfield in others:
                content = str(subfield() if not subfield.render_kw else subfield(**subfield.render_kw))
                if subfield.type in ("HiddenField", "CSRFTokenField"):
                    hidden += str(subfield)
                elif subfield.type in ("BooleanField",):
                    html += "<tr><td colspan='2'>%s%s%s</td></tr>\n" % (content, str(subfield.label), hidden)
                else:
                    html += "<tr><th>%s</th><td>%s%s</td></tr>\n" % (str(subfield.label), hidden, content)
            html += '</tbody>\n</table>\n'
            if hidden:
                html += hidden + "\n"
        return Markup(html)


class HtmlList:

    def __init__(self, items):
        self.items = items

    def __str__(self):
        return str(self.__html__())

    def __html__(self):
        h = '<ul>'
        for item in self.items:
            h += f'<li>{escape(item)}</li>'
        h += '</ul>'
        return Markup(h)



class MultilingualList:

    def __init__(self, items: dict[str, str | Markup]):
        self.items = items

    def __html__(self):
        valid_keys = [k for k in self.items.keys() if k and k[0] != '_' and self.items[k]]
        if len(valid_keys) == 0:
            return ''
        elif len(valid_keys) == 1:
            return list(self.items.values())[0]
        else:
            html = '<dl>'
            for key in valid_keys:
                if key == 'und':
                    continue
                html += '<dt>' + tr(f"languages.full.{key}") + '</dt>'
                html += '<dd>' + escape(self.items[key]) + '</dd>'
            if 'und' in valid_keys:
                html += '<dt>' + tr(f"languages.full.und") + '</dt>'
                html += '<dd>' + escape(self.items['und']) + '</dd>'
            html += '</dl>'
            return Markup(html)



class FlatPickrWidget:

    def __init__(self, placeholder: str | TString | None = None, with_calendar: bool = True, with_time: bool = False):
        self.placeholder = placeholder
        self.with_calendar = bool(with_calendar)
        self.with_time = bool(with_time)

    @injector.inject
    def __call__(self, field, ld: LanguageDetector = None, tm: TranslationManager = None, **kwargs):
        markup = f'<input class="form-control-datetime-flatpickr" id="{field.id}" name="{field.name}" data-input '
        if self.placeholder:
            markup += f"placeholder='{escape(self.placeholder)}' "
        if field.data:
            markup += f"value='{str(field.data)}' "
        markup += '/>'
        markup += f' <button type="button" id="flatpickr-clear-button-{field.id}">{tr("pipeman.common.clear")}</button>'
        markup += f'<script language="javascript" type="text/javascript" nonce="{csp_nonce("script-src")}">'
        markup += '$(document).ready(function() {\n'
        markup += f"  let fp = $('#{field.id}').flatpickr(" + "{\n"
        if self.with_time:
            markup += "    'enableTime': true,\n"
            markup += "    'enableSeconds': true,\n"
            markup += "    'minuteIncrement': 1,\n"
        if not self.with_calendar:
            markup += "    'noCalendar': true,\n"
        markup += f"    'locale': '{ld.detect_language(tm.supported_languages())}'\n"
        markup += "  });\n"
        markup += f"  $('#flatpickr-clear-button-{field.id}').data('flatpickr', fp);"
        markup += f"  $('#flatpickr-clear-button-{field.id}').click(function() " + "{\n"
        markup += f"      $(this).data('flatpickr').clear();\n"
        markup += "});\n"
        markup += "});\n"
        markup += '</script>'
        return Markup(markup)


class Select2Widget:

    def __init__(self,
                 ajax_callback=None,
                 allow_multiple: bool = False,
                 query_delay=None,
                 placeholder=None,
                 min_input=None):
        self.ajax_callback = ajax_callback
        self.allow_multiple = allow_multiple
        self.query_delay = query_delay or 0
        self.placeholder = placeholder
        self.min_input = min_input

    def __call__(self, field, **kwargs):
        markup = f'<select class="form-control-select2" id="{field.id}" name="{field.name}"'
        if self.allow_multiple:
            markup += 'multiple="multiple"'
        markup += '>\n'
        for opts in field.iter_choices():
            markup += self.render_option(*opts) + "\n"
        markup += f'</select>\n<script language="javascript" type="text/javascript" nonce="{csp_nonce("script-src")}">'
        markup += '$(document).ready(function() {\n'
        markup += f"  $('#{field.id}').select2(" + "{\n"
        if self.ajax_callback:
            markup += "    ajax: {\n"
            markup += f"      url: '{self.ajax_callback}',\n"
            markup += "      dataType: 'json'\n"
            markup += "    },\n"
            if self.min_input:
                markup += f"    minimumInputLength: {int(self.min_input)},\n"
        if self.placeholder:
            markup += "    allowClear: true,\n"
            markup += f"    placeholder: '{self.placeholder}',\n"
        markup += f"    delay: {int(self.query_delay)}\n"
        markup += "  });\n"
        markup += "});\n"
        markup += '</script>\n'
        return Markup(markup)

    def render_option(self, val, label, selected, render_kw = None, **kwargs):
        if val is True or val is False:
            val = str(val)
        if val is None:
            val = ""
        sel_text = " selected=\"selected\"" if selected else ""
        return f'<option{sel_text} value="{val}">{escape(label)}</option>'
