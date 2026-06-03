import flask
import re

from gcflask.i18n_url import MultiLanguageBlueprint

vocabularies = MultiLanguageBlueprint('vocabularies', __name__,)

@vocabularies.route("/vocabulary/<file_name>")
def deliver_vocabulary_file(file_name: str):
    if not re.match('^[a-z]+\\.ttl$', file_name):
        return flask.abort(404)
    from medsutil import ROOT_DIR
    return flask.send_from_directory(ROOT_DIR / 'vocab', file_name, as_attachment=False, mimetype="text/turtle")
