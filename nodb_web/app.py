import flask
from cnodc.nodb import NODBDatabaseProtocol
from autoinject import injector

from cnodc.nodb.proto import NODBTransaction

app = flask.Flask(__name__)


@app.before_request
@injector.inject
def before_request(nodb: NODBDatabaseProtocol = None):
    if 'tx' not in flask.g:
        flask.g.tx = nodb.start_bare_transaction()


@app.teardown_request
def teardown_request(ex=None):
    if 'tx' in flask.g:
        tx: NODBTransaction = flask.g.tx
        try:
            if ex is None:
                tx.commit()
            else:
                tx.rollback()
            tx.close()
        except Exception as ex:
            flask.current_app.logger.exception("Exception during transaction closure")
        finally:
            del flask.g.tx
