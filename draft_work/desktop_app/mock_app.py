import datetime
import flask

app = flask.Flask(__name__)


@app.route("/test000/login", methods=['POST'])
def login_000():
    return {
        'token': '54321',
        'expiry': (datetime.datetime.utcnow() + datetime.timedelta(seconds=30)).isoformat(),
    }


@app.route("/test000/renew", methods=['POST'])
def renew_000():
    return {
        'token': '54321',
        'expiry': (datetime.datetime.utcnow() + datetime.timedelta(seconds=30)).isoformat(),
    }
