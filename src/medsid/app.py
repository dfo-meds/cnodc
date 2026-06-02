from medweb.boot import boot_medsid


def create_app():
    import flask
    app = flask.Flask(__name__)
    system = boot_medsid()
    system.init_app(app)
    return app

app = create_app()

if __name__ == '__main__':
    app.run()