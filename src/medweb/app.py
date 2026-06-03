def create_app():
    from medweb.boot import boot_medweb
    system = boot_medweb("web")

    import flask
    app = flask.Flask(__name__)

    system.init_app(app)
    return app

if __name__ == '__main__':
    app = create_app()
    app.run()
