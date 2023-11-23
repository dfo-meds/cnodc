import click
import secrets


@click.group
def main():
    pass


@main.command
@click.argument("username")
def create_user(username):
    from cnodc.api.auth import UserController
    uc = UserController()
    password = secrets.token_urlsafe(16)
    uc.create_user(username, password)
    print(f"User created, password: {password}")
