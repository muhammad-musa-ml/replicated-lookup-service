from .app import Config, create_app

app = create_app(Config.from_env())
