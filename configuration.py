import json
import os


class configuration:
    @staticmethod
    def get_configs():
        env = os.getenv("PYTHON_ENVIRONMENT").lower()
        with open(f'config.{env}.json') as config_file:
            data = json.load(config_file)

        return data