import os
import tempfile
from zipfile import ZipFile

_DEP_PACKAGE = os.path.join(tempfile.gettempdir(), "deployment_package.zip")


def _get_root_dir() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))


def _get_version():
    with open(os.path.join(_get_root_dir(), "app.yaml"), "r") as f:
        version = f.readlines()[-1].split(":")[1].strip()
    return version


def _get_sources_dir() -> str:
    return os.path.join(_get_root_dir(), "src")


def zip_sources() -> str:
    if os.path.exists(_DEP_PACKAGE):
        os.remove(_DEP_PACKAGE)
    with ZipFile(_DEP_PACKAGE, "w") as z_file:
        for f in os.listdir(_get_sources_dir()):
            if f not in ["lambda_function.py", "countdb_cli.py", "deploy_lambda.py", "pack_sources.py"]:
                z_file.write(os.path.join(_get_sources_dir(), f), f)
        with open(os.path.join(_get_sources_dir(), "lambda_function.py"), "r") as f:
            lambda_main_code = f.read()
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(str.encode(lambda_main_code.replace("$VERSION$", _get_version())))
            tf.flush()
            z_file.write(tf.name, "lambda_function.py")
    return _DEP_PACKAGE


if __name__ == "__main__":
    zip_file = zip_sources()
    print(zip_file)
