import os
from prefect import task
from shutil import rmtree
from prefect.artifacts import create_table_artifact


@task()
def initialization():
    # git pull
    # print("Récupération du code (pull)...")
    # command = "git pull origin main"
    # subprocess.run(command.split(" "))

    print("Suppression + création du dossier /dist")
    if os.path.exists("dist"):
        rmtree("dist", ignore_errors=False)
    os.mkdir("dist")


def create_artifact(
    data,
    key: str,
    description: str = None,
):
    if data is list:
        create_table_artifact(key=key, table=data, description=description)
