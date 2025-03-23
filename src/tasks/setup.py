import os
from prefect import task
from shutil import rmtree


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
