import os
import subprocess
from dotenv import load_dotenv
from prefect import task


@task()
def initialization():
    # git pull
    # print("Récupération du code (pull)...")
    # command = "git pull origin main"
    # subprocess.run(command.split(" "))

    print("Création du dossier dist/")
    if os.path.exists("dist"):
        print("dist exists")
    else:
        os.mkdir("dist")
