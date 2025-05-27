from datetime import datetime
from dotenv import load_dotenv
import json
import os
import shutil

if not os.path.exists(".env"):
    print("Création du fichier .env à partir de template.env")
    shutil.copyfile("template.env", ".env")

if not os.path.exists("./dist"):
    os.mkdir("dist")

load_dotenv()

DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD

DIST_DIR = f"dist/" + DATE_NOW
DECP_PROCESSING_PUBLISH = os.environ.get("DECP_PROCESSING_PUBLISH", "")

with open(os.environ.get("DECP_JSON_FILES_PATH", "data/decp_json_files.json")) as f:
    DECP_JSON_FILES = json.load(f)
