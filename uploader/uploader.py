import os
import io
import json
import mimetypes
import tempfile
import pickle
import base64
import re

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# =========================
# SCOPES
# =========================

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]
YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
]

# =========================
# CONFIG (ALL FROM ENV VARS)
# =========================

READY_ROOT_FOLDER_ID = os.getenv("READY_ROOT_FOLDER_ID")
UPLOADED_ROOT_FOLDER_ID = os.getenv("UPLOADED_ROOT_FOLDER_ID")

MAX_UPLOADS_PER_RUN = int(os.getenv("MAX_UPLOADS_PER_RUN", "1"))
PRIVACY_STATUS = os.getenv("PRIVACY_STATUS", "public")
TAGS = [
    t.strip()
    for t in os.getenv(
        "TAGS",
        "Shorts,memes,ciencia,curiosidades,descubrimientos,historia,reflexiones"
    ).split(",")
    if t.strip()
]

# ---- Drive (service account JSON as text) ----
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# ---- YouTube OAuth ----
YOUTUBE_CREDENTIALS_JSON = os.getenv("YOUTUBE_CREDENTIALS_JSON")  # credentials.json content
YOUTUBE_TOKEN_BASE64 = os.getenv("YOUTUBE_TOKEN_BASE64")          # token.pickle base64

# Internal temp paths
TMP_DIR = tempfile.gettempdir()
CLIENT_SECRETS_FILE = os.path.join(TMP_DIR, "yt_credentials.json")
TOKEN_PICKLE_PATH = os.path.join(TMP_DIR, "token.pickle")

# =========================
# SANITY CHECK
# =========================

def sanity_check():
    required = {
        "READY_ROOT_FOLDER_ID": READY_ROOT_FOLDER_ID,
        "UPLOADED_ROOT_FOLDER_ID": UPLOADED_ROOT_FOLDER_ID,
        "GOOGLE_SERVICE_ACCOUNT_JSON": GOOGLE_SERVICE_ACCOUNT_JSON,
        "YOUTUBE_CREDENTIALS_JSON": YOUTUBE_CREDENTIALS_JSON,
        "YOUTUBE_TOKEN_BASE64": YOUTUBE_TOKEN_BASE64,
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

# =========================
# AUTH HELPERS
# =========================

def prepare_youtube_files():
    # Write credentials.json
    with open(CLIENT_SECRETS_FILE, "w", encoding="utf-8") as f:
        f.write(YOUTUBE_CREDENTIALS_JSON)

    # Decode token.pickle
    token_bytes = base64.b64decode(YOUTUBE_TOKEN_BASE64)
    with open(TOKEN_PICKLE_PATH, "wb") as f:
        f.write(token_bytes)


def get_drive_service():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=DRIVE_SCOPES,
    )
    return build("drive", "v3", credentials=creds)


def get_youtube_service():
    creds = None
    if os.path.exists(TOKEN_PICKLE_PATH):
        with open(TOKEN_PICKLE_PATH, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Should NOT happen on Railway; token must exist
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRETS_FILE,
                YOUTUBE_SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(TOKEN_PICKLE_PATH, "wb") as f:
            pickle.dump(creds, f)

    return build("youtube", "v3", credentials=creds)


def print_channel_info(youtube):
    resp = youtube.channels().list(part="snippet", mine=True).execute()
    ch = resp["items"][0]["snippet"]
    print(f"✅ Connected to YouTube channel: {ch['title']}")

# =========================
# TITLE CLEANING
# =========================

def clean_video_title(raw_title: str) -> str:
    """
    Removes everything after '#shorts' (inclusive keeps '#shorts').
    Examples:
      'Mi video #shorts AB13KC' -> 'Mi video #shorts'
      'Mi video #Shorts xyz123' -> 'Mi video #Shorts'
      'Mi video normal' -> 'Mi video normal'
    """
    title = raw_title.strip()

    match = re.search(r'(?i)#shorts\b', title)
    if match:
        title = title[:match.end()].strip()

    # Clean extra spaces
    title = re.sub(r'\s+', ' ', title).strip()

    # Fallback in case title ends up empty
    return title or "Short #shorts"

# =========================
# DRIVE HELPERS
# =========================

def list_subfolders(drive, parent_id):
    q = (
        f"'{parent_id}' in parents and "
        f"mimeType='application/vnd.google-apps.folder' and "
        f"trashed=false"
    )
    r = drive.files().list(q=q, fields="files(id,name)", pageSize=200).execute()
    return r.get("files", [])


def list_files_in_folder(drive, folder_id, page_size=10):
    q = f"'{folder_id}' in parents and trashed=false"
    r = drive.files().list(
        q=q,
        fields="files(id,name,mimeType)",
        pageSize=page_size,
    ).execute()
    return r.get("files", [])


def is_folder_empty(drive, folder_id):
    files = list_files_in_folder(drive, folder_id, page_size=1)
    return len(files) == 0


def delete_empty_ready_subfolders(drive):
    """
    Deletes empty date subfolders inside READY_ROOT_FOLDER_ID.
    """
    folders = list_subfolders(drive, READY_ROOT_FOLDER_ID)
    deleted_count = 0

    for folder in folders:
        if is_folder_empty(drive, folder["id"]):
            print(f"🗑 Deleting empty folder: {folder['name']}")
            drive.files().delete(fileId=folder["id"]).execute()
            deleted_count += 1

    print(f"🧹 Empty folders deleted from READY: {deleted_count}")


def list_first_video(drive, folder_id):
    q = (
        f"'{folder_id}' in parents and "
        f"mimeType contains 'video/' and trashed=false"
    )
    r = drive.files().list(
        q=q,
        fields="files(id,name,createdTime)",
        orderBy="createdTime",
        pageSize=1,
    ).execute()
    files = r.get("files", [])
    return files[0] if files else None


def get_next_date_folder_with_videos(drive):
    folders = list_subfolders(drive, READY_ROOT_FOLDER_ID)
    folders.sort(key=lambda f: f["name"])  # YYYY-MM-DD

    for f in folders:
        if list_first_video(drive, f["id"]):
            return f["name"], f["id"]

    return None, None


def ensure_uploaded_date_folder(drive, date_name):
    q = (
        f"'{UPLOADED_ROOT_FOLDER_ID}' in parents and "
        f"name='{date_name}' and "
        f"mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    r = drive.files().list(q=q, fields="files(id,name)").execute()
    files = r.get("files", [])

    if files:
        return files[0]["id"]

    meta = {
        "name": date_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [UPLOADED_ROOT_FOLDER_ID],
    }
    folder = drive.files().create(body=meta, fields="id").execute()
    return folder["id"]


def download_to_tmp(drive, file_id, filename):
    req = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    dl = MediaIoBaseDownload(fh, req)

    done = False
    while not done:
        status, done = dl.next_chunk()
        if status:
            print(f"   Download {int(status.progress() * 100)}%")

    path = os.path.join(TMP_DIR, filename)
    with open(path, "wb") as f:
        f.write(fh.getvalue())

    return path


def move_file(drive, file_id, from_id, to_id):
    drive.files().update(
        fileId=file_id,
        addParents=to_id,
        removeParents=from_id,
    ).execute()

# =========================
# YOUTUBE UPLOAD
# =========================

def upload_to_youtube(youtube, file_path, title):
    mimetype = mimetypes.guess_type(file_path)[0] or "video/mp4"

    cleaned_title = clean_video_title(title)
    print(f"   📝 Original title: {title}")
    print(f"   🧼 Cleaned title:  {cleaned_title}")

    body = {
        "snippet": {
            "title": cleaned_title[:100],
            "tags": TAGS,
            "categoryId": "22",
        },
        "status": {
            "privacyStatus": PRIVACY_STATUS,
            "selfDeclaredMadeForKids": False,
        },
    }

    media = MediaFileUpload(file_path, mimetype=mimetype, resumable=True)
    req = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    print(f"   🚀 Uploading {os.path.basename(file_path)}")
    resp = None
    while resp is None:
        status, resp = req.next_chunk()
        if status:
            print(f"   Uploaded {int(status.progress() * 100)}%")

    print(f"   ✅ https://youtube.com/watch?v={resp['id']}")
    return resp["id"]

# =========================
# MAIN
# =========================

def main():
    sanity_check()
    prepare_youtube_files()

    print("Authenticating Drive...")
    drive = get_drive_service()

    print("🧹 Cleaning empty folders in SHORTS_READY...")
    delete_empty_ready_subfolders(drive)

    print("Authenticating YouTube...")
    youtube = get_youtube_service()
    print_channel_info(youtube)

    uploaded = 0

    while uploaded < MAX_UPLOADS_PER_RUN:
        date_name, ready_date_id = get_next_date_folder_with_videos(drive)
        if not ready_date_id:
            print("⚠️ No videos pending.")
            return

        print(f"📅 Using date folder: {date_name}")
        video = list_first_video(drive, ready_date_id)

        uploaded_date_id = ensure_uploaded_date_folder(drive, date_name)

        print("⬇️ Downloading...")
        local_path = download_to_tmp(drive, video["id"], video["name"])

        print("⬆️ Uploading to YouTube...")
        original_title = os.path.splitext(video["name"])[0]
        upload_to_youtube(youtube, local_path, original_title)

        print("🗂 Moving file in Drive...")
        move_file(drive, video["id"], ready_date_id, uploaded_date_id)

        # If the ready folder becomes empty after moving the file, delete it too
        try:
            if is_folder_empty(drive, ready_date_id):
                print(f"🗑 Deleting now-empty date folder: {date_name}")
                drive.files().delete(fileId=ready_date_id).execute()
        except Exception as e:
            print(f"⚠️ Could not delete folder {date_name}: {e}")

        try:
            os.remove(local_path)
        except Exception:
            pass

        uploaded += 1

    print(f"🎉 Done. Uploaded {uploaded} video(s).")


if __name__ == "__main__":
    main()