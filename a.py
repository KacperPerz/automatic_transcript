import os
import io
import hashlib
import sqlite3
from elevenlabs.client import ElevenLabs
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv



# ======= KONFIGURACJA =======
load_dotenv()
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
FOLDER_ID = os.getenv("FOLDER_ID")  # ID folderu Google Drive
DOWNLOADS = os.getenv("DOWNLOADS_DIR")
DB_PATH = os.getenv("DB_PATH")
print(FOLDER_ID, DOWNLOADS, DB_PATH)

# ============================

# --- ElevenLabs client ---
eleven = ElevenLabs(api_key=os.getenv("ELEVENLABS_API_KEY"))

# --- SQLite ---
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS processed (hash TEXT PRIMARY KEY)")
conn.commit()

def file_hash(path):
    """Hash MD5 pliku - rozpoznaje unikalność pliku"""
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

def is_new_file(path):
    """Sprawdza czy plik jest już w bazie, jeśli nie - dodaje"""
    h = file_hash(path)
    cursor.execute("SELECT 1 FROM processed WHERE hash=?", (h,))
    if cursor.fetchone():
        return False
    cursor.execute("INSERT INTO processed (hash) VALUES (?)", (h,))
    conn.commit()
    return True

# --- Autoryzacja Google Drive ---
def get_service():
    creds = None
    token_path = os.path.abspath('token.json')
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    print(f"Using token: {token_path}")
    try:
        userinfo = build('oauth2', 'v2', credentials=creds).userinfo().get().execute()
        print(f"Drive account: {userinfo.get('email')}")
    except Exception:
        pass
    return build('drive', 'v3', credentials=creds)

# --- Pobieranie mp3 z Google Drive ---
def download_new_mp3(folder_id, download_path=DOWNLOADS):
    service = get_service()
    os.makedirs(download_path, exist_ok=True)

    try:
        folder_meta = service.files().get(
            fileId=folder_id,
            fields="id, name, mimeType, parents").execute()
        print(f"Folder meta: {folder_meta}")
    except Exception as e:
        print(f"Cannot read folder meta ({folder_id}): {e}")

    # List all direct children first, then filter locally to catch odd MIME types
    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])
    print(f"Children: {[{'name': i['name'], 'mimeType': i.get('mimeType')} for i in items]}")

    new_files = []
    for file in items:
        name_lower = file['name'].lower()
        mime = file.get('mimeType') or ""
        allowed_exts = ('.mp3', '.m4a', '.wav')
        if not (mime.startswith('audio/') or name_lower.endswith(allowed_exts)):
            continue
        file_path = os.path.join(download_path, file['name'])
        if not os.path.exists(file_path):
            print(f"⬇️ Pobieram {file['name']}...")
            request = service.files().get_media(fileId=file['id'])
            with io.FileIO(file_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        print(f"   Pobrano {int(status.progress() * 100)}%")
            print(f"✔ Zapisano: {file_path}")
            new_files.append(file_path)
        else:
            new_files.append(file_path)  # istnieje, ale sprawdzimy hash
    return new_files

# --- Transkrypcja ElevenLabs ---
def transcribe_with_elevenlabs(file_path):
    print(f"🎤 Transkrybuję {file_path}...")
    from io import BytesIO
    with open(file_path, "rb") as f:
        audio_data = BytesIO(f.read())
    # wywołanie STT
    transcription = eleven.speech_to_text.convert(
        file=audio_data,
        model_id="scribe_v1",
        tag_audio_events=False,  # ustaw True jeśli chcesz [muzyka], [śmiech] itd.
        diarize=True             # diarization = rozdzielanie mówców
    )
    text = transcription["text"] if isinstance(transcription, dict) else transcription.text

    base, _ = os.path.splitext(file_path)
    txt_path = base + ".txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"✔ Zapisano transkrypt: {txt_path}")

# --- Main ---
if __name__ == "__main__":
    files = download_new_mp3(FOLDER_ID)
    exit()
    for f in files:
        if is_new_file(f):
            try:
                transcribe_with_elevenlabs(f)
            except Exception as e:
                print(f"❌ Błąd transkrypcji {f}: {e}")
        else:
            print(f"⏩ Pomijam {f} (już przetworzony)")
