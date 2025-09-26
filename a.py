import os
import io
import hashlib
import sqlite3
from elevenlabs.client import ElevenLabs
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.exceptions import RefreshError
from dotenv import load_dotenv


# ======= KONFIGURACJA =======
load_dotenv()
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.file',
]
FOLDER_ID = os.getenv("FOLDER_ID")  # ID folderu Google Drive
DOWNLOADS = os.getenv("DOWNLOADS_DIR")
DB_PATH = os.getenv("DB_PATH")

TRANSCRIPTS_SUBFOLDER_NAME = os.getenv("TRANSCRIPTS_SUBFOLDER_NAME", "transkrypty")
# Ścieżki plików autoryzacji obok tego pliku
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_PATH = os.path.join(BASE_DIR, 'token.json')
CREDENTIALS_PATH = os.path.join(BASE_DIR, 'credentials.json')
# ============================

# --- ElevenLabs client ---
eleven = ElevenLabs(api_key=os.getenv("ELEVEN_LABS_API_KEY"))

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

# --- Utils: timestamps & diarization formatting ---
def _get_value(obj, key, default=None):
    # Single key helper for dicts or attributes
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)

def _get_any(obj, keys, default=None):
    for key in keys:
        val = _get_value(obj, key, None)
        if val is not None:
            return val
    return default

def format_timestamp(total_seconds):
    total_seconds = int(total_seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"

def build_diarized_lines_from_words(words):
    lines = []
    current_minute_index = None
    current_speaker = None
    buffer_words = []

    def flush_buffer():
        if buffer_words:
            text_chunk = " ".join(buffer_words).strip()
            if text_chunk:
                prefix = f"{current_speaker}: " if current_speaker else ""
                lines.append(prefix + text_chunk)
            buffer_words.clear()

    for w in sorted(words, key=lambda w: _get_any(w, ["start"], 0)):
        start = _get_any(w, ["start"], 0)
        minute_index = int(start // 60)
        speaker = _get_any(w, ["speaker", "speaker_label", "speaker_id"], None)
        if speaker is None:
            sp_idx = _get_any(w, ["speaker_id"], None)
            if isinstance(sp_idx, int):
                speaker = f"Speaker {sp_idx}"
        if isinstance(speaker, int):
            speaker = f"Speaker {speaker}"

        if minute_index != current_minute_index:
            flush_buffer()
            lines.append(f"[{format_timestamp(minute_index * 60)}]")
            current_minute_index = minute_index
            current_speaker = None

        if speaker != current_speaker:
            flush_buffer()
            current_speaker = speaker

        word_text = _get_any(w, ["word", "text", "token"], "")
        if word_text:
            buffer_words.append(word_text)

    flush_buffer()
    return lines

def build_diarized_lines_from_segments(segments):
    lines = []
    current_minute_index = None
    for seg in sorted(segments, key=lambda s: _get_any(s, ["start"], 0)):
        start = _get_any(seg, ["start"], 0)
        minute_index = int(start // 60)
        speaker = _get_any(seg, ["speaker", "speaker_label", "speaker_id"], None)
        if isinstance(speaker, int):
            speaker = f"Speaker {speaker}"
        if minute_index != current_minute_index:
            lines.append(f"[{format_timestamp(minute_index * 60)}]")
            current_minute_index = minute_index
        content = _get_any(seg, ["text", "content"], "")
        if content:
            prefix = f"{speaker}: " if speaker else ""
            lines.append(prefix + content.strip())
    return lines

# --- Autoryzacja Google Drive ---
def get_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                creds = None
        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0, access_type='offline', prompt='consent', include_granted_scopes='true')
            with open(TOKEN_PATH, 'w') as token:
                token.write(creds.to_json())
    try:
        userinfo = build('oauth2', 'v2', credentials=creds).userinfo().get().execute()
    except Exception:
        pass
    return build('drive', 'v3', credentials=creds)

# --- Helper: ensure transcripts subfolder exists under FOLDER_ID ---
def ensure_transcripts_subfolder(service, parent_folder_id, subfolder_name):
    q = (
        f"'{parent_folder_id}' in parents and "
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='{subfolder_name}' and trashed=false"
    )
    result = service.files().list(
        q=q,
        fields="files(id, name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        pageSize=10
    ).execute()
    items = result.get('files', [])
    if items:
        return items[0]['id']
    metadata = {
        'name': subfolder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id]
    }
    created = service.files().create(
        body=metadata,
        fields='id, name',
        supportsAllDrives=True
    ).execute()
    print(f"✔ Utworzono podfolder: {created.get('name')} (id={created.get('id')})")
    return created['id']

# --- Pobieranie mp3/m4a/wav z Google Drive ---
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
    print(f"Transkrybuję {file_path}...")
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
    # Build diarized text with timestamps every minute
    if isinstance(transcription, dict):
        words = transcription.get("words") or []
        segments = transcription.get("segments") or []
        if words:
            lines = build_diarized_lines_from_words(words)
            text = "\n".join(lines)
        elif segments:
            lines = build_diarized_lines_from_segments(segments)
            text = "\n".join(lines)
        else:
            text = transcription.get("text") or ""
    else:
        # SDK object - try attributes
        words = getattr(transcription, "words", None) or []
        segments = getattr(transcription, "segments", None) or []
        if words:
            lines = build_diarized_lines_from_words(words)
            text = "\n".join(lines)
        elif segments:
            lines = build_diarized_lines_from_segments(segments)
            text = "\n".join(lines)
        else:
            text = getattr(transcription, "text", "")

    # Zapis do pliku DOCX
    base, _ = os.path.splitext(file_path)
    docx_path = base + ".docx"
    try:
        from docx import Document
        from docx.shared import Pt
        doc = Document()
        style = doc.styles["Normal"]
        style.font.name = "Calibri"
        style.font.size = Pt(11)
        for paragraph_text in text.split("\n"):
            if paragraph_text.strip():
                doc.add_paragraph(paragraph_text)
        doc.save(docx_path)
        print(f"✔ Zapisano transkrypt: {docx_path}")
        # Upload do podfolderu w folderze nagrań
        try:
            service = get_service()
            transcripts_folder_id = ensure_transcripts_subfolder(service, FOLDER_ID, TRANSCRIPTS_SUBFOLDER_NAME)
            metadata = {
                'name': os.path.basename(docx_path),
                'parents': [transcripts_folder_id]
            }
            media = MediaFileUpload(
                docx_path,
                mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                resumable=True
            )
            uploaded = service.files().create(
                body=metadata,
                media_body=media,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
            print(f"✔ Przesłano transkrypt do '{TRANSCRIPTS_SUBFOLDER_NAME}': {uploaded.get('name')} (id={uploaded.get('id')})")
            if uploaded.get('webViewLink'):
                print(f"  Link: {uploaded.get('webViewLink')}")
            # Po udanym uploadzie usuń lokalny plik audio
            try:
                os.remove(file_path)
                print(f"🗑️ Usunięto lokalny plik audio: {file_path}")
            except Exception as e2:
                print(f"⚠️ Nie udało się usunąć pliku {file_path}: {e2}")
        except Exception as e:
            print(f"❌ Błąd uploadu transkryptu do podfolderu: {e}")
    except ImportError:
        # Fallback do TXT jeśli python-docx nie jest zainstalowane
        txt_path = base + ".txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"✔ Zapisano transkrypt (TXT fallback): {txt_path}")

# --- Main ---
if __name__ == "__main__":
    files = download_new_mp3(FOLDER_ID)
    for f in files:
        if is_new_file(f):
            try:
                transcribe_with_elevenlabs(f)
            except Exception as e:
                print(f"❌ Błąd transkrypcji {f}: {e}")
        else:
            print(f"⏩ Pomijam {f} (już przetworzony)")
