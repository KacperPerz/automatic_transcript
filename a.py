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
    try:
        userinfo = build('oauth2', 'v2', credentials=creds).userinfo().get().execute()
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
    print(f"Transkrybuję {file_path}...")
    from io import BytesIO
    with open(file_path, "rb") as f:
        audio_data = BytesIO(f.read())
    # wywołanie STT
    transcription = eleven.speech_to_text.convert(
        file=audio_data,
        model_id="scribe_v1",
        tag_audio_events=True,  # ustaw True jeśli chcesz [muzyka], [śmiech] itd.
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
