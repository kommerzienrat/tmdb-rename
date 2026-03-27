#!/usr/bin/env python3
"""
Universal Media Renamer for private movie and series collections based on TMDb.

Python 3.10+

Set environment variable:
    export TMDB_ACCESS_TOKEN="eyJ..."

Or when running:
    TMDB_ACCESS_TOKEN="eyJ..." python3 rename.py /path
"""

import errno
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Any, cast

# ========== CONFIGURATION ==========

MAX_PATH_LENGTH = 250
MIN_VIDEO_SIZE_MB = 100
MAIN_MOVIE_SIZE_RATIO = 1.1  # Minimum size ratio to consider a file as main movie (10% larger)

# ===================================


def get_tmdb_token() -> str | None:
    """Reads the TMDb access token from environment variables."""
    # Primary: TMDB_ACCESS_TOKEN
    token = os.environ.get("TMDB_ACCESS_TOKEN")
    if token:
        return token.strip()
    
    # Fallback: TMDB_API_TOKEN (alternative name)
    token = os.environ.get("TMDB_API_TOKEN")
    if token:
        return token.strip()
    
    # Fallback: From file ~/.tmdb_token
    token_file = Path.home() / ".tmdb_token"
    if token_file.exists():
        try:
            return token_file.read_text().strip()
        except OSError:
            pass
    
    return None


def check_token(token: str | None) -> bool:
    """Checks whether a valid token is available."""
    if not token:
        return False
    
    # Check JWT format (eyJ... with two dots)
    if token.startswith("eyJ") and token.count(".") == 2:
        return True
    
    # Also accept API Key v3 format (32 hex digits)
    if re.match(r'^[a-f0-9]{32}$', token, re.I):
        return True
    
    return False


class MediaType(Enum):
    MOVIE = auto()
    SERIES = auto()
    COLLECTION = auto()
    UNKNOWN = auto()


class MatchStatus(Enum):
    AUTO = "✓"
    UNSURE = "?"
    MANUAL = "✎"
    NONE = "✗"
    SKIP = "⊘"
    DONE = "✔"
    RENAMED = "★"


@dataclass
class EpisodeInfo:
    season: int
    episode: int


@dataclass
class VideoFile:
    path: Path
    size_bytes: int
    media_type: MediaType = MediaType.UNKNOWN
    episode_info: EpisodeInfo | None = None
    extracted_title: str | None = None
    extracted_year: str | None = None
    parent_folder: Path | None = None

    @property
    def size_gb(self) -> float:
        return self.size_bytes / (1024 ** 3)

    @property
    def best_name_source(self) -> str:
        if self.parent_folder:
            return self.parent_folder.name
        return self.path.stem


@dataclass
class MediaMatch:
    tmdb_id: int
    imdb_id: str | None
    title: str
    original_title: str
    year: str
    media_type: str
    popularity: float = 0.0


@dataclass
class CollectionItem:
    folder_path: Path | None
    video_path: Path
    video_size: float
    extracted_title: str | None
    extracted_year: str | None
    matches: list[MediaMatch]
    selected_match: MediaMatch | None = None
    status: MatchStatus = MatchStatus.NONE


@dataclass
class ScanResult:
    path: Path
    folder_name: str
    detected_type: MediaType
    extracted_title: str | None
    extracted_year: str | None
    videos: list[VideoFile]
    matches: list[MediaMatch]
    selected_match: MediaMatch | None = None
    status: MatchStatus = MatchStatus.NONE
    new_name: str | None = None
    error: str | None = None
    collection_items: list[CollectionItem] | None = None
    season_number: int | None = None  # Added for series season detection
    series_name: str | None = None  # Added for series grouping

    @property
    def is_collection(self) -> bool:
        return self.detected_type == MediaType.COLLECTION

    @property
    def video_count(self) -> int:
        return len(self.videos)


@dataclass
class RenameOp:
    old: str
    new: str
    done: bool = False


class RenameError(Exception):
    pass


class MediaRenamer:
    RELEASE_PATTERNS = [
        r'\bTrueHD\b',
        r'\bDTS-HD[\s\.]?MA\b',
        r'\bDTS-HD\b',
        r'\bDTS\b',
        r'\bAtmos\b',
        r'\bDD[57][\.\s]?1\b',
        r'\b5[\.\s]?1\b',
        r'\bDDP?5[\.\s]?1\b',
        r'\bAC3\b',
        r'\bAC3D\b',
        r'\bEAC3\b',
        r'\bEAC3D\b',
        r'\bAAC\b',
        r'\bAAC5[\s\.]?1D?\b',
        r'\bFLAC\b',
        r'\bGerman\b',
        r'\bGER\b',
        r'\bEnglish\b',
        r'\bENG\b',
        r'\bFrench\b',
        r'\bSpanish\b',
        r'\bItalian\b',
        r'\bRussian\b',
        r'\bDL\b',
        r'\bDUBBED\b',
        r'\bDUAL\b',
        r'\bMULTi\b',
        r'\bML\b',
        r'\b1080p\b',
        r'\b2160p\b',
        r'\b720p\b',
        r'\b480p\b',
        r'\b4K\b',
        r'\bUHD\b',
        r'\bBluRay\b',
        r'\bBlu-Ray\b',
        r'\bBDRip\b',
        r'\bBRRip\b',
        r'\bWEB-DL\b',
        r'\bWEBDL\b',
        r'\bWEBRip\b',
        r'\bWEB\b',
        r'\bWebHD\b',
        r'\bAMZN\b',
        r'\bNF\b',
        r'\bHULU\b',
        r'\bDSNY\b',
        r'\bDVDRip\b',
        r'\bDVD\b',
        r'\bHDTV\b',
        r'\bPDTV\b',
        r'\bx264\b',
        r'\bx265\b',
        r'\bx26\d?\b',
        r'\bH[\.\s]?264\b',
        r'\bH[\.\s]?265\b',
        r'\bHEVC\b',
        r'\bAVC\b',
        r'\bVC-?1\b',
        r'\bXviD\b',
        r'\bDivX\b',
        r'\bRemux\b',
        r'\bREMUX\b',
        r'\bEXTENDED\b',
        r'\bUNRATED\b',
        r'\bREMASTERED\b',
        r'\bDirectors[\s\.]?Cut\b',
        r'\bIMAX\b',
        r'\bTHEATRICAL\b',
        r'\bUNCUT\b',
        r'\bHDR10Plus\b',
        r'\bHDR10\b',
        r'\bHDR\b',
        r'\bDoVi\b',
        r'\bDolby[\s\.]?Vision\b',
        r'\bHLG\b',
        r'\b10bit\b',
        r'\b8bit\b',
        r'\bCOMPLETE\b',
        r'\bPROPER\b',
        r'\bREAL\b',
        r'\bREPACK\b',
        r'\bINTERNAL\b',
        r'\bLIMITED\b',
        r'-[A-Za-z0-9]+$',
        # German title parts commonly found in releases
        r'\bDie\s+Legende\s+der\s+Adlerkrieger\b',
        r'\bDie\s+Legende\b',
        r'\bDer\s+Legend\b',
        r'\bDas\s+Abenteuer\b',
        r'\bder\s+Adlerkrieger\b',
    ]

    END_ONLY_PATTERNS = [
        r'\bDC\b',
        r'\bHD\b',
        r'\bSD\b',
    ]

    COLLECTION_PATTERNS = [
        r'[Cc]ollection',
        r'[Ss]ammlung',
        r'[Aa]nthology',
        r'[Ss]aga',
        r'[Bb]ox\.?[Ss]et',
        r'\d{4}\s*[-–]\s*\d{4}',
    ]

    IGNORE_DIRS = frozenset({
        'sample', 'samples', 'proof', 'extra', 'extras', 'behind the scenes',
        'deleted scenes', 'featurettes', 'interviews', 'trailers', 'subs',
        'subtitles', 'sub', 'cover', 'covers', 'bonus', 'specials'
    })

    # Audio file extensions commonly used for audiobooks
    AUDIOBOOK_EXTS: frozenset[str] = frozenset({'.mp3', '.m4b', '.aac', '.flac', '.wav', '.ogg'})
    # Keywords commonly found in audiobook folder/filenames
    AUDIOBOOK_KEYWORDS: list[str] = ['h[oö]rbuch', 'audiobook', 'hörspiel', 'hörbuch', 'audio book', 'hör-buch']

    VIDEO_EXT = frozenset({'.mkv', '.mp4', '.avi', '.m4v', '.wmv', '.mov', '.ts', '.m2ts'})
    SUB_EXT = frozenset({'.srt', '.sub', '.ass', '.ssa', '.vtt', '.idx', '.sup'})
    RENAME_EXT = frozenset({'.mkv', '.mp4', '.avi', '.m4v', '.nfo'})
    SCENE_TRASH_EXT = frozenset({'.sfv', '.par2', '.md5', '.sha1', '.sha256', '.crc', '.diz'})

    LANG_MAP = {
        'ger': 'de', 'german': 'de', 'deu': 'de', 'deutsch': 'de',
        'eng': 'en', 'english': 'en', 'fre': 'fr', 'french': 'fr',
        'spa': 'es', 'spanish': 'es', 'ita': 'it', 'italian': 'it',
    }

    UMLAUTS = {'ae': 'ä', 'oe': 'ö', 'ue': 'ü', 'Ae': 'Ä', 'Oe': 'Ö', 'Ue': 'Ü'}

    EP_PATTERNS = [
        r'[Ss](\d{1,2})[Ee](\d{1,3})',
        r'[Ss](\d{1,2})[\.\-\s]?[Ee](\d{1,3})',
        r'(\d{1,2})[xX](\d{1,3})',
    ]

    EP_ONLY_PATTERNS = [
        r'\b[Ee][Pp]?[\.\-\s]?(\d{1,3})\b',
        r'\b(?:Episode|Folge)[\.\-\s]?(\d{1,3})\b',
    ]
    
    # Enhanced season patterns for better series detection
    SEASON_PATTERNS = [
        r'[Ss]eason\s*(\d{1,2})',
        r'[Ss]taffel\s*(\d{1,2})',
        r'(?:Staffel|Season)\s*(\d{1,2})',
        r'S\s?(\d{1,2})\s*$',
        r'^\s*(\d{1,2})\s*x\s*\d{1,3}',  # Pattern like "1x01" at start of name
    ]

    TMDB_BASE = "https://api.themoviedb.org/3"

    def __init__(
        self,
        access_token: str,
        interactive: bool = True,
        debug_series: bool = False,
        series_batch_mode: bool = True,
    ):
        self.access_token = access_token
        self.interactive = interactive
        self.debug_series = debug_series
        self.series_batch_mode = series_batch_mode
        self._ops: list[RenameOp] = []
        self._cache: dict[str, Any] = {}
        
        # Manual mappings storage (folder_name -> imdb_id)
        self._manual_mappings: dict[str, str] = {}
        self._manual_mappings_file = Path.home() / ".tmdb_manual_mappings.json"
        self._load_manual_mappings()

    def _load_manual_mappings(self) -> None:
        """Load manually saved IMDB mappings from JSON file."""
        if self._manual_mappings_file.exists():
            try:
                with open(self._manual_mappings_file, 'r', encoding='utf-8') as f:
                    self._manual_mappings = json.load(f)
                print(f"  ✓ Loaded {len(self._manual_mappings)} manual mappings")
            except (json.JSONDecodeError, IOError) as e:
                print(f"  ⚠ Failed to load manual mappings: {e}")
                self._manual_mappings = {}

    def _save_manual_mappings(self) -> None:
        """Save manually entered IMDB mappings to JSON file."""
        try:
            with open(self._manual_mappings_file, 'w', encoding='utf-8') as f:
                json.dump(self._manual_mappings, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"  ⚠ Failed to save manual mappings: {e}")

    def verify_api_connection(self) -> bool:
        """Tests the API connection."""
        data = self._tmdb_request("/configuration")
        return data is not None and "images" in data

    # ==================== FILESYSTEM ====================

    def _same_fs(self, a: Path, b: Path) -> bool:
        try:
            dev_a = (a if a.exists() else a.parent).stat().st_dev
            dev_b = (b if b.exists() else b.parent).stat().st_dev
            return dev_a == dev_b
        except OSError:
            return False

    def _rename(self, src: Path, dst: Path) -> None:
        if not src.exists():
            raise RenameError(f"Source missing: {src.name}")
        if dst.exists() and src.resolve() != dst.resolve():
            raise RenameError(f"Destination exists: {dst.name}")
        if not self._same_fs(src, dst):
            raise RenameError("Cross-filesystem moves are not allowed")
        try:
            os.rename(src, dst)
        except OSError as e:
            errno_value = e.errno if e.errno is not None else 0
            msg = {errno.EXDEV: "Cross-device", errno.EACCES: "Access denied",
                   errno.EPERM: "Permission denied"}.get(errno_value, f"OS error {e.errno}")
            raise RenameError(f"{msg}: {src.name}")

    def _do_rename(self, old: Path, new: Path) -> None:
        op = RenameOp(old=str(old), new=str(new))
        self._ops.append(op)
        self._rename(old, new)
        op.done = True

    def _rollback(self) -> None:
        done = [o for o in self._ops if o.done]
        if not done:
            self._ops.clear()
            return
        print("\n  🔄 Rollback...")
        for op in reversed(done):
            src, dst = Path(op.new), Path(op.old)
            if src.exists():
                try:
                    os.rename(src, dst)
                    print(f"     ↩ {src.name}")
                except OSError as e:
                    print(f"     ❌ {src.name}: {e}")
        self._ops.clear()

    def _commit(self) -> None:
        self._ops.clear()

    # ==================== TITLE EXTRACTION ====================

    def _extract_title_year(self, name: str) -> tuple[str | None, str | None]:
        # Skip audiobook folders entirely
        for keyword in self.AUDIOBOOK_KEYWORDS:
            if re.search(keyword, name, re.IGNORECASE):
                return None, None

        clean = name

        for ext in self.VIDEO_EXT:
            if clean.lower().endswith(ext):
                clean = clean[:-len(ext)]
                break

        clean = clean.replace('.', ' ').replace('_', ' ')

        for pat in [r'[Ss]\d{1,2}[Ee]\d{1,3}', r'\b[Ss]\d{1,2}\b', r'\d{1,2}[xX]\d{1,3}',
                r'[Ss]eason\s?\d{1,2}', r'[Ss]taffel\s?\d{1,2}',
                r'\b[Ee][Pp]?[\.\-\s]?\d{1,3}\b', r'\b(?:Episode|Folge)[\.\-\s]?\d{1,3}\b']:
            clean = re.sub(pat, ' ', clean, flags=re.I)

        for pat in self.COLLECTION_PATTERNS:
            clean = re.sub(pat, ' ', clean, flags=re.I)

        year_m = re.search(r'\b(19\d{2}|20\d{2})\b', clean)
        year = year_m[1] if year_m else None

        if year:
            year_pos = clean.find(year)
            if year_pos > 10:
                clean = clean[:year_pos + 4]

        for pat in self.RELEASE_PATTERNS:
            clean = re.sub(pat, ' ', clean, flags=re.I)

        for pat in self.END_ONLY_PATTERNS:
            clean = re.sub(pat + r'\s*$', ' ', clean, flags=re.I)

        clean = re.sub(r'^[a-z0-9]{2,8}[-_]\s*', '', clean, flags=re.I)

        title = re.sub(r'\b(19\d{2}|20\d{2})\b', '', clean)

        title = re.sub(r'\s+', ' ', title).strip()
        title = re.sub(r"[^\w\s&'\-äöüÄÖÜß]", '', title).strip()
        title = re.sub(r'\s+', ' ', title).strip()
        title = re.sub(r'^[\s\-]+|[\s\-]+$', '', title)

        if not title or len(title) < 2:
            return None, year

        # Convert ASCII umlauts to actual umlauts for better search results
        title = self._to_umlauts(title)

        return title, year

    def _is_cryptic_filename(self, filename: str) -> bool:
        stem = Path(filename).stem

        if re.match(r'^[a-z0-9]{2,8}[-_][a-z0-9]+[-_][a-z0-9]+$', stem, re.I):
            return True

        words = re.findall(r'[a-zA-Z]{3,}', stem)
        if len(words) < 2:
            return True

        return False

    def _to_umlauts(self, text: str) -> str:
        """Convert ASCII representations of German umlauts to actual umlauts."""
        result = text
        for ascii_v, umlaut in self.UMLAUTS.items():
            if ascii_v == 'ue':
                result = re.sub(r'(?<=[bcdfghjklmnpqrstvwxzBCDFGHJKLMNPQRSTVWXZ])ue', umlaut, result)
            else:
                result = result.replace(ascii_v, umlaut)
        return result

    def _from_umlauts(self, text: str) -> str:
        """Convert German umlauts to ASCII representations for search."""
        result = text
        for ascii_v, umlaut in self.UMLAUTS.items():
            result = result.replace(umlaut, ascii_v)
        return result

    def _is_audiobook(self, path: Path) -> bool:
        """Check if a file or directory is related to audiobooks.
        
        This method prevents audiobooks from being processed as video media by:
        1. Checking file extensions against known audiobook formats
        2. Searching for audiobook keywords in filenames and folder names
        
        Args:
            path: Path to check
            
        Returns:
            bool: True if the path is identified as audiobook content
        """
        # Check file extension for audio files
        if path.is_file() and path.suffix.lower() in self.AUDIOBOOK_EXTS:
            return True
        
        # Check for audiobook keywords in filename or parent folder names
        names_to_check = [path.name.lower()]
        if path.parent:
            names_to_check.append(path.parent.name.lower())
            # Check grandparent as well for deeper nesting
            if path.parent.parent:
                names_to_check.append(path.parent.parent.name.lower())
        
        for name in names_to_check:
            for keyword in self.AUDIOBOOK_KEYWORDS:
                # Prüfe auf genaue Übereinstimmung oder Übereinstimmung als separates Wort
                if re.search(r'\b' + keyword + r'\b', name, re.IGNORECASE):
                    return True
                
                # Zusätzliche Prüfung für Teile des Namens
                if re.search(keyword, name, re.IGNORECASE):
                    return True
        
        return False

    # ==================== DETECTION ====================

    def _find_videos(self, directory: Path, max_depth: int = 5) -> list[VideoFile]:
        videos: list[VideoFile] = []

        def scan(d: Path, depth: int = 0, parent_is_root: bool = True):
            if depth > max_depth or d.name.lower() in self.IGNORE_DIRS:
                return
            # Skip audiobook directories entirely
            if self._is_audiobook(d):
                return
            try:
                for item in d.iterdir():
                    # Skip audiobook files
                    if self._is_audiobook(item):
                        continue
                    if item.is_file() and item.suffix.lower() in self.VIDEO_EXT:
                        try:
                            size = item.stat().st_size
                            if size >= MIN_VIDEO_SIZE_MB * 1024 * 1024:
                                vf = VideoFile(path=item, size_bytes=size)
                                vf.episode_info = self._parse_episode(item.name)
                                vf.media_type = MediaType.SERIES if vf.episode_info else MediaType.MOVIE

                                if not parent_is_root and item.parent != directory:
                                    vf.parent_folder = item.parent

                                best_name = vf.parent_folder.name if vf.parent_folder else item.stem

                                if self._is_cryptic_filename(item.name) and vf.parent_folder:
                                    best_name = vf.parent_folder.name

                                title, year = self._extract_title_year(best_name)
                                vf.extracted_title = title
                                vf.extracted_year = year
                                videos.append(vf)
                        except OSError:
                            pass
                    elif item.is_dir():
                        scan(item, depth + 1, parent_is_root=False)
            except PermissionError:
                pass

        scan(directory)
        return sorted(videos, key=lambda v: v.size_bytes, reverse=True)

    def _parse_episode(self, name: str) -> EpisodeInfo | None:
        """Parse episode information from filename with enhanced series context support.
        
        Extended version that handles various naming conventions including:
        - Standard SXXEXX patterns
        - Season folders with episode numbers
        - Episode/Ep naming conventions
        
        Args:
            name: Filename or folder name to parse
            
        Returns:
            EpisodeInfo | None: Parsed episode information or None if not found
        """
        # First try standard episode patterns
        for pat in self.EP_PATTERNS:
            if m := re.search(pat, name, re.IGNORECASE):
                try:
                    return EpisodeInfo(season=int(m[1]), episode=int(m[2]))
                except (ValueError, IndexError):
                    pass
        
        # Then try to extract season from the name for better series context
        season = self._extract_season_info(name)
        if season:
            # Look for episode number in patterns like "Episode 01" or "Ep 01"
            ep_match = re.search(r'[Ee]p(?:isode)?\s*[#:]?\s*(\d{1,3})', name, re.IGNORECASE)
            if ep_match:
                try:
                    return EpisodeInfo(season=season, episode=int(ep_match.group(1)))
                except (ValueError, IndexError):
                    pass
            
            # If we have a season but no specific episode, default to episode 1
            # This helps with cases where each episode is in its own folder
            return EpisodeInfo(season=season, episode=1)

        # Handle standalone episode patterns like E01, EP01, Folge 01, Episode 01
        for pat in self.EP_ONLY_PATTERNS:
            ep_match = re.search(pat, name, re.IGNORECASE)
            if ep_match:
                try:
                    return EpisodeInfo(season=1, episode=int(ep_match.group(1)))
                except (ValueError, IndexError):
                    pass
        
        return None

    def _extract_season_info(self, name: str) -> int | None:
        """Extract season number from folder/filename.
        
        Used to improve series detection for folders with season information
        but without standard episode patterns.
        
        Args:
            name: String to extract season info from
            
        Returns:
            int | None: Season number if found, None otherwise
        """
        for pattern in self.SEASON_PATTERNS:
            match = re.search(pattern, name, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except (ValueError, IndexError):
                    pass
        return None

    def _get_series_context(self, directory: Path) -> str | None:
        """Extract series name from parent folder names for better context.
        
        Helps identify series content when individual episodes are in separate folders
        by looking at parent directory names for series indicators.
        
        Args:
            directory: Path to the directory being analyzed
            
        Returns:
            str | None: Series name if found, None otherwise
        """
        # Check parent directory for series name
        if directory.parent:
            parent_name = directory.parent.name
            # If parent directory contains season info, it's likely a series name
            if re.search(r'\b(?:season|staffel)\b', parent_name, re.IGNORECASE):
                # Extract series name by removing season info
                series_name = re.sub(
                    r'\s*\b(?:[Ss]eason|[Ss]taffel)\b(?:\s*\d+)?\b.*',
                    '',
                    parent_name,
                    flags=re.IGNORECASE,
                )
                return series_name.strip()
            # If parent directory contains year pattern, it might be a series
            elif re.search(r'\(\d{4}\)', parent_name):
                return parent_name
        return None

    def _extract_series_title_year(self, directory: Path) -> tuple[str | None, str | None]:
        candidates: list[str] = [directory.name]

        series_context = self._get_series_context(directory)
        if series_context:
            candidates.append(series_context)

        if directory.parent:
            candidates.append(directory.parent.name)

        for candidate in candidates:
            title, year = self._extract_title_year(candidate)
            if title and len(title) >= 2:
                return title, year

        return None, None

    def _normalize_title(self, value: str | None) -> str:
        if not value:
            return ""
        value = value.lower()
        value = re.sub(r"[^a-z0-9äöüß]+", " ", value)
        return re.sub(r"\s+", " ", value).strip()

    def _default_match_index(self, matches: list[MediaMatch], title: str | None, year: str | None) -> int:
        if not matches:
            return 0

        source = self._normalize_title(title)
        best_idx = 0
        best_score = -1

        for idx, match in enumerate(matches):
            score = 0
            if year and match.year == year:
                score += 3

            candidate = self._normalize_title(match.title or match.original_title)
            if source and candidate:
                if source == candidate:
                    score += 3
                elif source in candidate or candidate in source:
                    score += 2

            score += max(0, 1.0 - (idx * 0.1))

            if score > best_score:
                best_score = score
                best_idx = idx

        return best_idx

    def _is_collection(self, folder_name: str, videos: list[VideoFile]) -> bool:
        if re.search(r'[Ss]\d{1,2}[Ee]\d{1,3}|\d{1,2}[xX]\d{1,3}|season|staffel', folder_name, re.IGNORECASE):
            return False

        if any(v.episode_info for v in videos):
            return False

        for pat in self.COLLECTION_PATTERNS:
            if re.search(pat, folder_name, re.IGNORECASE):
                return True

        if len(videos) >= 2:
            years = set(v.extracted_year for v in videos if v.extracted_year)
            if len(years) >= 2:
                return True

            titles = set(v.extracted_title for v in videos if v.extracted_title)
            if len(titles) >= 2:
                return True

            big_movies = [v for v in videos if v.size_gb > 1 and not v.episode_info]
            if len(big_movies) >= 2:
                return True

        return False

    def _detect_type(self, directory: Path) -> tuple[MediaType, list[VideoFile]]:
        """Detect media type with enhanced series recognition.
        
        Improved version that considers:
        - Parent folder context for series identification
        - Enhanced pattern matching for season/episode info
        - Series context from folder structure
        - Better handling of episodes in separate folders
        
        Args:
            directory: Directory to analyze
            
        Returns:
            tuple: Media type and list of video files
        """
        videos = self._find_videos(directory)
        # Check if this is an audiobook directory
        if self._is_audiobook(directory):
            return MediaType.UNKNOWN, []
            
        if not videos:
            return MediaType.UNKNOWN, videos

        eps = [v for v in videos if v.media_type == MediaType.SERIES]
        if eps:
            return MediaType.SERIES, videos

        if self._is_collection(directory.name, videos):
            return MediaType.COLLECTION, videos

        movies = [v for v in videos if v.media_type == MediaType.MOVIE]
        if movies and movies[0].size_gb > 1:
            return MediaType.MOVIE, videos

        # Check for series patterns in directory name and parent directory name
        dir_and_parent_names = [directory.name]
        if directory.parent:
            dir_and_parent_names.append(directory.parent.name)

        for name in dir_and_parent_names:
            if re.search(r's\d{1,2}|season|staffel', name, re.I):
                return MediaType.SERIES, videos

        # Enhanced series detection for separate episode folders
        # Check if we have episode info in any video files
        if any(v.episode_info for v in videos):
            return MediaType.SERIES, videos

        # Check for series context from parent folders
        series_context = self._get_series_context(directory)
        if series_context:
            return MediaType.SERIES, videos

        # Check if this looks like an episode folder (common pattern: E074, Ep074, Episode 74, numeric folders like 074)
        folder_name = directory.name
        # Match patterns like "Ep 07", "E07", "Episode 007" or numerical folders like "074" at the end
        if re.search(r"(?:\b[Ee][Pp]?[\.\-\s]?\d{1,3}\b)|(?:\b(?:Episode|Folge)[\.\-\s]?\d{1,3}\b)|(?:\b\d{2,4}\b)$", folder_name):
            # This looks like an episode folder - check parent for series name
            if directory.parent and directory.parent.name:
                parent_name = directory.parent.name
                # If parent doesn't contain season/episode or numeric patterns, it might be the series name
                if not re.search(r'season|staffel|\d+x\d+', parent_name, re.I):
                    # Extract title from parent directory to help with series detection
                    title, year = self._extract_title_year(parent_name)
                    if title and len(title) >= 3:
                        # This is likely an episode folder within a series structure
                        return MediaType.SERIES, videos

        return (videos[0].media_type if videos else MediaType.UNKNOWN), videos

    # ==================== TMDB API ====================

    def _tmdb_request(self, endpoint: str, params: dict | None = None) -> dict | None:
        url = f"{self.TMDB_BASE}{endpoint}"
        if params:
            url += "?" + urllib.parse.urlencode(params)

        if url in self._cache:
            return self._cache[url]

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode())
                self._cache[url] = data
                return data
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2)
                return self._tmdb_request(endpoint, params)
            return None
        except Exception:
            return None

    def _get_imdb_id(self, tmdb_id: int, media_type: str) -> str | None:
        endpoint = f"/{media_type}/{tmdb_id}/external_ids"
        data = self._tmdb_request(endpoint)

        if not data:
            return None

        return data.get('imdb_id')

    def _generate_search_variants(self, title: str, year: str | None) -> list[tuple[str, str | None]]:
        variants = []

        variants.append((title, year))
        variants.append((title, None))

        # Convert ASCII umlauts to actual umlauts (ue -> ü)
        title_umlaut = self._to_umlauts(title)
        if title_umlaut != title:
            variants.append((title_umlaut, year))
            variants.append((title_umlaut, None))

        # Convert actual umlauts to ASCII (ü -> ue) - for searching English titles
        title_ascii = self._from_umlauts(title)
        if title_ascii != title:
            variants.append((title_ascii, year))
            variants.append((title_ascii, None))

        if ' And ' in title or ' and ' in title:
            title_amp = re.sub(r'\s+[Aa]nd\s+', ' & ', title)
            variants.append((title_amp, year))
            variants.append((title_amp, None))

        words = title.split()
        if len(words) > 3:
            short2 = ' '.join(words[:2])
            variants.append((short2, year))

            short3 = ' '.join(words[:3])
            variants.append((short3, year))

        if ' - ' in title:
            main_title = title.split(' - ')[0].strip()
            if len(main_title) > 3:
                variants.append((main_title, year))

        seen = set()
        unique = []
        for v in variants:
            if v not in seen:
                seen.add(v)
                unique.append(v)

        return unique

    def _search_tmdb(self, title: str, year: str | None, mtype: MediaType) -> list[MediaMatch]:
        results: list[MediaMatch] = []
        seen_ids: set[int] = set()

        endpoint = "/search/movie" if mtype != MediaType.SERIES else "/search/tv"
        media_str = "movie" if mtype != MediaType.SERIES else "tv"

        search_variants = self._generate_search_variants(title, year)

        for query, yr in search_variants:
            if len(results) >= 8:
                break

            params = {'query': query, 'language': 'de-DE', 'include_adult': 'false'}
            if yr:
                params['year' if mtype != MediaType.SERIES else 'first_air_date_year'] = yr

            data = self._tmdb_request(endpoint, params)

            if not data or 'results' not in data:
                continue

            for item in data['results']:
                tmdb_id = item.get('id')
                if not tmdb_id or tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                if mtype != MediaType.SERIES:
                    release_date = item.get('release_date', '')
                    item_title = item.get('title', '')
                    orig_title = item.get('original_title', '')
                else:
                    release_date = item.get('first_air_date', '')
                    item_title = item.get('name', '')
                    orig_title = item.get('original_name', '')

                item_year = release_date[:4] if release_date else ''

                imdb_id = None
                if len(results) < 3:
                    imdb_id = self._get_imdb_id(tmdb_id, media_str)

                results.append(MediaMatch(
                    tmdb_id=tmdb_id,
                    imdb_id=imdb_id,
                    title=item_title,
                    original_title=orig_title,
                    year=item_year,
                    media_type=media_str,
                    popularity=item.get('popularity', 0)
                ))

        if year:
            results.sort(key=lambda r: (0 if r.year == year else 1, -r.popularity))
        else:
            results.sort(key=lambda r: -r.popularity)

        return results[:10]

    def _lookup_by_tmdb_id(self, tmdb_id: int) -> MediaMatch | None:
        """Look up a movie/TV show by TMDb ID."""
        # Try as movie first
        endpoint = f"/movie/{tmdb_id}"
        params = {'language': 'de-DE'}
        data = self._tmdb_request(endpoint, params)

        if data and 'id' in data:
            imdb_id = self._get_imdb_id(tmdb_id, 'movie')
            return MediaMatch(
                tmdb_id=tmdb_id,
                imdb_id=imdb_id,
                title=data.get('title', ''),
                original_title=data.get('original_title', ''),
                year=(data.get('release_date', '') or '')[:4],
                media_type='movie'
            )

        # Try as TV series
        endpoint = f"/tv/{tmdb_id}"
        data = self._tmdb_request(endpoint, params)

        if data and 'id' in data:
            imdb_id = self._get_imdb_id(tmdb_id, 'tv')
            return MediaMatch(
                tmdb_id=tmdb_id,
                imdb_id=imdb_id,
                title=data.get('name', ''),
                original_title=data.get('original_name', ''),
                year=(data.get('first_air_date', '') or '')[:4],
                media_type='tv'
            )

        return None

    def _manual_lookup_direct(self, manual: str, folder_name: str = "") -> MediaMatch | None:
        """Create a MediaMatch directly from user input without TMDb lookup.
        
        This allows saving any ID the user provides, even if not found in TMDb.
        """
        if not manual:
            return None
        
        # Clean the input
        manual = manual.strip()
        
        # Determine if it's a TMDb or IMDB ID
        if manual.startswith('tt'):
            # IMDB ID
            return MediaMatch(
                tmdb_id=0,
                imdb_id=manual,
                title=folder_name,  # Use folder name as fallback title
                original_title="",
                year="",
                media_type='movie'
            )
        elif manual.isdigit():
            # Try TMDb API first - this gets us proper title/year
            tmdb_result = self._lookup_by_tmdb_id(int(manual))
            if tmdb_result:
                return tmdb_result
            
            # Fallback: just save the ID without proper title
            return MediaMatch(
                tmdb_id=int(manual),
                imdb_id=f"tmdb{manual}",  # Use tmdb prefix
                title=folder_name,
                original_title="",
                year="",
                media_type='movie'
            )
        elif manual.startswith('tmdb:'):
            # TMDb ID with prefix
            tmdb_id = manual[5:].strip()
            if tmdb_id.isdigit():
                return MediaMatch(
                    tmdb_id=int(tmdb_id),
                    imdb_id=f"tmdb{tmdb_id}",
                    title=folder_name,
                    original_title="",
                    year="",
                    media_type='movie'
                )
        
        # If it's a title, search as last resort
        return self._manual_lookup(manual)

    def _manual_lookup(self, manual: str) -> MediaMatch | None:
        if not manual:
            return None

        # Support explicit prefixes: imdb:tt1234567 or tmdb:12345
        manual_lower = manual.lower()
        if manual_lower.startswith('imdb:'):
            manual = manual[5:]  # Remove 'imdb:' prefix
        elif manual_lower.startswith('tmdb:'):
            # TMDb ID - directly fetch
            tmdb_id = manual[5:].strip()
            if tmdb_id.isdigit():
                return self._lookup_by_tmdb_id(int(tmdb_id))
            return None
        
        # For pure numeric input - ALWAYS treat as TMDb ID first
        # TMDb IDs are typically 5-7 digits, IMDB IDs need 'tt' prefix
        if manual.isdigit() and len(manual) >= 1:
            # Try as TMDb ID
            tmdb_result = self._lookup_by_tmdb_id(int(manual))
            if tmdb_result:
                return tmdb_result
            
            # If not found and starts with 0, try removing leading zero
            if manual.startswith('0'):
                tmdb_result = self._lookup_by_tmdb_id(int(manual.lstrip('0')))
                if tmdb_result:
                    return tmdb_result

        # Check for tt prefix or 7+ digit IMDB ID (only if not already handled as TMDb)
        if manual.startswith('tt') or (manual.isdigit() and len(manual) >= 7):
            if not manual.startswith('tt'):
                manual = 'tt' + manual

            if re.match(r'^tt\d{7,8}$', manual):
                endpoint = f"/find/{manual}"
                params = {'external_source': 'imdb_id'}
                data = self._tmdb_request(endpoint, params)

                if data:
                    results = data.get('movie_results', []) + data.get('tv_results', [])
                    if results:
                        item = results[0]
                        is_movie = 'title' in item
                        return MediaMatch(
                            tmdb_id=item['id'],
                            imdb_id=manual,
                            title=item.get('title' if is_movie else 'name', ''),
                            original_title=item.get('original_title' if is_movie else 'original_name', ''),
                            year=(item.get('release_date' if is_movie else 'first_air_date', '') or '')[:4],
                            media_type='movie' if is_movie else 'tv'
                        )

        # If manual input is a title (contains letters and spaces), search for it
        if len(manual) > 2 and not manual.startswith('tt') and not manual.isdigit():
            # Try searching as a movie title
            search_results = self._search_tmdb(manual, None, MediaType.MOVIE)
            if search_results:
                best = search_results[0]
                # Return match without IMDB validation - some TMDb entries have no IMDB
                return MediaMatch(
                    tmdb_id=best.tmdb_id,
                    imdb_id=best.imdb_id,  # May be None, that's okay
                    title=best.title,
                    original_title=best.original_title,
                    year=best.year,
                    media_type=best.media_type
                )
            # Try as TV series
            search_results = self._search_tmdb(manual, None, MediaType.SERIES)
            if search_results:
                best = search_results[0]
                return MediaMatch(
                    tmdb_id=best.tmdb_id,
                    imdb_id=best.imdb_id,
                    title=best.title,
                    original_title=best.original_title,
                    year=best.year,
                    media_type=best.media_type
                )

        return None

    # ==================== SCANNING ====================

    def scan_folder(self, folder: Path) -> ScanResult:
        folder_name = folder.name
        named_match = re.match(r'^(?P<title>.+)\s\((?P<year>\d{4})\)\s\[(?P<tag>imdbid-(?P<imdb>tt\d{7,}))\]$', folder_name)

        detected_type, videos = self._detect_type(folder)
        title, year = self._extract_title_year(folder_name)

        # If folder name doesn't yield a good title, try to get title from video files
        # Check if extracted title is usable for TMDb search
        def is_good_title(t: str | None) -> bool:
            if not t or len(t) < 3:
                return False
            # Must have at least one word with 5+ letters AND at least 2 words total
            words = re.findall(r'[a-zA-ZäöüÄÖÜß]{5,}', t, re.I)
            if len(words) < 1:
                return False
            # Also check for at least 2 words (real titles usually have multiple words)
            word_count = len(t.split())
            return word_count >= 2

        if not is_good_title(title):
            for v in videos:
                if is_good_title(v.extracted_title):
                    title = v.extracted_title
                    year = v.extracted_year
                    break

        if named_match:
            title = named_match.group('title').strip()
            year = named_match.group('year')

        if detected_type == MediaType.SERIES:
            series_title, series_year = self._extract_series_title_year(folder)
            if series_title:
                title = series_title
            if series_year:
                year = series_year

        result = ScanResult(
            path=folder, folder_name=folder_name,
            detected_type=detected_type,
            extracted_title=title, extracted_year=year,
            videos=videos, matches=[]
        )

        if named_match and detected_type != MediaType.SERIES:
            result.status = MatchStatus.DONE
            return result

        if detected_type == MediaType.SERIES:
            result.series_name = title
            season = self._extract_season_info(folder_name)
            if not season:
                for v in videos:
                    if v.episode_info:
                        season = v.episode_info.season
                        break
            result.season_number = season

            if named_match:
                imdb_id = named_match.group('imdb')
                series_match = self._manual_lookup(imdb_id)
                if not series_match:
                    series_match = MediaMatch(
                        tmdb_id=0,
                        imdb_id=imdb_id,
                        title=title or folder_name,
                        original_title=title or folder_name,
                        year=year or '',
                        media_type='tv',
                        popularity=0.0,
                    )
                result.selected_match = series_match
                result.status = MatchStatus.AUTO
                return result

        if not videos:
            result.status = MatchStatus.NONE
            result.error = "No videos"
            return result

        if detected_type == MediaType.COLLECTION:
            result.status = MatchStatus.MANUAL
            result.error = f"Collection ({len(videos)} films)"
            result.collection_items = self._prepare_collection_items(videos, folder_name)
            return result

        if not title:
            result.status = MatchStatus.NONE
            result.error = "No title"
            return result

        print(f"  [DEBUG] Searching TMDb for: title='{title}', year='{year}', type={detected_type}")
        
        matches = self._search_tmdb(title, year, detected_type)
        result.matches = matches

        print(f"  [DEBUG] Found {len(matches)} matches")
        for i, m in enumerate(matches[:3]):
            print(f"    [{i+1}] {m.title} ({m.year}) [tmdb={m.tmdb_id}, imdb={m.imdb_id}]")

        # Check if we have a saved manual mapping for this folder
        if folder_name in self._manual_mappings:
            saved_imdb_id = self._manual_mappings[folder_name]
            print(f"  [DEBUG] Found saved mapping: {saved_imdb_id}")
            # Try to look up the saved IMDB ID
            saved_match = self._manual_lookup(saved_imdb_id)
            if saved_match:
                result.selected_match = saved_match
                result.status = MatchStatus.MANUAL
                print(f"  ✓ Using saved match: {saved_match.title} ({saved_match.year}) [{saved_match.imdb_id}]")
                return result
            else:
                print(f"  ⚠ Saved mapping not found in TMDb, will try search results")

        if not matches:
            result.status = MatchStatus.NONE
            result.error = "No matches"
        elif len(matches) == 1 and year and matches[0].year == year:
            result.status = MatchStatus.AUTO
            result.selected_match = matches[0]
        else:
            result.status = MatchStatus.UNSURE

        return result

    def _prepare_collection_items(self, videos: list[VideoFile], collection_folder_name: str | None = None) -> list[CollectionItem]:
        items: list[CollectionItem] = []

        # Helper to check if title is usable (not just any 5+ letter string)
        def is_good_title(t: str | None) -> bool:
            if not t or len(t) < 3:
                return False
            # Must have at least one word with 5+ letters AND at least 2 words total
            words = re.findall(r'[a-zA-ZäöüÄÖÜß]{5,}', t, re.I)
            if len(words) < 1:
                return False
            # Also check for at least 2 words (real titles usually have multiple words)
            word_count = len(t.split())
            return word_count >= 2

        # Extract potential title from collection folder name
        # e.g., "My.Movie.1-5" -> "My Movie" (remove "1-5" part)
        collection_title = None
        collection_year = None
        if collection_folder_name:
            raw_title, collection_year = self._extract_title_year(collection_folder_name)
            if raw_title:
                # Remove patterns like "1-5", "1", "2", "3" at the end that indicate collection numbers
                # This converts "My Movie 1-5" to "My Movie"
                collection_title = re.sub(r'\s+\d+(-\d+)?\s*$', '', raw_title).strip()
                # If removal left only one word, use the original
                if len(collection_title.split()) < 2:
                    collection_title = raw_title

        for v in videos:
            title = v.extracted_title
            year = v.extracted_year

            # If video has a parent folder with good name, use that
            if v.parent_folder:
                parent_title, parent_year = self._extract_title_year(v.parent_folder.name)
                if is_good_title(parent_title):
                    title = parent_title
                    year = parent_year or year

            # If title is not usable, try collection folder name
            # e.g., "My.Awesome.Movie.1-5" with files "xyz1.mkv"
            if not is_good_title(title) and is_good_title(collection_title):
                # Try to extract movie number from filename (e.g., "xyz1" -> 1, "xyz2" -> 2)
                filename_stem = v.path.stem
                num_match = re.search(r'(\d+)$', filename_stem)
                movie_number = num_match.group(1) if num_match else None
                
                if movie_number:
                    # Create title with number: "My Movie" + " 1" = "My Movie 1"
                    title = f"{collection_title} {movie_number}"
                else:
                    title = collection_title
                year = collection_year or year

            matches = []
            if title:
                matches = self._search_tmdb(title, year, MediaType.MOVIE)

            item = CollectionItem(
                folder_path=v.parent_folder,
                video_path=v.path,
                video_size=v.size_gb,
                extracted_title=title,
                extracted_year=year,
                matches=matches,
                selected_match=matches[0] if len(matches) == 1 and year and matches[0].year == year else None,
                status=MatchStatus.AUTO if (matches and len(matches) == 1 and year and matches[0].year == year) else MatchStatus.UNSURE
            )
            items.append(item)

        return items

    def _group_series_folders(self, results: list[ScanResult]) -> dict[str, list[ScanResult]]:
        """Group folders that likely belong to the same series based on name similarity.
        
        This helps with series where episodes are in separate folders by grouping them
        together for better context during matching.
        
        Args:
            results: List of scan results to group
            
        Returns:
            dict: Mapping of series identifiers to lists of related results
        """
        series_groups: dict[str, list[ScanResult]] = {}
        
        for result in results:
            if result.detected_type != MediaType.SERIES:
                continue
                
            # Extract series name by removing season/episode info
            series_name = result.folder_name
            
            # Remove season/episode patterns
            for pattern in self.EP_PATTERNS + self.SEASON_PATTERNS:
                series_name = re.sub(pattern, '', series_name, flags=re.IGNORECASE)
            
            # Clean up the name
            series_name = re.sub(r'[^\w\s\-äöüÄÖÜ]', ' ', series_name)
            series_name = re.sub(r'\s+', ' ', series_name).strip()
            
            # Use first significant word(s) as key for grouping
            words = series_name.split()
            if len(words) >= 2:
                # Use first 2-3 words as series identifier
                key = ' '.join(words[:min(3, len(words))]).lower()
            elif words:
                key = words[0].lower()
            else:
                key = series_name.lower()
                
            if key not in series_groups:
                series_groups[key] = []
            series_groups[key].append(result)
        
        # Only return groups with multiple entries
        return {k: v for k, v in series_groups.items() if len(v) > 1}

    def scan_all(self, folders: list[Path], show_progress: bool = True) -> list[ScanResult]:
        results: list[ScanResult] = []

        if show_progress:
            print(f"\n  🔍 Scanning {len(folders)} folders...")

        for i, folder in enumerate(folders):
            if show_progress:
                pct = (i + 1) / len(folders) * 100
                print(f"  [{i+1}/{len(folders)}] {pct:.0f}% {folder.name[:40]}...", end="\r")

            if self._is_audiobook(folder):
                continue

            result = self.scan_folder(folder)
            results.append(result)

        if show_progress:
            print(" " * 80, end="\r")

        # Group series folders for better context
        series_groups = self._group_series_folders(results)
        
        # Add context information to series results
        for group_key, group_results in series_groups.items():
            # If we have multiple folders for the same series,
            # we can use this context to improve matching
            if len(group_results) > 1:
                for result in group_results:
                    # We could add more sophisticated logic here to share information
                    # between episodes of the same series, but for now we'll just
                    # note that this is part of a group
                    pass

        return results

    # ==================== COLLECTION HANDLING ====================

    def handle_collection(self, result: ScanResult) -> list[ScanResult]:
        print(f"\n{'═' * 80}")
        print(f"  📦 COLLECTION: {result.folder_name[:60]}")
        print(f"{'═' * 80}")

        if not result.collection_items:
            print("  ❌ No items found")
            return []

        print(f"\n  {len(result.collection_items)} movies found:\n")

        for i, item in enumerate(result.collection_items, 1):
            status = item.status.value
            size = f"{item.video_size:.1f}GB"

            if item.folder_path:
                source = f"📁 {item.folder_path.name[:45]}"
            else:
                source = f"📄 {item.video_path.name[:45]}"

            title = item.extracted_title or "(not detected)"
            year = item.extracted_year or "?"

            if item.selected_match:
                match_str = f"→ {item.selected_match.title} ({item.selected_match.year})"
                if item.selected_match.imdb_id:
                    match_str += f" [{item.selected_match.imdb_id}]"
            elif item.matches:
                match_str = f"→ [{len(item.matches)} matches]"
            else:
                match_str = "→ (no matches)"

            print(f"  {status} {i:2}. [{size:>6}] {source}")
            print(f"              Detected: {title} ({year})")
            print(f"              {match_str}")
            print()

        auto_count = sum(1 for i in result.collection_items if i.status == MatchStatus.AUTO)
        unsure_count = sum(1 for i in result.collection_items if i.status == MatchStatus.UNSURE)
        none_count = sum(1 for i in result.collection_items if i.status == MatchStatus.NONE)

        print(f"{'─' * 80}")
        print(f"  Status: ✓ {auto_count} Automatic | ? {unsure_count} Uncertain | ✗ {none_count} No matches")
        print(f"""
      Commands:
        <Enter>  Accept all AUTO entries, review uncertain only
        a        Review every movie individually
        s        Rename everything (preserve existing subfolders)
        0        Skip
        q        Back
        """)

        while True:
            choice = input("  Choice (Enter=accept AUTO entries): ").strip().lower()

            if choice == 'q':
                return []

            if choice == '0':
                result.status = MatchStatus.SKIP
                return [result]

            if choice in ('', 's', 'a'):
                break

            print("  ❌ Invalid")

        if choice == 'a':
            indices_to_review = list(range(len(result.collection_items)))
        else:
            indices_to_review = [i for i, item in enumerate(result.collection_items)
                                 if item.status != MatchStatus.AUTO]

        for idx in indices_to_review:
            item = result.collection_items[idx]

            print(f"\n{'─' * 60}")
            if item.folder_path:
                print(f"  📁 {item.folder_path.name[:55]}")
            else:
                print(f"  📄 {item.video_path.name[:55]}")
            print(f"  Detected: {item.extracted_title or '?'} ({item.extracted_year or '?'})")

            if item.matches:
                default_idx = self._default_match_index(item.matches, item.extracted_title, item.extracted_year)
                print(f"\n  Matches:")
                for j, m in enumerate(item.matches[:6], 1):
                    marker = "▶" if (j - 1) == default_idx else " "
                    id_info = ""
                    if m.imdb_id:
                        id_info = f" [imdbid-{m.imdb_id}]"
                    if m.tmdb_id:
                        id_info += f" [tmdbid-{m.tmdb_id}]"
                    print(f"    {marker} {j}. {m.title} ({m.year}){id_info}")

                print(f"\n    0 = Skip")
                print(f"    m = Enter manual ID")
                print(f"    Enter = suggested #{default_idx + 1}")

                while True:
                    sel = input(f"\n  Choice (Enter=suggested #{default_idx + 1}): ").strip().lower()

                    if sel == '0':
                        item.status = MatchStatus.SKIP
                        break

                    if sel == 'm' or sel == 'x':
                        manual = input("  ID (tt.../TMDb/Titel): ").strip()
                        match = self._manual_lookup_direct(manual, result.folder_name if result else "")
                        if match:
                            item.selected_match = match
                            item.status = MatchStatus.MANUAL
                            # Save manual mapping for future runs
                            folder_name = item.folder_path.name if item.folder_path else None
                            if match.imdb_id and folder_name:
                                self._manual_mappings[folder_name] = match.imdb_id
                                self._save_manual_mappings()
                                print(f"  ✓ {match.title} ({match.year}) [{match.imdb_id}] (saved)")
                            else:
                                print(f"  ✓ {match.title} ({match.year}) [{match.imdb_id}]")
                        else:
                            print("  ❌ Not found")
                        break

                    try:
                        num = int(sel) if sel else (default_idx + 1)
                        if 1 <= num <= len(item.matches):
                            item.selected_match = item.matches[num - 1]
                            item.status = MatchStatus.AUTO
                            break
                    except ValueError:
                        pass
                    print("  ❌ Invalid")
            else:
                print(f"\n  No matches. m/x = Manual, 0 = Skip")
                sel = input("  Choice (Enter=Skip): ").strip().lower()

                if sel == 'm' or sel == 'x':
                    manual = input("  ID (tt.../TMDb/Titel): ").strip()
                else:
                    # Allow direct ID input
                    manual = sel if sel else None

                if manual:
                    match = self._manual_lookup_direct(manual, result.folder_name if result else "")
                    if match:
                        item.selected_match = match
                        item.status = MatchStatus.MANUAL
                        # Save manual mapping for future runs
                        folder_name = item.folder_path.name if item.folder_path else None
                        if match.imdb_id and folder_name:
                            self._manual_mappings[folder_name] = match.imdb_id
                            self._save_manual_mappings()
                            print(f"  ✓ {match.title} ({match.year}) [{match.imdb_id}] (saved)")
                        else:
                            print(f"  ✓ {match.title} ({match.year}) [{match.imdb_id}]")
                    else:
                        print("  ❌ Not found")
                        item.status = MatchStatus.SKIP

        new_results: list[ScanResult] = []

        for item in result.collection_items:
            if item.status in (MatchStatus.AUTO, MatchStatus.MANUAL) and item.selected_match:
                new_result = ScanResult(
                    path=item.folder_path if item.folder_path else item.video_path,
                    folder_name=item.folder_path.name if item.folder_path else item.video_path.name,
                    detected_type=MediaType.MOVIE,
                    extracted_title=item.extracted_title,
                    extracted_year=item.extracted_year,
                    videos=[],
                    matches=item.matches,
                    selected_match=item.selected_match,
                    status=item.status
                )
                new_results.append(new_result)

        if new_results:
            print(f"\n  ✓ {len(new_results)} movies prepared")

        result.status = MatchStatus.SKIP

        return new_results

    # ==================== UI ====================

    def show_scan_results(self, results: list[ScanResult]) -> None:
        print(f"\n{'═' * 80}")
        print(f"  📋 SCAN RESULTS")
        print(f"{'═' * 80}")

        by_status = {}
        for r in results:
            by_status[r.status] = by_status.get(r.status, 0) + 1

        print(f"\n  Total: {len(results)} folders")
        status_order = [MatchStatus.AUTO, MatchStatus.UNSURE, MatchStatus.MANUAL,
                        MatchStatus.NONE, MatchStatus.SKIP, MatchStatus.DONE, MatchStatus.RENAMED]
        for status in status_order:
            if status in by_status:
                print(f"    {status.value} {status.name}: {by_status[status]}")

        print(f"\n{'─' * 80}")
        print(f"  {'#':>3}  {'St':>2}  {'Type':>4}  {'Folder':<35}  {'→ Match':<25}")
        print(f"{'─' * 80}")

        for i, r in enumerate(results, 1):
            status = r.status.value
            mtype = r.detected_type.name[:4] if r.detected_type else "?"
            folder = r.folder_name[:35]
            
            if r.selected_match:
                match = f"→ {r.selected_match.title} ({r.selected_match.year})"
            elif r.matches:
                match = f"→ [{len(r.matches)} matches]"
            elif r.error:
                match = f"→ {r.error}"
            else:
                match = ""
            
            print(f"  {i:>3}  {status:>2}  {mtype:>4}  {folder:<35}  {match[:25]}")

    def _build_series_batch_preview(
        self,
        results: list[ScanResult],
        include_candidates: bool = False,
    ) -> list[dict[str, Any]]:
        batches: dict[str, dict[str, Any]] = {}

        for result in results:
            match = result.selected_match

            if not match and include_candidates and result.detected_type == MediaType.SERIES and result.matches:
                default_idx = self._default_match_index(result.matches, result.extracted_title, result.extracted_year)
                match = result.matches[default_idx]

            if not match:
                continue

            is_series = result.detected_type == MediaType.SERIES or match.media_type == 'tv'
            if not is_series:
                continue

            title = match.title or match.original_title or result.extracted_title or result.folder_name
            year = match.year or result.extracted_year or '0000'
            imdb = match.imdb_id or ''

            display_name = f"{title} ({year})"
            if imdb:
                display_name += f" [imdbid-{imdb}]"

            entry = batches.setdefault(display_name, {
                'imdb': imdb,
                'folders': 0,
                'seasons': set(),
                'members': [],
            })
            entry['folders'] = int(entry['folders']) + 1  # type: ignore[union-attr]
            members = entry['members']
            if isinstance(members, list):
                members.append(result.folder_name)

            if result.season_number:
                seasons = entry['seasons']
                if isinstance(seasons, set):
                    seasons.add(result.season_number)

        preview: list[dict[str, Any]] = []
        for name, info in batches.items():
            imdb = str(info.get('imdb', ''))
            folders = int(info['folders'])  # type: ignore[arg-type]
            seasons_obj = info.get('seasons', set())
            seasons = seasons_obj if isinstance(seasons_obj, set) else set()
            members_obj = info.get('members', [])
            members = members_obj if isinstance(members_obj, list) else []
            preview.append({
                'imdb': imdb,
                'name': name,
                'folders': folders,
                'seasons': seasons,
                'members': members,
            })

        preview.sort(key=lambda item: (
            1 if not str(item.get('imdb', '')) else 0,
            str(item.get('imdb', '')),
            str(item.get('name', '')).lower(),
        ))
        return preview

    def interactive_review(self, results: list[ScanResult], dry_run: bool = True) -> list[ScanResult]:

        while True:
            to_review = [i for i, r in enumerate(results)
                         if r.status in (MatchStatus.UNSURE, MatchStatus.NONE, MatchStatus.MANUAL)
                         and r.status != MatchStatus.RENAMED]
            ready = [i for i, r in enumerate(results)
                     if r.status in (MatchStatus.AUTO, MatchStatus.MANUAL) and r.selected_match]
            done = [i for i, r in enumerate(results)
                    if r.status in (MatchStatus.RENAMED, MatchStatus.DONE)]

            print(f"\n{'═' * 80}")
            print(f"  🔧 INTERACTIVE REVIEW")
            print(f"{'═' * 80}")
            print(f"\n  Status:")
            print(f"    ✓ Ready: {len(ready)}   ? To review: {len(to_review)}   ✔ Done: {len(done)}")

            if self.series_batch_mode:
                preview = self._build_series_batch_preview(results, include_candidates=True)
                merge_groups = [p for p in preview if int(p['folders']) > 1]  # type: ignore[arg-type]
                if merge_groups:
                    print(f"\n  🔗 Series batch preview ({len(merge_groups)} merge group(s)):")
                    for group in merge_groups:
                        name = str(group.get('name', ''))
                        folders = int(group['folders'])  # type: ignore[arg-type]
                        seasons_obj = group.get('seasons', set())
                        seasons = seasons_obj if isinstance(seasons_obj, set) else set()
                        season_text = self._format_season_summary(seasons)
                        members_obj = group.get('members', [])
                        members = members_obj if isinstance(members_obj, list) else []
                        members_short = ", ".join(m[:18] for m in members[:3])
                        more = " ..." if len(members) > 3 else ""
                        print(f"    • {name[:56]} ← {folders} folders ({season_text})")
                        print(f"      sources: {members_short}{more}")

            print(f"""

    Commands:
        <Enter>  Handle uncertain items ({len(to_review)})
        x        Rename {len(ready)} now {'[DRY RUN]' if dry_run else '[EXECUTE]'}
        1,3,5    Specific numbers
        a        Review all
        l        Show list
        q        Quit
            """)

            choice = input("  Choice (Enter=handle uncertain items): ").strip().lower()

            if choice == 'q':
                return results

            if choice == 'l':
                self.show_scan_results(results)
                continue

            if choice == 'x':
                ok, skip, err = self.execute_renames(results, dry_run=dry_run)
                print(f"\n  Result: ✅ {ok}  ⏭️ {skip}  ❌ {err}")

                for r in results:
                    if r.status in (MatchStatus.AUTO, MatchStatus.MANUAL) and r.selected_match and not dry_run:
                        r.status = MatchStatus.RENAMED

                if dry_run:
                    print(f"\n  💡 Use -x to perform the renaming for real")

                input("\n  Press <Enter> to continue...")
                self.show_scan_results(results)
                continue

            if choice == '' or choice == 'auto':
                indices = to_review
            elif choice == 'a':
                indices = [i for i in range(len(results)) if results[i].status != MatchStatus.RENAMED]
            else:
                indices = []
                try:
                    parts = choice.replace(',', ' ').split()
                    for part in parts:
                        if '-' in part:
                            start, end = part.split('-')
                            indices.extend(range(int(start) - 1, int(end)))
                        else:
                            indices.append(int(part) - 1)
                    indices = [i for i in indices if 0 <= i < len(results)]
                except ValueError:
                    print("  ❌ Invalid")
                    continue

            if not indices:
                print("  ℹ️  Nothing to work on")
                continue

            for idx in indices:
                result = results[idx]

                if result.status in (MatchStatus.DONE, MatchStatus.RENAMED):
                    continue

                if result.detected_type == MediaType.COLLECTION:
                    new_results = self.handle_collection(result)
                    if new_results:
                        results.extend(new_results)
                    continue

                print(f"\n{'─' * 80}")
                print(f"  [{idx + 1}/{len(results)}] {result.folder_name[:60]}")
                print(f"{'─' * 80}")

                if result.extracted_title:
                    print(f"  Detected: {result.extracted_title} ({result.extracted_year or '?'})")

                if result.detected_type == MediaType.SERIES:
                    season_target = result.season_number or 1
                    print(f"  📺 Series recognized | planned target folder: Season {season_target:02d}")

                if result.matches:
                    default_idx = self._default_match_index(result.matches, result.extracted_title, result.extracted_year)
                    print(f"\n  Matches:")
                    for j, m in enumerate(result.matches[:8], 1):
                        year_match = "▶" if (j - 1) == default_idx else " "
                        id_info = ""
                        if m.imdb_id:
                            id_info = f" [imdbid-{m.imdb_id}]"
                        if m.tmdb_id:
                            id_info += f" [tmdbid-{m.tmdb_id}]"
                        print(f"    {year_match} {j}. {m.title} ({m.year}){id_info}")

                    print(f"\n    0 = Skip | m/x = Manual")
                    print(f"    Enter = suggested #{default_idx + 1}")

                    if result.detected_type == MediaType.SERIES:
                        default_match = result.matches[default_idx]
                        id_info = ""
                        if default_match.imdb_id:
                            id_info = f" [imdbid-{default_match.imdb_id}]"
                        if default_match.tmdb_id:
                            id_info += f" [tmdbid-{default_match.tmdb_id}]"
                        if not id_info:
                            id_info = " (IMDb pending)"
                        print(f"    Batch candidate: {default_match.title} ({default_match.year}){id_info}")

                    while True:
                        sel = input(f"\n  Choice (Enter=suggested #{default_idx + 1}): ").strip().lower()

                        if sel == '0':
                            result.status = MatchStatus.SKIP
                            break

                        if sel == 'm' or sel == 'x':
                            manual = input("  ID (tt.../TMDb/Titel): ").strip()
                            match = self._manual_lookup_direct(manual, result.folder_name if result else "")
                            if match:
                                result.selected_match = match
                                result.status = MatchStatus.MANUAL
                                # Save manual mapping for future runs
                                if match.imdb_id:
                                    self._manual_mappings[result.folder_name] = match.imdb_id
                                    self._save_manual_mappings()
                                    print(f"  ✓ {match.title} ({match.year}) [{match.imdb_id}] (saved)")
                                else:
                                    print(f"  ✓ {match.title} ({match.year}) [{match.imdb_id}]")
                            else:
                                print("  ❌ Not found")
                            break

                        try:
                            num = int(sel) if sel else (default_idx + 1)
                            if 1 <= num <= len(result.matches):
                                result.selected_match = result.matches[num - 1]
                                result.status = MatchStatus.AUTO
                                break
                        except ValueError:
                            pass
                        print("  ❌ Invalid")

                else:
                    print(f"\n  No matches. m/x = Manual | 0 = Skip")
                    sel = input("  Choice (Enter=Skip): ").strip().lower()

                    if sel == 'm' or sel == 'x':
                        manual = input("  ID (tt.../TMDb/Titel): ").strip()
                    else:
                        # Allow direct ID input (e.g., user types tt0120188 directly)
                        manual = sel if sel else None

                    if manual:
                        match = self._manual_lookup_direct(manual, result.folder_name if result else "")
                        if match:
                            result.selected_match = match
                            result.status = MatchStatus.MANUAL
                            # Save manual mapping for future runs
                            if match.imdb_id:
                                self._manual_mappings[result.folder_name] = match.imdb_id
                                self._save_manual_mappings()
                                print(f"  ✓ Saved mapping: {match.title} -> {match.imdb_id}")
                            else:
                                print(f"  ✓ Match: {match.title} ({match.year})")
                        else:
                            print("  ❌ Not found")
                            result.status = MatchStatus.SKIP
                    else:
                        result.status = MatchStatus.SKIP

            self.show_scan_results(results)

        return results

    # ==================== RENAME ====================

    def _sanitize(self, name: str) -> str:
        if not name:
            raise RenameError("Empty name")
        for c in '<>:"/\\|?*':
            name = name.replace(c, '')
        name = re.sub(r'[\x00-\x1f]', '', name)
        name = re.sub(r'\s+', ' ', name).strip('. ')
        if not name:
            raise RenameError("Name empty after sanitization")
        if len(name) > 200:
            raise RenameError(f"Name too long")
        return name

    def _sub_lang(self, filename: str) -> str:
        low = filename.lower()
        for code, norm in self.LANG_MAP.items():
            if re.search(rf'[._]{code}[._]', low):
                return f'.{norm}'
        if re.search(r'[._]forced[._]', low):
            return '.forced'
        return ''

    def _season_inference_details(self, path: Path, fallback: int | None = None) -> tuple[int, str]:
        candidates = [path.name]
        if path.parent:
            candidates.append(path.parent.name)
        if path.parent and path.parent.parent:
            candidates.append(path.parent.parent.name)

        for candidate in candidates:
            season = self._extract_season_info(candidate)
            if season:
                return season, f"season-pattern:{candidate}"

        for candidate in candidates:
            for pat in self.EP_PATTERNS:
                match = re.search(pat, candidate, re.IGNORECASE)
                if match:
                    try:
                        return int(match.group(1)), f"episode-pattern:{candidate}"
                    except (ValueError, IndexError):
                        pass

        for candidate in candidates:
            episode_info = self._parse_episode(candidate)
            if episode_info:
                return episode_info.season, f"episode-only:{candidate}"

        return fallback or 1, f"fallback:{fallback or 1}"

    def _guess_season_for_path(self, path: Path, fallback: int | None = None) -> int:
        season, _reason = self._season_inference_details(path, fallback=fallback)
        return season

    def _organize_series_files(self, root_dir: Path, fallback_season: int | None = None) -> tuple[int, set[int]]:
        moved_files = 0
        used_seasons: set[int] = set()
        episode_ext = self.VIDEO_EXT | self.SUB_EXT | frozenset({'.nfo'})

        def move_file_to_season(item: Path, season: int, reason: str) -> bool:
            nonlocal moved_files

            season_dir = root_dir / f"Season {season:02d}"
            season_dir.mkdir(parents=True, exist_ok=True)
            used_seasons.add(season)

            target = season_dir / item.name
            if item.parent == season_dir:
                if self.debug_series:
                    print(f"     🧭 SERIES DEBUG: keep '{item.name}' in Season {season:02d} ({reason})")
                return False

            if target.exists():
                stem = item.stem
                suffix = item.suffix
                counter = 2
                while True:
                    candidate = season_dir / f"{stem}.{counter}{suffix}"
                    if not candidate.exists():
                        target = candidate
                        break
                    counter += 1

            self._do_rename(item, target)
            moved_files += 1
            if self.debug_series:
                print(f"     🧭 SERIES DEBUG: move '{item.name}' -> 'Season {season:02d}/{target.name}' ({reason})")
            return True

        removed_junk = 0
        for item in [p for p in root_dir.rglob('*') if p.is_file()]:
            ext = item.suffix.lower()
            if ext in self.SCENE_TRASH_EXT or self._is_sample_video(item):
                try:
                    item.unlink()
                    removed_junk += 1
                    if self.debug_series:
                        print(f"     🧭 SERIES DEBUG: remove junk '{item.name}'")
                except OSError:
                    pass

        files = [
            item for item in root_dir.rglob('*')
            if item.is_file() and item.suffix.lower() in episode_ext
        ]
        for item in files:
            season, reason = self._season_inference_details(item, fallback=fallback_season)
            move_file_to_season(item, season, reason)

        remaining_files = [
            p for p in root_dir.rglob('*')
            if p.is_file()
            and p.suffix.lower() in episode_ext
            and p.parent != root_dir
            and not re.fullmatch(r'Season\s\d{2}', p.parent.name)
        ]
        for item in remaining_files:
            season, reason = self._season_inference_details(item, fallback=fallback_season)
            move_file_to_season(item, season, f"rescan-{reason}")

        for folder in sorted((d for d in root_dir.rglob('*') if d.is_dir()), key=lambda d: len(d.parts), reverse=True):
            if folder == root_dir:
                continue
            if re.fullmatch(r'Season\s\d{2}', folder.name):
                continue
            try:
                next(folder.iterdir())
            except StopIteration:
                folder.rmdir()
            except OSError:
                pass

        if removed_junk:
            print(f"     🧹 Series cleanup: removed {removed_junk} junk/sample file(s)")

        return moved_files, used_seasons

    def _format_season_summary(self, seasons: set[int]) -> str:
        if not seasons:
            return "(none)"

        ordered = sorted(seasons)
        if len(ordered) == 1:
            return f"Season {ordered[0]:02d}"

        return f"Seasons {ordered[0]:02d}-{ordered[-1]:02d}"

    def _is_sample_video(self, path: Path) -> bool:
        if path.suffix.lower() not in self.VIDEO_EXT:
            return False
        return bool(re.search(r'(^|[\._\-\s])sample([\._\-\s]|$)', path.stem, re.IGNORECASE))

    def execute_renames(self, results: list[ScanResult], dry_run: bool = True) -> tuple[int, int, int]:
        ok, skip, err = 0, 0, 0
        series_stats: dict[str, dict[str, Any]] = {}

        to_rename = [r for r in results
                     if r.status in (MatchStatus.AUTO, MatchStatus.MANUAL)
                     and r.selected_match
                     and r.status != MatchStatus.RENAMED]

        if self.series_batch_mode and to_rename:
            series_groups: dict[str, list[ScanResult]] = {}
            series_order: list[str] = []
            non_series: list[ScanResult] = []

            for r in to_rename:
                m = r.selected_match
                is_series = bool(m) and (r.detected_type == MediaType.SERIES or m.media_type == 'tv')
                if not is_series or not m:
                    non_series.append(r)
                    continue

                group_key = f"{m.imdb_id or ''}|{m.tmdb_id}|{(m.title or m.original_title or '').lower()}"
                if group_key not in series_groups:
                    series_groups[group_key] = []
                    series_order.append(group_key)
                series_groups[group_key].append(r)

            ordered: list[ScanResult] = []
            for key in series_order:
                ordered.extend(series_groups[key])
            ordered.extend(non_series)
            to_rename = ordered

        if not to_rename:
            return 0, len(results), 0

        print(f"\n{'═' * 80}")
        print(f"  {'📋 DRY RUN' if dry_run else '⚡ RENAMING'} - {len(to_rename)} folders")
        print(f"{'═' * 80}")

        if self.series_batch_mode:
            series_count = sum(
                1 for r in to_rename
                if r.selected_match and (r.detected_type == MediaType.SERIES or r.selected_match.media_type == 'tv')
            )
            if series_count:
                print(f"  🔗 SERIES BATCH MODE active ({series_count} series folders grouped)")

        for result in to_rename:
            self._ops.clear()

            match = result.selected_match
            if not match:
                skip += 1
                continue

            if not match.imdb_id:
                print(f"\n  🔍 Fetching IMDb ID for: {match.title} (TMDb: {match.tmdb_id})")
                match.imdb_id = self._get_imdb_id(match.tmdb_id, match.media_type)

            # Allow films without IMDB if TMDb has no IMDB link
            use_imdb_id = match.imdb_id
            if not use_imdb_id:
                # Only show warning but don't use tmdb prefix in filename
                print(f"  ⚠️ No IMDB ID available for: {match.title} (TMDb: {match.tmdb_id})")
                use_imdb_id = None

            if use_imdb_id and use_imdb_id.startswith('tt') and not re.match(r'^tt\d{7,}$', use_imdb_id):
                print(f"\n  ❌ {result.folder_name}")
                print(f"     Invalid IMDb ID: {use_imdb_id}")
                err += 1
                continue

            title = match.title or match.original_title
            year = match.year or '0000'

            try:
                # Determine ID format: imdbid- for IMDB (tt...), tmdbid- for TMDb (numbers)
                if use_imdb_id:
                    if use_imdb_id.startswith('tt'):
                        id_str = f"[imdbid-{use_imdb_id}]"
                    else:
                        # TMDb fallback
                        id_str = f"[tmdbid-{match.tmdb_id}]" if match.tmdb_id else ""
                    new_name = self._sanitize(f"{title} ({year}) {id_str}")
                else:
                    new_name = self._sanitize(f"{title} ({year})")
            except RenameError as e:
                print(f"\n  ❌ {result.folder_name}: {e}")
                err += 1
                continue

            result.new_name = new_name

            print(f"\n  {'📋' if dry_run else '⚡'} {result.folder_name}")
            print(f"     → {new_name}")
            print(f"     🆔 IMDb: {match.imdb_id} | TMDb: {match.tmdb_id}")

            is_series_result = result.detected_type == MediaType.SERIES or match.media_type == 'tv'

            if is_series_result:
                stats = series_stats.setdefault(new_name, {
                    'folders': 0,
                    'seasons': set(),
                    'imdb_id': match.imdb_id or '',
                })
                stats['folders'] = int(stats['folders']) + 1  # type: ignore[union-attr]
                if result.season_number:
                    cast_seasons = stats['seasons']
                    if isinstance(cast_seasons, set):
                        cast_seasons.add(result.season_number)

            if dry_run:
                ok += 1
                continue

            try:
                if result.path.is_dir():
                    new_dir = result.path.parent / new_name
                    is_series = is_series_result

                    if not self._same_fs(result.path, new_dir):
                        raise RenameError("Cross-Filesystem")

                    if result.path.name != new_name:
                        if new_dir.exists():
                            if not is_series:
                                raise RenameError("Target already exists")

                            print(f"     🔀 SERIES MERGE: source '{result.path.name}' -> existing '{new_dir.name}'")

                            import_name = result.path.name
                            import_dir = new_dir / import_name
                            if import_dir.exists():
                                import_dir = new_dir / f"{import_name}.{int(time.time() * 1000)}"

                            self._do_rename(result.path, import_dir)
                            working = new_dir
                        else:
                            if is_series:
                                print(f"     🆕 SERIES ROOT: create '{new_dir.name}' from '{result.path.name}'")
                            temp = result.path.parent / f".tmp_{int(time.time() * 1000)}_{match.tmdb_id}"
                            self._do_rename(result.path, temp)
                            self._do_rename(temp, new_dir)
                            working = new_dir
                    else:
                        working = result.path

                    if is_series:
                        moved, seasons = self._organize_series_files(working, fallback_season=result.season_number)
                        if new_name in series_stats:
                            cast_seasons = series_stats[new_name]['seasons']
                            if isinstance(cast_seasons, set):
                                cast_seasons.update(seasons)
                        print(f"     ✅ Series folder renamed + {moved} files in {len(seasons)} season folder(s)")
                        self._commit()
                        result.status = MatchStatus.RENAMED
                        ok += 1
                        continue

                    renamed = 0
                    skipped = 0
                    
                    # Get all video files and sort by size (largest first)
                    video_files = [f for f in working.iterdir() if f.is_file() and f.suffix.lower() in self.VIDEO_EXT]
                    video_files.sort(key=lambda x: x.stat().st_size, reverse=True)
                    
                    # Handle empty video files case
                    if not video_files:
                        print(f"     ⚠️  No video files found in {working.name}")
                        continue
                    
                    # Identify the main movie (largest video file)
                    main_movie = video_files[0]
                    
                    # First pass: handle non-video files and main movie
                    for item in working.iterdir():
                        if not item.is_file():
                            continue
                        ext = item.suffix.lower()
                        
                        # Skip video files in first pass (handled separately)
                        if ext in self.VIDEO_EXT:
                            continue
                        
                        new_fn = None
                        if ext in self.RENAME_EXT:
                            new_fn = f"{new_name}{ext}"
                        elif ext in self.SUB_EXT:
                            new_fn = f"{new_name}{self._sub_lang(item.name)}{ext}"
                        
                        if new_fn and item.name != new_fn:
                            target = working / new_fn
                            if not target.exists():
                                self._do_rename(item, target)
                                renamed += 1
                            else:
                                skipped += 1
                    
                    # Second pass: handle video files - main movie gets priority
                    for item in video_files:
                        ext = item.suffix.lower()
                        is_main_movie = (item == main_movie)
                        
                        new_fn = f"{new_name}{ext}"
                        
                        if item.name == new_fn:
                            # File already has correct name
                            if not is_main_movie and main_movie:
                                # Smaller video file has the correct name - this is wrong!
                                try:
                                    size_mb = item.stat().st_size / MB
                                    main_size_mb = main_movie.stat().st_size / MB
                                    print(f"     ⚠️  Wrong file has target name: {item.name} ({size_mb:.1f}MB) < main movie ({main_size_mb:.1f}MB)")
                                    print(f"        Main movie: {main_movie.name}")
                                except OSError:
                                    pass  # File may have been deleted, skip warning
                            continue
                        
                        target = working / new_fn
                        if not target.exists():
                            if not is_main_movie:
                                # Warn about smaller video files
                                try:
                                    size_mb = item.stat().st_size / MB
                                    main_size_mb = main_movie.stat().st_size / MB if main_movie else 0
                                    print(f"     ⚠️  Smaller video file: {item.name} ({size_mb:.1f}MB) vs main ({main_size_mb:.1f}MB)")
                                except OSError:
                                    pass  # File may have been deleted, skip warning
                            self._do_rename(item, target)
                            renamed += 1
                        else:
                            # Target exists - check if we should swap
                            if is_main_movie:
                                # Main movie should get the correct name - swap with smaller file
                                smaller_file = target
                                main_file = item
                                main_size = main_file.stat().st_size
                                smaller_size = smaller_file.stat().st_size
                                
                                # Validate: warn if sizes are too close (main might not be main)
                                size_ratio = main_size / smaller_size if smaller_size > 0 and main_size > 0 else 0
                                if size_ratio > 0 and size_ratio < MAIN_MOVIE_SIZE_RATIO:
                                    print(f"     ⚠️  Size difference small ({main_size/MB:.1f}MB vs {smaller_size/MB:.1f}MB) - may not be main movie")
                                
                                # Swap: rename smaller to temp, main to target, smaller to main's old name
                                # Use smaller file's extension for temp name
                                temp_ext = smaller_file.suffix
                                temp_name = working / f".tmp_swap_{int(time.time() * 1000)}{temp_ext}"
                                
                                # Atomic swap with rollback on failure
                                swap_success = False
                                try:
                                    self._do_rename(smaller_file, temp_name)
                                    self._do_rename(main_file, target)
                                    self._do_rename(temp_name, working / main_file.name)
                                    swap_success = True
                                except Exception as e:
                                    # Rollback: try to restore original state
                                    print(f"     ❌ Swap failed: {e}")
                                    if temp_name.exists():
                                        # Temp was created, try to restore smaller_file
                                        try:
                                            os.rename(temp_name, smaller_file)
                                        except Exception:
                                            pass  # Rollback failed, log for manual recovery
                                    if target.exists() and not main_file.exists():
                                        # main_file was already renamed to target, try to restore
                                        try:
                                            os.rename(target, main_file)
                                        except Exception:
                                            pass  # Rollback failed, log for manual recovery
                                    raise RenameError(f"Swap failed and rollback attempted: {e}")
                                
                                if swap_success:
                                    print(f"     🔄 Swapped: {main_file.name} ({main_size/MB:.1f}MB) -> {target.name}")
                                    print(f"        {smaller_file.name} ({smaller_size/MB:.1f}MB) -> {main_file.name}")
                                    renamed += 2  # Two files were swapped
                                    self._commit()
                                    result.status = MatchStatus.RENAMED
                            else:
                                skipped += 1

                    print(f"     ✅ Folder + {renamed} files renamed" + (f" (skipped {skipped} duplicates)" if skipped else ""))
                    ok += 1

                else:
                    new_dir = result.path.parent / new_name

                    if new_dir.exists():
                        raise RenameError("Target folder already exists")

                    new_dir.mkdir()
                    new_file = new_dir / f"{new_name}{result.path.suffix}"
                    self._do_rename(result.path, new_file)
                    print(f"     ✅ Folder created + file moved")

                self._commit()
                result.status = MatchStatus.RENAMED
                ok += 1

            except RenameError as e:
                print(f"     ❌ {e}")
                self._rollback()
                err += 1
            except Exception as e:
                print(f"     ❌ {e}")
                self._rollback()
                err += 1

        if series_stats:
            print(f"\n{'─' * 80}")
            print("  📺 SERIES SUMMARY")
            print(f"{'─' * 80}")
            sorted_items = sorted(
                series_stats.items(),
                key=lambda item: (
                    1 if not str(item[1].get('imdb_id', '')) else 0,
                    str(item[1].get('imdb_id', '')),
                    item[0].lower(),
                ),
            )

            for series_name, stats in sorted_items:
                folders = int(stats['folders'])  # type: ignore[arg-type]
                seasons_obj = stats.get('seasons', set())
                seasons = seasons_obj if isinstance(seasons_obj, set) else set()
                season_text = self._format_season_summary(seasons)
                action = "planned from" if dry_run else "merged"
                print(f"  • {series_name}: {action} {folders} source folder(s) -> {season_text}")

        return ok, skip, err


def print_token_help():
        """Displays help for token configuration."""
        print("""
    ❌ TMDb access token not found!

    How to get a token:
        1. Register at https://www.themoviedb.org/
        2. Go to: Settings → API → API Read Access Token (v4 auth)
        3. Copy the token (starts with "eyJ...")

    How to configure the token:

        Option 1: Environment variable (recommended)
            export TMDB_ACCESS_TOKEN="eyJ..."
      
            Add to ~/.bashrc or ~/.zshrc for permanent use.

        Option 2: File ~/.tmdb_token
            echo "eyJ..." > ~/.tmdb_token
            chmod 600 ~/.tmdb_token

        Option 3: Command line
            python3 rename.py /path -t "eyJ..."

""")


def main():
    import argparse

    p = argparse.ArgumentParser(
        description='Media Renamer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s /movies                   Interactive (Dry-Run)
  %(prog)s /movies -x                Interactive (Execute)
  %(prog)s /movies -n                Auto-run (AUTO only)

Environment variables:
  TMDB_ACCESS_TOKEN    TMDb API Read Access Token (v4)
'''
    )
    p.add_argument('paths', nargs='+', help='Paths')
    p.add_argument('-x', '--execute', action='store_true', help='Perform the rename')
    p.add_argument('-s', '--single', action='store_true', help='Treat each path as an individual folder')
    p.add_argument('-n', '--no-interactive', action='store_true', help='Run non-interactively')
    p.add_argument('--debug-series', action='store_true', help='Show detailed season inference for series files')
    p.add_argument('--no-series-batch', action='store_true', help='Disable grouping of multiple season folders per series')
    p.add_argument('-t', '--token', help='TMDb Access Token')

    args = p.parse_args()

    # Determine token: CLI > environment > file
    token = args.token or get_tmdb_token()

    # Token validation
    if not token:
        print_token_help()
        sys.exit(1)

    if not check_token(token):
        print(f"""
  ❌ Invalid token format!

  Expected: JWT (eyJ...) or API Key v3 (32 hex digits)
  Received: {token[:20]}...

  Make sure you are using the "API Read Access Token (v4 auth)",
  not the "API Key (v3 auth)".
""")
        sys.exit(1)

    # Collect directories
    dirs: list[Path] = []
    for ps in args.paths:
        path = Path(ps).resolve()
        if not path.exists():
            print(f"⚠️  Not found: {ps}")
            continue
        if not path.is_dir():
            print(f"⚠️  Not a directory: {ps}")
            continue

        if args.single:
            dirs.append(path)
        else:
            dirs.extend(d for d in path.iterdir() if d.is_dir() and not d.name.startswith('.'))

    if not dirs:
        print("❌ No directories!")
        sys.exit(1)

    dirs = sorted(set(dirs))
    renamer = MediaRenamer(
        token,
        interactive=not args.no_interactive,
        debug_series=args.debug_series,
        series_batch_mode=not args.no_series_batch,
    )

    # Test API connection
    print(f"\n  🔑 Checking API connection...", end=" ")
    if not renamer.verify_api_connection():
        print("❌")
        print("""
  ❌ API connection failed!

  Possible causes:
    - Token invalid or expired
    - No internet connection
    - TMDb API unreachable

  Check your token at https://www.themoviedb.org/settings/api
""")
        sys.exit(1)
    print("✓")

    print(f"\n{'═' * 80}")
    print(f"  🎬 MEDIA RENAMER")
    print(f"{'═' * 80}")

    results = renamer.scan_all(dirs)
    renamer.show_scan_results(results)

    if not args.no_interactive:
        results = renamer.interactive_review(results, dry_run=not args.execute)
    else:
        ok, skip, err = renamer.execute_renames(results, dry_run=not args.execute)
        print(f"\n  ✅ {ok}  ⏭️ {skip}  ❌ {err}")

    if not args.execute:
        ready = sum(1 for r in results if r.status in (MatchStatus.AUTO, MatchStatus.MANUAL) and r.selected_match)
        if ready:
            print(f"\n  💡 Use -x to rename ({ready} ready)")

    print()


if __name__ == "__main__":
    main()