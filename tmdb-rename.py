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
import urllib.parse
import urllib.request
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path

# ========== CONFIGURATION ==========

MAX_PATH_LENGTH = 250
MIN_VIDEO_SIZE_MB = 100

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
        r'\bDDP?5[\.\s]?1\b',
        r'\bAC3\b',
        r'\bEAC3\b',
        r'\bAAC\b',
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
        r'\bDVDRip\b',
        r'\bDVD\b',
        r'\bHDTV\b',
        r'\bPDTV\b',
        r'\bx264\b',
        r'\bx265\b',
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

    VIDEO_EXT = frozenset({'.mkv', '.mp4', '.avi', '.m4v', '.wmv', '.mov', '.ts', '.m2ts'})
    SUB_EXT = frozenset({'.srt', '.sub', '.ass', '.ssa', '.vtt', '.idx', '.sup'})
    RENAME_EXT = frozenset({'.mkv', '.mp4', '.avi', '.m4v', '.nfo'})

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

    TMDB_BASE = "https://api.themoviedb.org/3"

    def __init__(self, access_token: str, interactive: bool = True):
        self.access_token = access_token
        self.interactive = interactive
        self._ops: list[RenameOp] = []
        self._cache: dict[str, any] = {}

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
            msg = {errno.EXDEV: "Cross-device", errno.EACCES: "Access denied",
                   errno.EPERM: "Permission denied"}.get(e.errno, f"OS error {e.errno}")
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
        clean = name

        for ext in self.VIDEO_EXT:
            if clean.lower().endswith(ext):
                clean = clean[:-len(ext)]
                break

        clean = clean.replace('.', ' ').replace('_', ' ')

        for pat in [r'[Ss]\d{1,2}[Ee]\d{1,3}', r'\b[Ss]\d{1,2}\b', r'\d{1,2}[xX]\d{1,3}',
                    r'[Ss]eason\s?\d{1,2}', r'[Ss]taffel\s?\d{1,2}']:
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
        result = text
        for ascii_v, umlaut in self.UMLAUTS.items():
            if ascii_v == 'ue':
                result = re.sub(r'(?<=[bcdfghjklmnpqrstvwxzBCDFGHJKLMNPQRSTVWXZ])ue', umlaut, result)
            else:
                result = result.replace(ascii_v, umlaut)
        return result

    # ==================== DETECTION ====================

    def _find_videos(self, directory: Path, max_depth: int = 5) -> list[VideoFile]:
        videos: list[VideoFile] = []

        def scan(d: Path, depth: int = 0, parent_is_root: bool = True):
            if depth > max_depth or d.name.lower() in self.IGNORE_DIRS:
                return
            try:
                for item in d.iterdir():
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
        for pat in self.EP_PATTERNS:
            if m := re.search(pat, name, re.IGNORECASE):
                try:
                    return EpisodeInfo(season=int(m[1]), episode=int(m[2]))
                except (ValueError, IndexError):
                    pass
        return None

    def _is_collection(self, folder_name: str, videos: list[VideoFile]) -> bool:
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
        videos = self._find_videos(directory)
        if not videos:
            return MediaType.UNKNOWN, videos

        if self._is_collection(directory.name, videos):
            return MediaType.COLLECTION, videos

        eps = [v for v in videos if v.media_type == MediaType.SERIES]
        if len(eps) >= 2:
            return MediaType.SERIES, videos

        movies = [v for v in videos if v.media_type == MediaType.MOVIE]
        if movies and movies[0].size_gb > 1:
            return MediaType.MOVIE, videos

        if re.search(r's\d{1,2}|season|staffel', directory.name, re.I):
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

        title_umlaut = self._to_umlauts(title)
        if title_umlaut != title:
            variants.append((title_umlaut, year))
            variants.append((title_umlaut, None))

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

    def _manual_lookup(self, manual: str) -> MediaMatch | None:
        if not manual:
            return None

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

        if manual.isdigit():
            tmdb_id = int(manual)

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

    # ==================== SCANNING ====================

    def scan_folder(self, folder: Path) -> ScanResult:
        folder_name = folder.name

        if re.match(r'^.+\s\(\d{4}\)\s\[imdbid-tt\d{7,}\]$', folder_name):
            return ScanResult(
                path=folder, folder_name=folder_name,
                detected_type=MediaType.MOVIE,
                extracted_title=None, extracted_year=None,
                videos=[], matches=[],
                status=MatchStatus.DONE
            )

        detected_type, videos = self._detect_type(folder)
        title, year = self._extract_title_year(folder_name)

        result = ScanResult(
            path=folder, folder_name=folder_name,
            detected_type=detected_type,
            extracted_title=title, extracted_year=year,
            videos=videos, matches=[]
        )

        if not videos:
            result.status = MatchStatus.NONE
            result.error = "No videos"
            return result

        if detected_type == MediaType.COLLECTION:
            result.status = MatchStatus.MANUAL
            result.error = f"Collection ({len(videos)} films)"
            result.collection_items = self._prepare_collection_items(videos)
            return result

        if not title:
            result.status = MatchStatus.NONE
            result.error = "No title"
            return result

        matches = self._search_tmdb(title, year, detected_type)
        result.matches = matches

        if not matches:
            result.status = MatchStatus.NONE
            result.error = "No matches"
        elif len(matches) == 1 and year and matches[0].year == year:
            result.status = MatchStatus.AUTO
            result.selected_match = matches[0]
        else:
            result.status = MatchStatus.UNSURE

        return result

    def _prepare_collection_items(self, videos: list[VideoFile]) -> list[CollectionItem]:
        items: list[CollectionItem] = []

        for v in videos:
            title = v.extracted_title
            year = v.extracted_year

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

    def scan_all(self, folders: list[Path], show_progress: bool = True) -> list[ScanResult]:
        results: list[ScanResult] = []

        if show_progress:
            print(f"\n  🔍 Scanning {len(folders)} folders...")

        for i, folder in enumerate(folders):
            if show_progress:
                pct = (i + 1) / len(folders) * 100
                print(f"  [{i+1}/{len(folders)}] {pct:.0f}% {folder.name[:40]}...", end="\r")

            result = self.scan_folder(folder)
            results.append(result)

        if show_progress:
            print(" " * 80, end="\r")

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
            choice = input("  Choice: ").strip().lower()

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
                print(f"\n  Matches:")
                for j, m in enumerate(item.matches[:6], 1):
                    marker = "✓" if item.extracted_year and m.year == item.extracted_year else " "
                    imdb_info = f" [{m.imdb_id}]" if m.imdb_id else ""
                    print(f"    {marker} {j}. {m.title} ({m.year}){imdb_info}")

                print(f"\n    0 = Skip")
                print(f"    m = Enter manual ID")

                while True:
                    sel = input(f"\n  Choice [1]: ").strip().lower()

                    if sel == '0':
                        item.status = MatchStatus.SKIP
                        break

                    if sel == 'm':
                        manual = input("  ID (tt.../TMDb): ").strip()
                        match = self._manual_lookup(manual)
                        if match:
                            item.selected_match = match
                            item.status = MatchStatus.MANUAL
                            print(f"  ✓ {match.title} ({match.year}) [{match.imdb_id}]")
                        else:
                            print("  ❌ Not found")
                        break

                    try:
                        num = int(sel) if sel else 1
                        if 1 <= num <= len(item.matches):
                            item.selected_match = item.matches[num - 1]
                            item.status = MatchStatus.AUTO
                            break
                    except ValueError:
                        pass
                    print("  ❌ Invalid")
            else:
                print(f"\n  No matches. m = Manual, 0 = Skip")
                sel = input("  Choice: ").strip().lower()

                if sel == 'm':
                    manual = input("  ID: ").strip()
                    match = self._manual_lookup(manual)
                    if match:
                        item.selected_match = match
                        item.status = MatchStatus.MANUAL
                        print(f"  ✓ {match.title} ({match.year}) [{match.imdb_id}]")
                    else:
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
            status_icon = r.status.value
            type_icons = {
                MediaType.MOVIE: "🎬", MediaType.SERIES: "📺",
                MediaType.COLLECTION: "📦", MediaType.UNKNOWN: "❓"
            }
            type_icon = type_icons.get(r.detected_type, "?")

            folder_short = r.folder_name[:33] + ".." if len(r.folder_name) > 35 else r.folder_name

            if r.selected_match:
                match_str = f"{r.selected_match.title[:20]} ({r.selected_match.year})"
            elif r.status == MatchStatus.RENAMED:
                match_str = "✔ Renamed"
            elif r.error:
                match_str = f"[{r.error[:20]}]"
            elif r.matches:
                match_str = f"[{len(r.matches)} matches]"
            else:
                match_str = "-"

            print(f"  {i:>3}  {status_icon:>2}  {type_icon:>3}  {folder_short:<35}  {match_str:<25}")

        print(f"{'─' * 80}")

    def interactive_review(self, results: list[ScanResult], dry_run: bool = True) -> list[ScanResult]:

        while True:
            to_review = [i for i, r in enumerate(results)
                         if r.status in (MatchStatus.UNSURE, MatchStatus.NONE, MatchStatus.MANUAL)
                         and r.status != MatchStatus.RENAMED]
            ready = [i for i, r in enumerate(results)
                     if r.status == MatchStatus.AUTO and r.selected_match]
            done = [i for i, r in enumerate(results)
                    if r.status in (MatchStatus.RENAMED, MatchStatus.DONE)]

            print(f"\n{'═' * 80}")
            print(f"  🔧 INTERACTIVE REVIEW")
            print(f"{'═' * 80}")
            print(f"""
  Status:
    ✓ Ready: {len(ready)}   ? To review: {len(to_review)}   ✔ Done: {len(done)}

  Commands:
    <Enter>  Handle uncertain items ({len(to_review)})
    x        Rename {len(ready)} now {'[DRY RUN]' if dry_run else '[EXECUTE]'}
    1,3,5    Specific numbers
    a        Review all
    l        Show list
    q        Quit
            """)

            choice = input("  Choice: ").strip().lower()

            if choice == 'q':
                return results

            if choice == 'l':
                self.show_scan_results(results)
                continue

            if choice == 'x':
                ok, skip, err = self.execute_renames(results, dry_run=dry_run)
                print(f"\n  Result: ✅ {ok}  ⏭️ {skip}  ❌ {err}")

                for r in results:
                    if r.status == MatchStatus.AUTO and r.selected_match and not dry_run:
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

                if result.matches:
                    print(f"\n  Matches:")
                    for j, m in enumerate(result.matches[:8], 1):
                        year_match = "✓" if result.extracted_year and m.year == result.extracted_year else " "
                        imdb_info = f" [{m.imdb_id}]" if m.imdb_id else ""
                        print(f"    {year_match} {j}. {m.title} ({m.year}){imdb_info}")

                    print(f"\n    0 = Skip | m = Manual")

                    while True:
                        sel = input(f"\n  Choice [1]: ").strip().lower()

                        if sel == '0':
                            result.status = MatchStatus.SKIP
                            break

                        if sel == 'm':
                            manual = input("  ID: ").strip()
                            match = self._manual_lookup(manual)
                            if match:
                                result.selected_match = match
                                result.status = MatchStatus.MANUAL
                                print(f"  ✓ {match.title} ({match.year}) [{match.imdb_id}]")
                            else:
                                print("  ❌ Not found")
                            break

                        try:
                            num = int(sel) if sel else 1
                            if 1 <= num <= len(result.matches):
                                result.selected_match = result.matches[num - 1]
                                result.status = MatchStatus.AUTO
                                break
                        except ValueError:
                            pass
                        print("  ❌ Invalid")

                else:
                    print(f"\n  No matches. m = Manual | 0 = Skip")
                    sel = input("  Choice: ").strip().lower()

                    if sel == 'm':
                        manual = input("  ID: ").strip()
                        match = self._manual_lookup(manual)
                        if match:
                            result.selected_match = match
                            result.status = MatchStatus.MANUAL
                        else:
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

    def execute_renames(self, results: list[ScanResult], dry_run: bool = True) -> tuple[int, int, int]:
        ok, skip, err = 0, 0, 0

        to_rename = [r for r in results
                     if r.status in (MatchStatus.AUTO, MatchStatus.MANUAL)
                     and r.selected_match
                     and r.status != MatchStatus.RENAMED]

        if not to_rename:
            return 0, len(results), 0

        print(f"\n{'═' * 80}")
        print(f"  {'📋 DRY RUN' if dry_run else '⚡ RENAMING'} - {len(to_rename)} folders")
        print(f"{'═' * 80}")

        for result in to_rename:
            self._ops.clear()

            match = result.selected_match
            if not match:
                skip += 1
                continue

            if not match.imdb_id:
                print(f"\n  🔍 Fetching IMDb ID for: {match.title} (TMDb: {match.tmdb_id})")
                match.imdb_id = self._get_imdb_id(match.tmdb_id, match.media_type)

            if not match.imdb_id:
                print(f"\n  ❌ {result.folder_name}")
                print(f"     No IMDb ID found (TMDb: {match.tmdb_id}, type: {match.media_type})")
                err += 1
                continue

            if not re.match(r'^tt\d{7,}$', match.imdb_id):
                print(f"\n  ❌ {result.folder_name}")
                print(f"     Invalid IMDb ID: {match.imdb_id}")
                err += 1
                continue

            title = match.title or match.original_title
            year = match.year or '0000'

            try:
                new_name = self._sanitize(f"{title} ({year}) [imdbid-{match.imdb_id}]")
            except RenameError as e:
                print(f"\n  ❌ {result.folder_name}: {e}")
                err += 1
                continue

            result.new_name = new_name

            print(f"\n  {'📋' if dry_run else '⚡'} {result.folder_name}")
            print(f"     → {new_name}")
            print(f"     🆔 IMDb: {match.imdb_id} | TMDb: {match.tmdb_id}")

            if dry_run:
                ok += 1
                continue

            try:
                if result.path.is_dir():
                    new_dir = result.path.parent / new_name

                    if not self._same_fs(result.path, new_dir):
                        raise RenameError("Cross-Filesystem")

                    if result.path.name != new_name:
                        if new_dir.exists():
                            raise RenameError("Target already exists")

                        temp = result.path.parent / f".tmp_{int(time.time() * 1000)}_{match.tmdb_id}"
                        self._do_rename(result.path, temp)
                        self._do_rename(temp, new_dir)
                        working = new_dir
                    else:
                        working = result.path

                    renamed = 0
                    for item in list(working.iterdir()):
                        if not item.is_file():
                            continue
                        ext = item.suffix.lower()
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

                    print(f"     ✅ Folder + {renamed} files renamed")

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
    renamer = MediaRenamer(token, interactive=not args.no_interactive)

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
        ready = sum(1 for r in results if r.status == MatchStatus.AUTO and r.selected_match)
        if ready:
            print(f"\n  💡 Use -x to rename ({ready} ready)")

    print()


if __name__ == "__main__":
    main()