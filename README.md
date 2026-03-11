# TMDb Rename Tool

Command-line helper for renaming movie and TV folders using TMDb/IMDb metadata.

Standard target format:

- Movies: `Title (Year) [imdbid-tt######]`
- Series root: `Series Title (Year) [imdbid-tt######]`
- Series seasons inside root: `Season 01`, `Season 02`, ...

## Requirements

- Python 3.10+
- TMDb API Read Access Token (v4 auth)

## Setup

1. Get your TMDb token: <https://www.themoviedb.org/settings/api>
2. Configure token with one of these methods:

```sh
export TMDB_ACCESS_TOKEN="eyJ..."
```

or store it in `~/.tmdb_token`.

## Usage

```sh
python3 tmdb-rename.py /path/to/library
```

Useful options:

- `-x`, `--execute`  Perform real renaming (default is dry-run preview)
- `-n`, `--no-interactive`  No prompts, process automatically matched entries
- `-s`, `--single`  Treat each input path as one folder (no child-folder scan)
- `-t`, `--token`  Override token for this run
- `--debug-series`  Show detailed season inference/move decisions per file
- `--no-series-batch`  Disable grouping of multiple season folders of same series

## Series Handling

- Season/episode patterns like `S01E02`, `1x02`, `E02`, `EP02`, `Folge 02`, `Episode 02` are recognized.
- Episode files are organized into `Season XX` folders under one series root.
- Existing episode filenames are preserved (no forced renaming for episode files).
- When multiple source folders (e.g. `S01`, `S02`, `S03`) match the same series, they are merged into one series root.
- Scene leftovers are cleaned during series import (e.g. `sample` video files, `.sfv`, `.par2`, checksum sidecar files).
- Console output includes:
   - `SERIES ROOT` when a new series root is created
   - `SERIES MERGE` when another source folder is merged into an existing root
   - `SERIES SUMMARY` at the end (sorted by IMDb ID)

## Manual Input

When no match is found or you want to override, press `m` or `x`. You can enter:

- **IMDb ID**: `tt0120188` or `imdb:tt0120188`
- **TMDb ID**: `12345` or `tmdb:12345`
- **Title**: `Movie Title` (triggers TMDb search)

Manual mappings are saved to `~/.tmdb_manual_mappings.json` and automatically reused on future scans.

## Detection Notes

- Audiobook/book-like folders are skipped.
- Video files smaller than 100 MB are ignored.
- Supported video/subtitle extensions are processed (`.mkv`, `.mp4`, `.avi`, `.m4v`, `.nfo`, `.srt`, ...).
- German umlauts (ü, ö, ä) are handled for both directions (ue → ü and ü → ue).
- Films without IMDb link in TMDb are supported (uses tmdb-XXXXX format).

## License

- MIT (see [LICENSE](LICENSE))
