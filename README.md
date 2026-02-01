# TMDb Rename Tool

A small command-line helper that scans movie and series folders, finds matches on TMDb, and renames each folder (plus video files) to the standardized format `Title (Year) [imdbid-tt######]`.

## Requirements

- Python 3.10 or newer
- A TMDb **API Read Access Token (v4 auth)**

## Setup

1. Obtain your TMDb token at https://www.themoviedb.org/settings/api.
2. Export it for the current shell session:
   ```sh
   export TMDB_ACCESS_TOKEN="eyJ..."
   ```
   Or store it in `~/.tmdb_token` and make sure the file is readable only by you.

## Usage

```sh
python3 tmdb-rename.py /path/to/collection
```

- Add `-x` to execute the renaming instead of just previewing it (dry run).
- Use `-n` to skip the interactive review and rename only automatically matched folders.
- Pass `-s` to treat each provided path as a standalone folder rather than scanning its subdirectories.
- Use `-t "token"` to override the environment/file token for a single run.

## Notes

- Video files smaller than 100 MB are ignored by default.
- The script only renames supported video and subtitle extensions (`.mkv`, `.mp4`, `.avi`, `.m4v`, `.nfo`, `.srt`, etc.).
- If you need to override automated matches, enter an IMDb or TMDb ID when prompted.
