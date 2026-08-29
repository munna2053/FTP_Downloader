# FTP/HTTP Downloader

Private-network web app for downloading folders from FTP servers or HTTP directory listings.

## Run

```powershell
python app.py --host 0.0.0.0 --port 8080
```

Open:

- This PC: `http://localhost:8080`
- LAN: `http://<this-computer-ip>:8080`

## Docker Compose

Build and start:

```powershell
docker compose up -d --build
```

Open:

- This PC: `http://localhost:8080`
- LAN: `http://<docker-host-ip>:8080`

Downloaded files are stored on the host in `./downloads` and mounted into the container at `/app/downloads`.
Download status history is stored on the host in `./database/downloader.sqlite` and mounted into the container at `/data/downloader.sqlite`.

Stop:

```powershell
docker compose down
```

## Source Types

- `FTP`: Uses host, port, username, password, passive mode, and optional FTPS/TLS.
- `HTTP` / `HTTPS`: Uses a plain web directory listing such as `http://172.16.50.4/`. Login fields are ignored.
- Encoded URLs are supported. You can paste a full folder URL into `Source folder`, or enter the host and folder separately.

## Download Options

- Recursively downloads all child folders.
- Preserves the source folder structure under the selected save folder.
- Limits files per folder when `Files per folder` is greater than `0`.
- Runs up to `8` parallel file downloads.
- Shows per-file progress and overall job status.
- Saves download status in SQLite so history survives restarts.
- Shows saved downloads in a paginated table when the page loads.
- Runs downloads in server-side background threads; the browser can be closed while the server keeps working.
- Retries failed file transfers up to 3 times before marking the file as failed.
- Marks unfinished downloads as interrupted if the server/container restarts before they finish.
- Defaults to media file extensions. Choose `Everything` or custom extensions to download other files.
