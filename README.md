# Pipeline Ingestion OpenSky (CI/CD)

Skrip Python mengambil *state vectors* OpenSky untuk Asia Tenggara, lalu menyimpan hasilnya ke Azure Blob Storage. GitHub Actions menjalankan unit test dan scheduler tiap 6 jam.

## 1. Siapkan kredensial

- **OpenSky:** buat akun, lalu buat API client di halaman *Account* untuk mendapat `client_id` dan `client_secret`. Sejak 18 Maret 2026 basic auth tidak diterima, hanya OAuth2 client credentials.
- **Azure:** buat Storage Account + container `bronze`, lalu salin *connection string*-nya.

## 2. Jalankan lokal

```bash
pip install -r requirements.txt
python -m pytest -q
python ingest.py --snapshots 3 --interval 5 --min-rows 0 --no-upload
```

Perintah terakhir hanya untuk uji cepat dan tidak menyentuh Azure. Hasil JSONL ada di `data/raw/`.

## 3. Jalankan penuh (mode historis)

Mode historis mengambil timestamp 5 detik sekali dari rentang 1 jam terakhir, tanpa menunggu antar request. Mode ini **wajib** memakai kredensial OAuth OpenSky.

```bash
set OPENSKY_CLIENT_ID=...
set OPENSKY_CLIENT_SECRET=...
set AZURE_STORAGE_CONNECTION_STRING=...
python ingest.py --historical --snapshots 240 --min-rows 0 --container bronze
```

Satu snapshot Asia Tenggara menghasilkan sekitar 100-500 baris (tergantung jam). Dengan 240 snapshot per jadwal dan 4 jadwal per hari, total baris harian melewati 100k. Jika ingin satu file besar sekaligus, jalankan `--historical --snapshots 720` (batas 1 jam API) lalu lanjutkan dengan jadwal berikutnya.

Bbox Asia Tenggara: latitude -11 s/d 20, longitude 95 s/d 141. Luas ini termasuk kategori >400 sq derajat, jadi biaya 4 credit per request. Akun standar mendapat 4.000 credit/hari, cukup untuk sekitar 1.000 request/hari. Konfigurasi 240 snapshot x 4 jadwal = 960 request/hari = 3.840 credit/hari.

## 4. GitHub Actions

Tambahkan Repository Secrets:

- `OPENSKY_CLIENT_ID`
- `OPENSKY_CLIENT_SECRET`
- `AZURE_STORAGE_CONNECTION_STRING`
- `AZURE_STORAGE_CONTAINER` (opsional, default `bronze`)

Workflow `.github/workflows/ingest.yaml` berjalan pada:

- `push` / `pull_request` ke `main` -> job `ci` menjalankan `pytest`
- `cron: '0 */6 * * *'` -> 00:00, 06:00, 12:00, 18:00 UTC
- `workflow_dispatch` -> tombol *Run workflow* untuk uji manual

Job `ingest` tidak berjalan pada `pull_request` karena secrets tidak tersedia untuk PR dari fork.