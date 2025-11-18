## Companies House Lookup Tool

**Companies House Lookup** is a desktop tool for bulk, high‑accuracy lookups against the UK Companies House API from Excel spreadsheets (for example, debtor lists from credit control systems).

### Key features

- **Bulk spreadsheet input**: point the tool at any `.xlsx` file, pick the name column and row range.
- **Smart matching engine**:
  - Fuzzy name matching and multiple search variants (handles `TRADING AS`, abbreviations, truncated names).
  - Uses address and postcode as additional signals to separate similar companies.
  - Detects non‑company rows (people, schools, etc.) to avoid bogus matches.
- **Rich Companies House data**:
  - Core company profile (status, dates, registered office).
  - Officers and disqualified officers.
  - Insolvency and filing history (including proposals to strike off).
- **Insightful Excel output**:
  - Adds matched company, address, insolvency status, directors and accuracy columns.
  - Colour‑codes rows (green for matched/active, red for insolvent/flagged).
  - Creates a `Summary` sheet with insolvent vs active pie charts by count and value.
- **Responsive GUI**:
  - Progress bar, ETA, pause/resume and stop controls.
  - Detachable log window, optional verbose debug logging.
  - Remembers your last spreadsheet, output path, columns profile and window layout.
- **CLI mode for automation**: run the same matching engine from scripts, scheduled tasks or servers.

Companies using this tool can reduce manual Companies House lookups, standardise insolvency screening and keep a clear audit trail of how each match was made.

---

### 1. Installation

From the project directory (the folder containing `gus_trace_tool.py` and `requirements.txt`):

```bash
python -m venv .venv
.venv\Scripts\activate  # on Windows
pip install -r requirements.txt
```

Tkinter is part of the standard Python installation on Windows; if it is missing, install a standard python.org build of Python.

---

### 2. API Key Configuration (Important)

You must use your own Companies House API key. **Never commit your real key to GitHub.**

There are two supported ways to provide the key:

- **Environment variable**:

  ```bash
  set GUS_API_KEY=your_real_api_key_here  # PowerShell: $env:GUS_API_KEY="..."
  ```

- **Config file** (local only, ignored by git because of `.gitignore`):

  Create a file called `config.ini` in this folder:

  ```ini
  [API]
  GUS_API_KEY = your_real_api_key_here
  ```

If both are present, the environment variable wins. The GUI also lets you paste any key directly into the field for ad‑hoc use.

---

### 3. Running the GUI

From the project directory:

```bash
python gus_trace_tool.py
```

In the GUI you can:

- Browse for an input `.xlsx`.
- Choose start/end rows and the name column.
- Choose output columns and save/load column profiles.
- Choose whether to use cache files (for faster repeat runs).
- Start processing and watch the log, progress bar, and estimated time.

The result Excel file is written to the output path you choose (default `insolvency_results.xlsx`).

---

### 4. Command‑Line Mode (CLI)

For automated runs (e.g. scheduled tasks), you can run without the GUI.

Example:

```bash
python gus_trace_tool.py --cli ^
  --input EE.xlsx ^
  --output insolvency_results.xlsx ^
  --name-column "Debtor Name" ^
  --start-row 1 ^
  --end-row 1000 ^
  --use-cache
```

Key arguments:

- `--cli` – required to activate CLI mode.
- `--input` – input Excel file path.
- `--output` – output Excel file path.
- `--name-column` – column header containing the company names.
- `--start-row` / `--end-row` – 1‑based inclusive row range.
- `--api-key` – optional; if omitted, the tool uses `GUS_API_KEY` from env/config.
- `--use-cache` / `--no-cache` – whether to re‑use cached API responses.
- `--columns` – optional repeated flag to specify output columns in order; if omitted, all available columns are written.

The CLI uses the same matching logic and output formatting as the GUI.

---

### 5. Cache behaviour

- **GUI mode**:
  - Each time you start the app, all Companies House lookups begin **fresh** – no old cache files are loaded.
  - While the app stays open, if **Use Cache** is ticked, results are cached **in memory only** to avoid repeat calls during that session.
  - If **Use Cache** is unticked, in‑memory caches are cleared at the start of each run so every lookup hits the live API.

- **CLI mode (`--cli`)**:
  - When `--use-cache` is specified, the tool loads and saves `*.pickle` cache files so repeated runs can reuse previous responses.
  - Use `--no-cache` if you always want fresh results in CLI as well.

This design keeps day‑to‑day GUI usage always fresh per launch, while still allowing cache reuse in automated/CLI scenarios when desired.

---

### 6. Packaging as a Windows EXE

You can use **PyInstaller** to build a single‑file executable for easier distribution:

```bash
pip install pyinstaller
pyinstaller --noconsole --onefile gus_trace_tool.py
```

This will create `dist\gus_trace_tool.exe`. You can ship that EXE along with a `config.ini` and your users’ Excel files. Remember: never bundle a real API key inside the executable for public distribution.

---

### 7. GitHub Repository

The project can be pushed to a GitHub repository such as `https://github.com/Gusmack1/CHC.git`. The included `.gitignore` ensures that:

- Local cache files (`*.pickle`), Excel files, and `config.ini` are **not committed**.
- Only the actual source code and project configuration go into version control.


