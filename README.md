# Win-It-Inline

This project packages the CDT and GTO EPD analysis flow into a small Python application with a reusable module layout and a command-line entry point.

## Project layout

```
.
|-- main.py
|-- pyproject.toml
|-- requirements.txt
|-- requirements-dev.txt
|-- src/
|   `-- win_it_inline/
|       |-- __init__.py
|       |-- cli.py
|       |-- pipeline.py
|       `-- settings.py
`-- tests/
    `-- test_pipeline.py
```

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

If `pip install -r requirements-dev.txt` fails on `PyUber`, install public packages first and then install `PyUber` from your internal package source:

```powershell
pip install numpy pandas pytest
# then install PyUber from your internal index/wheel source
```

## Run

```powershell
python main.py
```

Optional arguments:

```powershell
python main.py --output reports\final_with_row_20_200.csv --datasource F28_PROD_XEUS --lookback-days 30
```

The script writes the final CSV to the selected output path. By default it writes `final_with_row_20_200.csv` in the repository root.