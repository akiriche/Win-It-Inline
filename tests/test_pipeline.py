from datetime import datetime, timedelta

import pandas as pd

from win_it_inline.pipeline import build_report
from win_it_inline.pipeline import compute_persistent_flags
from win_it_inline.settings import DELTA_FLAG_COLUMN
from win_it_inline.settings import PERSISTENT_FLAG_COLUMN


def test_compute_persistent_flags_marks_only_long_enough_runs() -> None:
    flags = pd.Series([False, True, True, False, True, True, True])

    result = compute_persistent_flags(flags, min_points=3)

    assert result.tolist() == [False, False, False, False, True, True, True]


def test_build_report_returns_expected_columns() -> None:
    now = datetime(2026, 4, 23, 12, 0, 0)
    rows = 210
    change_times = [now - timedelta(hours=rows - index) for index in range(rows)]

    apc = pd.DataFrame(
        {
            "RC": ["CDT_A_CH1"] * rows,
            "GPC": ["1.0"] * rows,
            "ENTITY": ["CDT_A"] * rows,
            "OPERATION": ["174438"] * rows,
            "LOT": ["LOT1234"] * rows,
            "LOT7": ["LOT1234"] * rows,
            "Cycles": ["5"] * rows,
            "CHANGE_TIME": change_times,
        }
    )
    gto_fdc = pd.DataFrame(
        {
            "E3_ENTITY": ["GTO_A"] * rows,
            "LOT": ["GTOLOT"] * rows,
            "OPERATION": ["212268"] * rows,
            "WAFER": list(range(rows)),
            "EPD": [1.0] * 200 + [2.0] * 10,
        }
    )
    cdt_fdc = pd.DataFrame(
        {
            "E3_ENTITY": ["CDT_A_CH1"] * rows,
            "LOT": ["LOT1234"] * rows,
            "OPERATION": ["174438"] * rows,
            "WAFER": list(range(rows)),
            "VALUE": [10.0] * rows,
        }
    )

    result = build_report(apc=apc, gto_fdc=gto_fdc, cdt_fdc=cdt_fdc, now=now)

    assert DELTA_FLAG_COLUMN in result.columns
    assert PERSISTENT_FLAG_COLUMN in result.columns
    assert "flagged" in result.columns
    assert len(result) == rows