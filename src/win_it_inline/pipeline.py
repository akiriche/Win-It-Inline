from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .settings import DATA_LOOKBACK_DAYS
from .settings import DEFAULT_DATASOURCE
from .settings import DEFAULT_OUTPUT
from .settings import DELTA_FLAG_COLUMN
from .settings import DELTA_THRESHOLD
from .settings import LONG_ROLLING_WINDOW
from .settings import PERSISTENT_FLAG_COLUMN
from .settings import PERSISTENT_FLAG_MIN_POINTS
from .settings import PRIOR_MEAN_WINDOW
from .settings import RECENT_FLAG_HOURS
from .settings import SHORT_ROLLING_WINDOW


def connect_xeus(datasource: str = DEFAULT_DATASOURCE) -> Any:
    try:
        import PyUber
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "PyUber is not installed in the active environment. Install the internal PyUber package "
            "in this venv, then rerun the pipeline."
        ) from exc

    return PyUber.connect(datasource=datasource)


def build_apc_sql(lookback_days: int) -> str:
    return f"""
      SELECT ENTITY || '_' || Chamber as RC , GPC, ENTITY, AH.OPERATION, AH.LOT, substr(AH.LOT,1,7) as LOT7, AD.Cycles, CHANGE_TIME
   FROM P_APC_TXN_HIST as AH INNER JOIN
   (
   SELECT substr(a.attribute_name,1,8) as Chamber, A.ATTRIBUTE_VALUE as GPC, B.ATTRIBUTE_VALUE as Cycles,  A.APC_DATA_ID as APC_DATA

    FROM P_APC_TXN_DATA as A
    JOIN P_APC_TXN_DATA as B on A.APC_DATA_ID = B.APC_DATA_ID and substr(a.attribute_name,1,8) = substr(b.attribute_name,1,8)
    WHERE A.ATTRIBUTE_NAME like '%_RATE_EST'
    AND B.ATTRIBUTE_NAME like '%_REC_CYCLES'
      ) as AD
   on AH.APC_DATA_ID = AD.APC_DATA
   WHERE AH.APC_OBJECT_NAME = 'CDT_LOT'
   AND AH.LAST_VERSION_FLAG = 'Y'
    and AH.CHANGE_TIME > current_date - {lookback_days}
   and AH.ENTITY <> 'CDT_1.0.0'
   and AH.OPERATION = '174438'
   order by RC asc
"""


def build_gto_fdc_sql(lookback_days: int) -> str:
    return f"""
SELECT
E3_ENTITY
,LOT
,OPERATION
,WAFER
,VALUE as EPD
FROM
F28S.P_FDC_SUMMARY_VALUE
where collection_name = 'P1274_GTOcu_CE_SASCH'
and CONTEXT_GROUP = 'M2-M5 SASCH'
and window = 'Step07_52_Spacer_EPD'
and VARIABLE = 'CurrentStepTime'
and statistic = 'Max'
and e3_entity like 'GTO%'
and RUN_START_TIME > current_date - {lookback_days}
and operation = '212268'
"""


def build_cdt_fdc_sql(lookback_days: int) -> str:
    return f"""
SELECT
E3_ENTITY
,LOT
,OPERATION
,WAFER
,VALUE
FROM
F28S.P_FDC_SUMMARY_VALUE
where collection_name = 'MFGxx_CDTnc_CE_RUIBOSDRF_R1'
and CONTEXT_GROUP = 'PROD'
and window = 'Temperature'
and VARIABLE = 'Bottle2_Temp'
and statistic = 'Mean'
and e3_entity like 'CDT%'
and RUN_START_TIME > current_date - {lookback_days}
and operation = '174438'
"""


def fetch_source_data(connection: Any, lookback_days: int = DATA_LOOKBACK_DAYS) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    apc = pd.read_sql(build_apc_sql(lookback_days), connection)
    gto_fdc = pd.read_sql(build_gto_fdc_sql(lookback_days), connection)
    cdt_fdc = pd.read_sql(build_cdt_fdc_sql(lookback_days), connection)
    return apc, gto_fdc, cdt_fdc


def compute_persistent_flags(flags: pd.Series, min_points: int = PERSISTENT_FLAG_MIN_POINTS) -> pd.Series:
    values = flags.astype(bool).to_numpy()
    run_start = np.diff(values.astype(int), prepend=0) != 0
    run_id = run_start.cumsum()
    run_lengths = pd.Series(run_id).map(pd.Series(run_id).value_counts()).to_numpy()
    persistent_values = values & (run_lengths >= min_points)
    return pd.Series(persistent_values, index=flags.index)


def build_report(
    apc: pd.DataFrame,
    gto_fdc: pd.DataFrame,
    cdt_fdc: pd.DataFrame,
    now: datetime | None = None,
) -> pd.DataFrame:
    reference_time = now or datetime.now()

    fdc_merged = gto_fdc.merge(cdt_fdc, on="WAFER", suffixes=("_GTO", "_CDT"))
    final = fdc_merged.merge(apc, left_on=["LOT_CDT", "E3_ENTITY_CDT"], right_on=["LOT", "RC"])
    final = final.drop(["LOT_CDT", "LOT_GTO", "LOT7", "ENTITY"], axis=1)

    final["CHANGE_TIME"] = pd.to_datetime(final["CHANGE_TIME"])
    final = final.sort_values(["E3_ENTITY_GTO", "CHANGE_TIME"]).reset_index(drop=True)
    final = final.groupby("E3_ENTITY_GTO", group_keys=False).apply(
        lambda group: group.assign(
            mean_1w_prior=group.rolling(window=PRIOR_MEAN_WINDOW, on="CHANGE_TIME", closed="left")["EPD"].mean()
        )
    )
    final["ratio_EPD_to_1w_mean"] = final["EPD"] / final["mean_1w_prior"]

    df = final.sort_values(["RC", "CHANGE_TIME"]).reset_index(drop=True)
    df = df.groupby("RC", group_keys=False).apply(
        lambda group: group.assign(
            ratio_last_20=group["ratio_EPD_to_1w_mean"].rolling(SHORT_ROLLING_WINDOW).mean(),
            ratio_last_200=group["ratio_EPD_to_1w_mean"].rolling(LONG_ROLLING_WINDOW).mean(),
        )
    )

    df["diff_20_200"] = (df["ratio_last_20"] - df["ratio_last_200"]).abs()
    df[DELTA_FLAG_COLUMN] = (df["diff_20_200"] > DELTA_THRESHOLD).astype("category")

    persistent_flags: list[pd.Series] = []
    for _, group in df.groupby("RC"):
        flag_values = group[DELTA_FLAG_COLUMN].astype(str) == "True"
        persistent_flags.append(compute_persistent_flags(flag_values, PERSISTENT_FLAG_MIN_POINTS))

    df[PERSISTENT_FLAG_COLUMN] = pd.concat(persistent_flags).sort_index().astype("category")
    df["indicator"] = df[PERSISTENT_FLAG_COLUMN].astype(int)
    df["flagged"] = (df["indicator"] == 1) & (
        df["CHANGE_TIME"] > reference_time - timedelta(hours=RECENT_FLAG_HOURS)
    )

    return df


def run_pipeline(
    output_path: str | Path = DEFAULT_OUTPUT,
    datasource: str = DEFAULT_DATASOURCE,
    lookback_days: int = DATA_LOOKBACK_DAYS,
) -> Path:
    connection = connect_xeus(datasource=datasource)
    apc, gto_fdc, cdt_fdc = fetch_source_data(connection, lookback_days=lookback_days)
    report = build_report(apc, gto_fdc, cdt_fdc)

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(destination, index=False)
    return destination