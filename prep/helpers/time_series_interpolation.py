import pandas.core.dtypes.common
import pandas as pd


def interpolate_on_time_series_df(
    base_dataframe: pd.DataFrame,
    data_column_name: str,
    output_column_name: str,
    frequency="D",
    **interpolation_kwargs
) -> pd.DataFrame:
    if not pandas.core.dtypes.common.needs_i8_conversion(base_dataframe.index.dtype):
        raise ValueError(
            "time-weighted interpolation only works on Series or DataFrames with DatetimeIndex"
        )
    new_interpolation_dts = pd.date_range(
        base_dataframe.index.min(), base_dataframe.index.max(), freq=frequency
    )
    new_df = base_dataframe.copy(deep=True)

    new_interpolation_dts: pd.DatetimeIndex = new_interpolation_dts.append(
        base_dataframe.index
    )
    new_interpolation_dts = new_interpolation_dts.unique()
    new_df = new_df.reindex(new_interpolation_dts)
    new_df.loc[:, output_column_name] = new_df.loc[:, data_column_name].interpolate(
        method="time", **interpolation_kwargs
    )
    return new_df
