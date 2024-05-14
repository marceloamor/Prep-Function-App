# from prep.helpers import time_series_interpolation

# import pandas as pd
# import pytest

# from datetime import date
# import logging


# @pytest.mark.parametrize(
#     ["input_df", "expected_df"],
#     [
#         [
#             pd.DataFrame(
#                 {
#                     "date": [
#                         date(2023, 9, 15),
#                         date(2023, 9, 16),
#                         date(2023, 9, 17),
#                         date(2023, 9, 18),
#                         date(2023, 9, 19),
#                         date(2023, 9, 20),
#                         date(2023, 9, 21),
#                         date(2023, 9, 22),
#                         date(2023, 9, 23),
#                         date(2023, 9, 24),
#                         date(2023, 9, 25),
#                         date(2023, 9, 26),
#                     ],
#                     "continuous_rate": [
#                         0.003,
#                         None,
#                         None,
#                         0.0033,
#                         0.0035,
#                         0.0036,
#                         0.00365,
#                         0.0037,
#                         None,
#                         None,
#                         0.004,
#                         0.0041,
#                     ],
#                 },
#             ),
#             pd.DataFrame(
#                 {
#                     "date": [
#                         date(2023, 9, 15),
#                         date(2023, 9, 16),
#                         date(2023, 9, 17),
#                         date(2023, 9, 18),
#                         date(2023, 9, 19),
#                         date(2023, 9, 20),
#                         date(2023, 9, 21),
#                         date(2023, 9, 22),
#                         date(2023, 9, 23),
#                         date(2023, 9, 24),
#                         date(2023, 9, 25),
#                         date(2023, 9, 26),
#                     ],
#                     "continuous_rate": [
#                         0.003,
#                         None,
#                         None,
#                         0.0033,
#                         0.0035,
#                         0.0036,
#                         0.00365,
#                         0.0037,
#                         None,
#                         None,
#                         0.004,
#                         0.0041,
#                     ],
#                     "interp_cont_rate": [
#                         0.003,
#                         0.0031,
#                         0.0032,
#                         0.0033,
#                         0.0035,
#                         0.0036,
#                         0.00365,
#                         0.0037,
#                         0.0038,
#                         0.0039,
#                         0.004,
#                         0.0041,
#                     ],
#                 }
#             ),
#         ],
#         [
#             pd.DataFrame(
#                 {
#                     "date": [
#                         date(2023, 9, 15),
#                         date(2023, 9, 18),
#                         date(2023, 9, 19),
#                         date(2023, 9, 20),
#                         date(2023, 9, 21),
#                         date(2023, 9, 22),
#                         date(2023, 9, 25),
#                         date(2023, 9, 26),
#                     ],
#                     "continuous_rate": [
#                         0.003,
#                         0.0033,
#                         0.0035,
#                         0.0036,
#                         0.00365,
#                         0.0037,
#                         0.004,
#                         0.0041,
#                     ],
#                 },
#             ),
#             pd.DataFrame(
#                 {
#                     "date": [
#                         date(2023, 9, 15),
#                         date(2023, 9, 16),
#                         date(2023, 9, 17),
#                         date(2023, 9, 18),
#                         date(2023, 9, 19),
#                         date(2023, 9, 20),
#                         date(2023, 9, 21),
#                         date(2023, 9, 22),
#                         date(2023, 9, 23),
#                         date(2023, 9, 24),
#                         date(2023, 9, 25),
#                         date(2023, 9, 26),
#                     ],
#                     "continuous_rate": [
#                         0.003,
#                         None,
#                         None,
#                         0.0033,
#                         0.0035,
#                         0.0036,
#                         0.00365,
#                         0.0037,
#                         None,
#                         None,
#                         0.004,
#                         0.0041,
#                     ],
#                     "interp_cont_rate": [
#                         0.003,
#                         0.0031,
#                         0.0032,
#                         0.0033,
#                         0.0035,
#                         0.0036,
#                         0.00365,
#                         0.0037,
#                         0.0038,
#                         0.0039,
#                         0.004,
#                         0.0041,
#                     ],
#                 }
#             ),
#         ],
#     ],
# )
# def test_interpolate_dataframe_time_series(input_df, expected_df):
#     input_df.index = pd.DatetimeIndex(data=input_df.loc[:, "date"])
#     expected_df.index = pd.DatetimeIndex(data=expected_df.loc[:, "date"])
#     output_df = time_series_interpolation.interpolate_on_time_series_df(
#         input_df, "continuous_rate", "interp_cont_rate"
#     )

#     check_data: pd.Series = (
#         output_df.loc[:, "interp_cont_rate"] - expected_df.loc[:, "interp_cont_rate"]
#     )
#     logging.warning("interp_cont_rate differences:\n%s", check_data)

#     assert output_df.index.equals(
#         expected_df.index
#     ), "Output and expected indices don't match"
#     assert check_data.le(1e-10).all(), "Expected rate mismatch"


# def test_interpolate_dataframe_time_series_errors_on_non_dt_input():
#     input_df = pd.DataFrame(
#         {
#             "date": [
#                 date(2023, 9, 15),
#                 date(2023, 9, 18),
#                 date(2023, 9, 19),
#                 date(2023, 9, 20),
#                 date(2023, 9, 21),
#                 date(2023, 9, 22),
#                 date(2023, 9, 25),
#                 date(2023, 9, 26),
#             ],
#             "continuous_rate": [
#                 0.003,
#                 0.0033,
#                 0.0035,
#                 0.0036,
#                 0.00365,
#                 0.0037,
#                 0.004,
#                 0.0041,
#             ],
#         },
#     )
#     with pytest.raises(ValueError):
#         _ = time_series_interpolation.interpolate_on_time_series_df(
#             input_df, "continuous_rate", "shouldnt_be_populated"
#         )
