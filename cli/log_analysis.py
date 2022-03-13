
"""
utiliity function to summarize execution time, function calls from log file
"""

import pandas as pd

LOG_FILE = 'DKHousingLoss.log'


def read_log_file(f_path: str) -> pd.DataFrame:
    """
    reads log file, retruns as dataframe

    :param f_path:
    :return: df_logs : pd.DataFrame,
    """

    colnames =['level', 'time', 'caller', 'function', 'start_or_end', 'exec_time']
    df_logs = pd.read_csv(f_path, sep="|", header=None, names=colnames)

    # strip whitespace
    df_logs = df_logs.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    return df_logs


def log_file_summary_analysis(log_filename: str) -> pd.DataFrame:
    """
    performs pivot table type summary of log file
    returns table that includes:
        time duration in each function (total for all calls)
        number of times each function is called during execution
        (this assumes logging_level was set to INFO to record entry and exit fo each function)

    :param log_filename:
    :return: df_logs_tbl, pd.DataFrame, pivot table type summary log file
    """

    df_logs = read_log_file(log_filename)

    df_logs = df_logs[df_logs.level == 'INFO']

    # retain only 1 row for each function call, the row with the execution time
    df_logs = df_logs[df_logs.start_or_end == 'complete']

    # aggregate for number of calls and sum total execution time
    df_logs_tbl = df_logs.groupby('function').agg({'level': 'count', 'exec_time': 'sum'})\
                                            .reset_index()\
                                            .rename(columns={'level':'number_calls'})

    # sort in descending order
    df_logs_tbl.sort_values(by=['exec_time', 'number_calls'],
                            ascending=False,
                            inplace=True)
    df_logs_tbl.reset_index(inplace=True, drop=True)

    return df_logs_tbl


if __name__ == "__main__":

    df_summary_tbl = log_file_summary_analysis(LOG_FILE)

    df_summary_tbl.to_csv("log_summary.csv", index=False)
    print(df_summary_tbl)

