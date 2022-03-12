
import pandas as pd


def read_log_file(f_path):

    colnames =['level', 'time', 'caller', 'function', 'start_or_end', 'exec_time']
    df_logs = pd.read_csv(f_path, sep="|", header=None, names=colnames)

    # strip whitespace
    df_logs = df_logs.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    return df_logs


def log_file_summary_analysis():

    df_logs = read_log_file("FEAT.log")

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

    df_logs_tbl.to_csv("log_summary.csv", index=False)

    print(df_logs_tbl)


if __name__ == "__main__":

    log_file_summary_analysis()

