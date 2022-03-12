import csv
import datetime
import math
import os
import typing as T

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as stats
import seaborn as sns

from const import OUTPUT_PATH_PLOTS_DETAIL, STAT_SIGNIFICANCE_CUTOFF
from loggy import log_machine

# line below suppresses annoying SettingWithCopyWarning
pd.options.mode.chained_assignment = None

sig_results = {}
all_results = {}

@log_machine
def get_acs_vars_for_analysis() -> T.List:

    """Function to identify/grab which ACS variables
    will be included in the correlation analysis.

    Parameters
    ----------
    none

    Returns
    -------
    vars : list
        A list of human-readable variable names.  The
        ACS raw names are converted to human-readable
        names in the load_census_data function.
    """

    vars = [
        'total-renter-occupied-households',
        'pct-renter-occupied',
        'pct-owner-occupied',
        'pct-owner-occupied-mortgage',
        'pct-owner-occupied-without-mortgage',
        'median-gross-rent',
        'median-year-structure-built',
        'median-household-income',
        'median-property-value',
        'median-house-age',
        'median-monthly-housing-cost',
        'pct-white',
        'pct-af-am',
        'pct-hispanic',
        'pct-am-in',
        'pct-asian',
        'pct-nh-pi',
        'pct-multiple-race',
        'pct-other-race',
        'pct-non-white',
        'pct-below-poverty-level',
        'pct-without-health-insurance',
        'pct-pop-in-labor-force',
        'pop-total',
        'pct-households-married-with-own-children',
        'pct-male-single-parent-household',
        'pct-female-single-parent-household',
        'pct-male-older-adult-living-alone',
        'pct-female-older-adult-living-alone',
        'pct-households-with-children',
        'pct-households-with-elderly',
        'pct-enrolled-in-school',
        'education-attained',
        'pct-veterans',
        'pct-foreign-born',
        'pct-not-us-citizen',
        'pct-disability',
        'gini-index',
        'level-of-education-less-than-9th',
        'unemployment-rate',
        'pct-women-in-labor-force',
        'mean-commute-time',
        'pct-service-occupations',
        'pct-public-transport-to-work',
        'pct-with-snap-benefits',
        'per-capita-income',
        'pct-vacant-properties',
        'pct-no-vehicles-available',
        'pct-incomplete-plumbing',
        'pct-non-english-spoken-in-home',
        'pct-broadband-internet',
        'pct-own-computer',
        'pct-english-fluency-not-great',
        'pct-one-or-less-occupants-per-room',
        'pct-mobile-homes',
        'median-income-male-worker',
        'median-income-female-worker',
        'median-income-diff-male-vs-female',
        'median-population-age',
        'total-owner-occupied-households-mortgage',
    ]

    return vars


@log_machine
def calc_acs_correlations(df: pd.DataFrame, x_var: str, y_var: str):

    """Function to calculate correlations.

    Parameters
    ----------
    df : pandas df
        Final pandas df containing housing loss data
        and ACS variables to search within for correlations.

    x_var : string
        name of first variable to include in correlation search
        (likely an ACS variable name).

    y_var: string
        name of second variable to include in correlation
        search (likely a housing loss metric).


    Returns
    -------
    corr: 1x2 numerical array
        From either stats.pearsonr or stats.spearmanr.
        First element is the correlation, second is the
        p-value.  Note that we are assuming we have near-census
        level data collection so p-value doesn't make much
        sense here.

    """

    x = df[x_var]
    y = df[y_var]

    corr = 0.0

    try:
        corr = stats.pearsonr(x, y)
        # corr = stats.spearmanr(x,y)

    except:
        print('Couldn\'t calculate correlation between ' + x_var + ' and ' + y_var)
        corr = [999.0, 999.0]

    return corr


@log_machine
def plot_acs_correlations(
    df: pd.DataFrame, x_var: str, y_var: str, plot_write_path: str
) -> None:
    """For each variable pairs (x_var and y_var),
    calculate, visualize, and save correlation results.

    Parameters
    ----------
    df : pandas df
        Final pandas df containing housing loss data
        and ACS variables to search within for correlations.

    x_var : string
        name of first variable to include in correlation search
        (likely an ACS variable name).

    y_var: string
        name of second variable to include in correlation
        search (likely a housing loss metric).

    Returns
    -------
    none
    (function outputs: saving individual scatter plot
    images and storing important results in dictionary sig_results)

    """

    # ignore self-correlation
    if x_var == y_var:
        return

    # drop NaN values and negative before calculating correlation.
    df.dropna(subset=[x_var, y_var], inplace=True)

    # make sure all relevant values between 0 and 1e7.
    # this is because some error codes seem to be -1e8, some +1e8
    # note this excludes all negative values.  as far as I've seen,
    # no ACS/housing loss variables should have negative values.
    df = df[df[x_var] >= 0.0]
    df = df[df[y_var] >= 0.0]

    df = df[df[x_var] <= 1.0e7]
    df = df[df[y_var] <= 1.0e7]

    corr_results = calc_acs_correlations(df, x_var, y_var)

    r_value = round(corr_results[0], 3)
    p_value = round(corr_results[1], 3)

    title_string = "ACS Correlations\n {} vs. {}: \n r = {}".format(
        y_var, x_var, r_value
    )
    ###only relevant for non-production code study
    all_results[x_var] = r_value
    ###

    if math.fabs(r_value) >= STAT_SIGNIFICANCE_CUTOFF:
        file_string = "strong_corr_{}_vs_{}.png".format(y_var, x_var)
        sig_results[x_var] = r_value
    else:
        file_string = "weak_corr_{}_vs_{}.png".format(y_var, x_var)

    f, ax = plt.subplots()
    corr_plt = sns.regplot(x=x_var, y=y_var, data=df).set_title(title_string)
    figure = corr_plt.get_figure()
    try:
        figure.savefig(
            str(plot_write_path / OUTPUT_PATH_PLOTS_DETAIL / file_string), dpi=200
        )
    except FileNotFoundError:
        print(
            'Error: The absolute file path is too long for Python to save this file. '
            'Please shorten the file path to your data directory'
        )
    plt.close()


@log_machine
def correlation_analysis(
    census_df: pd.DataFrame,
    processed_data_df: pd.DataFrame,
    target_var: str,
    plot_write_path: str,
) -> None:

    ### Defining the list of variables to run correlations on
    acs_vars_for_correlations = get_acs_vars_for_analysis()

    ###  Here's the housing loss file.  for now it's grabbing a previous version.  we'll have to replace with calculated file.
    to_keep = ['geoid', target_var]
    processed_data_df = processed_data_df[to_keep]
    processed_data_df = processed_data_df[processed_data_df['geoid'].notna()]
    processed_data_df.geoid = processed_data_df.geoid.astype(str)
    census_df.GEOID = census_df.GEOID.astype(str)

    mrg = processed_data_df.merge(census_df, left_on='geoid', right_on='GEOID')

    hl_type = ''
    if target_var == 'total_filings':
        hl_type = 'evictions'
    if target_var == 'total_foreclosures':
        hl_type = 'all foreclosures'
    if target_var == 'housing-loss-index':
        hl_type = 'all types of housing loss'

    print(
        '\nCalculating correlations and visualizing the strongest relationships for '
        + str(hl_type)
        + '...'
    )

    for i in acs_vars_for_correlations:
        plot_acs_correlations(mrg, i, target_var, plot_write_path)

    acs_vars_for_correlations.append('GEOID')

    sig_results_series = pd.Series(sig_results)

    sig_results_df = pd.DataFrame(
        {'variable': sig_results_series.index, 'correlation': sig_results_series.values}
    )

    df_sorted = sig_results_df.loc[(sig_results_df.correlation).abs().argsort()]

    plt.rcParams['figure.figsize'] = [15, 25]

    df_sorted.plot(kind='barh', legend=None)
    plt.yticks(range(len(df_sorted['variable'])), list(df_sorted['variable']))

    plt.title(
        'significant relationships \n with ' + target_var, size=30, fontweight='bold'
    )
    plt.xlabel('correlation', size=30)
    plt.tight_layout()
    plt.yticks(size=30)
    plt.xticks(size=20)
    plt.grid()
    plt.tight_layout()

    fname = target_var + '_significant_correlation_results.png'

    plt.savefig(str(plot_write_path / fname))

    ser = pd.Series(all_results)

    all_res_df = pd.DataFrame({'var': ser.index, 'corr': ser.values})

    plt.figure()

    print('Summarizing all analysis results...')

    # Contextualize results with previous partner site data

    ###  Load previous site data.  unfortunately currently pulling from two
    ###  files (same data but different indices) due to a particularity
    ###  with the boxplot implementation
    prev_sites = pd.read_csv('static_data/prev_data_with_labels.csv', index_col='var')
    prev_sites_ind = prev_sites.reset_index(drop=True)

    bp = plt.boxplot(prev_sites_ind.T, vert=False, showfliers=False)
    _ = plt.yticks(np.arange(len(prev_sites)) + 1, prev_sites.index)

    for _, line_list in bp.items():
        for line in line_list:
            line.set_color('b')

    plt.xlabel('Pearson correlation', size=15)
    plt.title('Contextualized correlations \n (2019 ACS data)', size=30)
    plt.scatter(
        x=all_res_df['corr'], y=np.arange(len(all_res_df)) + 1, c='r', marker='o', s=100
    )
    plt.grid()
    plt.yticks(size=25)
    plt.xticks(size=15)
    plt.tight_layout()
    fname = 'contextualized-correlations-all-variables.png'
    plt.savefig(str(plot_write_path / fname))

    plt.figure()

    ### Focus on race/ethnicity results

    race_indices = [19, 18, 17, 16, 15, 14, 13, 12, 11]

    race_df = prev_sites_ind.filter(items=race_indices, axis=0)
    race_df_lb = prev_sites.filter(
        items=[
            'pct-non-white',
            'pct-other-race',
            'pct-multiple-race',
            'pct-nh-pi',
            'pct-asian',
            'pct-am-in',
            'pct-hispanic',
            'pct-af-am',
            'pct-white',
        ],
        axis=0,
    )
    race_df_prt = all_res_df.filter(items=race_indices, axis=0)

    bp = plt.boxplot(race_df.T, vert=False, showfliers=False)  # whis=[5,95])
    _ = plt.yticks(np.arange(len(race_df_lb)) + 1, race_df_lb.index)

    for _, line_list in bp.items():
        for line in line_list:
            line.set_color('b')

    plt.xlabel('Pearson correlation', size=20)
    plt.title('Contextualized correlations \n (2019 ACS data)', size=30)
    plt.scatter(
        x=race_df_prt['corr'],
        y=np.arange(len(race_df_prt)) + 1,
        c='r',
        marker='o',
        s=200,
    )
    plt.grid()
    plt.yticks(size=25)
    plt.xticks(size=20)
    plt.tight_layout()
    fname = 'contextualized-correlations-race-and-ethnicity-variables.png'
    plt.savefig(str(plot_write_path / OUTPUT_PATH_PLOTS_DETAIL / fname))

    plt.figure()

    ### Focus on financially-related results

    fin_indices = [7, 5, 10, 20, 21, 22, 37, 39, 44, 45, 51, 55, 56, 57]

    fin_df = prev_sites_ind.filter(items=fin_indices, axis=0)

    fin_df_lb = prev_sites.filter(
        items=[
            'median-household-income',
            'median-gross-rent',
            'median-monthly-housing-cost',
            'pct-below-poverty-level',
            'pct-without-health-insurance',
            'pct-pop-in-labor-force',
            'gini-index',
            'unemployment-rate',
            'pct-with-snap-benefits',
            'per-capita-income',
            'pct-own-computer',
            'median-income-male-worker',
            'median-income-female-worker',
            'median-income-diff-male-vs-female',
        ],
        axis=0,
    )

    fin_df_prt = all_res_df.filter(items=fin_indices, axis=0)

    bp = plt.boxplot(fin_df.T, vert=False, showfliers=False)
    _ = plt.yticks(np.arange(len(fin_df_lb)) + 1, fin_df_lb.index)

    for _, line_list in bp.items():
        for line in line_list:
            line.set_color('b')

    plt.xlabel('Pearson correlation', size=15)
    plt.title('Contextualized correlations \n (2019 ACS data)', size=30)
    plt.scatter(
        x=fin_df_prt['corr'], y=np.arange(len(fin_df_prt)) + 1, c='r', marker='o', s=200
    )
    plt.grid()
    plt.yticks(size=25)
    plt.xticks(size=20)
    plt.tight_layout()
    fname = 'contextualized-correlations-financial-variables.png'
    plt.savefig(str(plot_write_path / OUTPUT_PATH_PLOTS_DETAIL / fname))

    plt.figure()

    ### Focus on housing characteristic results

    hou_indices = [0, 1, 2, 3, 4, 6, 46, 48, 50, 8, 9, 54, 59]

    hou_df = prev_sites_ind.filter(items=hou_indices, axis=0)

    hou_df_lb = prev_sites.filter(
        items=[
            'total-renter-occupied-households',
            'pct-renter-occupied',
            'pct-owner-occupied',
            'pct-owner-occupied-mortgage',
            'pct-owner-occupied-without-mortgage',
            'median-year-structure-built',
            'pct-vacant-properties',
            'pct-incomplete-plumbing',
            'pct-broadband-internet',
            'median-property-value',
            'median-house-age',
            'pct-mobile-homes',
            'total-owner-occupied-households-mortgage',
        ],
        axis=0,
    )

    hou_df_prt = all_res_df.filter(items=hou_indices, axis=0)

    bp = plt.boxplot(hou_df.T, vert=False, showfliers=False)
    _ = plt.yticks(np.arange(len(hou_df_lb)) + 1, hou_df_lb.index)

    for _, line_list in bp.items():
        for line in line_list:
            line.set_color('b')

    plt.xlabel('Pearson correlation', size=15)
    plt.title('Contextualized correlations \n (2019 ACS data)', size=30)
    plt.scatter(
        x=hou_df_prt['corr'], y=np.arange(len(hou_df_prt)) + 1, c='r', marker='o', s=200
    )
    plt.grid()
    plt.yticks(size=25)
    plt.xticks(size=20)
    plt.tight_layout()
    fname = 'contextualized-correlations-housing-variables.png'
    plt.savefig(str(plot_write_path / OUTPUT_PATH_PLOTS_DETAIL / fname))

    plt.figure()

    ### All other housing characteristic results

    oth_indices = [
        23,
        24,
        25,
        26,
        27,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        47,
        48,
        49,
        52,
        53,
        58,
    ]

    oth_df = prev_sites_ind.filter(items=oth_indices, axis=0)

    oth_df_lb = prev_sites.filter(
        items=[
            'pop-total',
            'pct-households-married-with-own-children',
            'pct-male-single-parent-household',
            'pct-female-single-parent-household',
            'pct-male-older-adult-living-alone',
            'pct-female-older-adult-living-alone',
            'pct-households-with-children',
            'pct-households-with-elderly',
            'pct-enrolled-in-school',
            'education-attained',
            'pct-veterans',
            'pct-foreign-born',
            'pct-not-us-citizen',
            'pct-disability',
            'level-of-education-less-than-9th',
            'unemployment-rate',
            'pct-women-in-labor-force',
            'mean-commute-time',
            'pct-service-occupations',
            'pct-public-transport-to-work',
            'pct-no-vehicles-available',
            'pct-incomplete-plumbing',
            'pct-non-english-spoken-in-home',
            'pct-english-fluency-not-great',
            'pct-one-or-less-occupants-per-room',
            'median-population-age',
        ],
        axis=0,
    )

    oth_df_prt = all_res_df.filter(items=oth_indices, axis=0)

    bp = plt.boxplot(oth_df.T, vert=False, showfliers=False)
    _ = plt.yticks(np.arange(len(oth_df_lb)) + 1, oth_df_lb.index)

    for _, line_list in bp.items():
        for line in line_list:
            line.set_color('b')

    plt.xlabel('Pearson correlation', size=15)
    plt.title('Contextualized correlations \n (2019 ACS data)', size=30)
    plt.scatter(
        x=oth_df_prt['corr'], y=np.arange(len(oth_df_prt)) + 1, c='r', marker='o', s=200
    )
    plt.grid()
    plt.yticks(size=25)
    plt.xticks(size=20)
    plt.tight_layout()
    fname = 'contextualized-correlations-other-variables.png'
    plt.savefig(str(plot_write_path / OUTPUT_PATH_PLOTS_DETAIL / fname))

    print(
        '*** Saved ACS correlation analysis summaries for '
        + str(hl_type)
        + ' to '
        + str(plot_write_path)
    )
