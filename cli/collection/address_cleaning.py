import typing as T

import numpy as np
import pandas as pd


def remove_special_chars(text: str) -> str:
    """Remove special characters from text
    Inputs
    ------
    text: a string-valued row of a column or a single text string
    Outputs
    -------
    text: a clean string with no special characters
    """
    if not isinstance(text, str):
        print('Non-string text identified, setting field to empty string')
        return ''

    for special_chars in [
        '\\',
        '`',
        '*',
        '{',
        '}',
        '[',
        ']',
        '(',
        ')',
        '>',
        '#',
        '^',
        '*',
        '@',
        '!',
        '+',
        '.',
        '%',
        '!',
        '$',
        '&',
        ':',
        '\'',
    ]:
        if special_chars in text:
            text = text.replace(special_chars, "")
    return text


def get_zipcode5(raw_zipcode: T.Union[int, float, str, None]) -> T.Union[str, None]:
    """Clean up the zip code to convert it to 5-character strings."""
    if pd.isna(raw_zipcode):
        zip_code_5 = np.nan
    # If column is numeric, convert to string
    elif isinstance(raw_zipcode, (int, float)):
        if raw_zipcode <= 99999:
            zip_code_5 = str(int(raw_zipcode)).zfill(5)
        else:
            zip_code_5 = str(int(raw_zipcode))[:5].zfill(5)
    else:
        zip_code_5 = raw_zipcode[:5].zfill(5)

    return zip_code_5
