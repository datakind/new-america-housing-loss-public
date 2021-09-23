"""
Command line interface for analyzing housing loss data and creating
statistical summaries and visualization files.
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter


def main():
    print('hello world')


if __name__ == '__main__':
    parser = ArgumentParser(
        description=(
            "Command line interface for analyzing housing loss data and creating "
            "statistical summaries and visualization files."
        ),
        prog='collection',
        formatter_class=RawDescriptionHelpFormatter,
        epilog="""
    Examples:
      python -m collection
    """,
    )
    exit(main())
