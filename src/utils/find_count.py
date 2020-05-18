import logging

run_logger1 = logging.getLogger(__name__)


def find_cnt(dataframe, inp):
    run_logger1.info("generating count")

    if inp == 1:
        run_logger1.info("inp is 1")
    else:
        run_logger1.info("inp is {}".format(inp))
    return dataframe.count()
