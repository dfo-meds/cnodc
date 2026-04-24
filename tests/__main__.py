import os
import pathlib
import unittest
import logging
import sys
import coverage

TEST_DIR = pathlib.Path(__file__).absolute().parent

if __name__ == '__main__':

    new_argv = None
    if len(sys.argv) == 1:
        new_argv = [sys.argv[0], 'discover', '-s', 'tests', '-t', str(TEST_DIR.parent)]
    else:
        new_argv = list(sys.argv)

    skip_long_tests = True
    if '--with-long-tests' in new_argv:
        new_argv.remove('--with-long-tests')
        skip_long_tests = False

    cov = coverage.Coverage(
        config_file=TEST_DIR.parent / ".coveragerc",
    )
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = "C:/my/cnodc/.temp_prometheus"
    with cov.collect():
        from pipeman.boot import init_for_tests
        init_for_tests(skip_long_tests)
        unittest.main(
            module=None,
            argv=new_argv
        )

    cov.save()

