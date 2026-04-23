import pathlib
import unittest
import logging
import sys
import coverage

TEST_DIR = pathlib.Path(__file__).absolute().parent

if __name__ == '__main__':

    new_argv = None
    if len(sys.argv) == 1:
        new_argv = [sys.argv[0], 'discover', '-s', 'tests', '-t', '.']
    else:
        new_argv = list(sys.argv)

    skip_long_tests = True
    if '--with-long-tests' in new_argv:
        new_argv.remove('--with-long-tests')
        skip_long_tests = False

    cov = coverage.Coverage(
        config_file=TEST_DIR.parent / ".coveragerc",
        source_pkgs=["cnodc"]
    )

    with cov.collect():

        from pipeman.boot import init_cnodc
        init_cnodc('tests')
        logging.disable(logging.NOTSET)

        # speed up password hashing for tests only!
        import medsutil.secure as s
        s.DEFAULT_PASSWORD_HASH_ITERATIONS = 1
        s.MINIMUM_ITERATIONS = 2

        # skip long tests unless requested to run (there's a lot of them
        if skip_long_tests:
            import tests.helpers.base_test_case as btc
            btc.SKIP_FLAG.set()

        unittest.main(
            module=None,
            argv=new_argv
        )

    cov.save()

