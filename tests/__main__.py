import pathlib
import unittest
import logging
import sys
import coverage

TEST_DIR = pathlib.Path(__file__).absolute().parent

if __name__ == '__main__':
    logging.disable(logging.NOTSET)
    logging.getLogger().setLevel('WARNING')
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter('[%(levelname)s %(asctime)s]: %(message)s [%(name)s]', '%H:%M:%S'))
    new_argv = None
    if len(sys.argv) == 1:
        new_argv = [sys.argv[0], 'discover', '-s', 'tests', '-t', '.']
    logging.getLogger().addHandler(h)
    cov = coverage.Coverage(
        config_file=TEST_DIR.parent / ".coveragerc",
        source_pkgs=["cnodc"]
    )
    with cov.collect():
        from pipeman.boot import init_cnodc
        # speed up password hashing for tests only!
        init_cnodc('tests')
        import medsutil.secure as s
        s.DEFAULT_PASSWORD_HASH_ITERATIONS = 1
        s.MINIMUM_ITERATIONS = 2
        unittest.main(
            module=None,
            argv=new_argv
        )
    cov.save()

