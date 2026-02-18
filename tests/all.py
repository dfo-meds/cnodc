import pathlib
import unittest
import coverage

TEST_DIR = pathlib.Path(__file__).absolute().parent

if __name__ == '__main__':
    cov = coverage.Coverage(
        config_file=TEST_DIR / ".coveragerc",
        source_pkgs=["cnodc"]
    )
    loader = unittest.TestLoader()
    cov.start()
    suite = loader.discover(str(TEST_DIR))
    runner = unittest.TextTestRunner()
    runner.run(suite)
    cov.stop()
    cov.save()


