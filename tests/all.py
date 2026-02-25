import pathlib
import unittest
import coverage
import sys

TEST_DIR = pathlib.Path(__file__).absolute().parent

sys.path.append(str(TEST_DIR.parent / "src"))
sys.path.append(str(TEST_DIR))

if __name__ == '__main__':
    cov = coverage.Coverage(
        config_file=TEST_DIR / ".coveragerc",
        source_pkgs=["cnodc"]
    )
    target = str(TEST_DIR)
    if len(sys.argv) > 1:
        target = str(TEST_DIR / sys.argv[1])
    loader = unittest.TestLoader()
    cov.start()
    suite = loader.discover(target)
    runner = unittest.TextTestRunner()
    runner.run(suite)
    cov.stop()
    cov.save()


