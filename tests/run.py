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
    files = 'test*.py'
    if len(sys.argv) > 1:
        target = TEST_DIR / sys.argv[1]
        if target.name.endswith('.py'):
            files = target.name
            target = target.parent
    loader = unittest.TestLoader()
    cov.start()
    suite = loader.discover(target, files)
    runner = unittest.TextTestRunner()
    runner.run(suite)
    cov.stop()
    cov.save()


