import pathlib
import unittest
import sys

TEST_DIR = pathlib.Path(__file__).absolute().parent

def run_tests(argv: list,
              with_profile: bool = False,
              with_coverage: bool = False,
              **kwargs):
    if with_coverage:
        import coverage
        cov = coverage.Coverage(
            config_file=TEST_DIR.parent / ".coveragerc",
        )
        with cov.collect():
            run_tests(argv, with_profile, False, **kwargs)
        cov.save()

    elif with_profile:
        import cProfile
        import pstats
        profile = cProfile.Profile()
        with profile as pr:
            run_tests(argv, False, False, **kwargs)
            pr.dump_stats(TEST_DIR.parent / ".profile.dat")
            stats = pstats.Stats(pr)
            stats.sort_stats('time')
            stats.print_stats(25)
            with open(TEST_DIR.parent / ".profile_results.txt", "w") as f:
                stats2 = pstats.Stats(pr, stream=f)
                stats2.sort_stats('time')
                stats2.print_stats()



    else:
        from pipeman.boot import init_for_tests
        init_for_tests(**kwargs)
        unittest.main(
            module=None,
            argv=new_argv
        )



if __name__ == '__main__':

    new_argv = None
    if len(sys.argv) == 1:
        new_argv = [sys.argv[0], 'discover', '-s', 'tests', '-t', str(TEST_DIR.parent)]
    else:
        new_argv = list(sys.argv)


    short_args = []
    for arg in new_argv:
        if len(arg) > 2 and arg[0] == "-" and arg[1] != "-":
            short_args.append(arg)

    for short_arg in short_args:
        new_argv.remove(short_arg)
        new_argv.extend(f"-{x}" for x in short_arg[1:])

    # Strip out some custom arguments for this module, the rest go to unittest
    kwargs = {
        'with_profile': False,
        'with_coverage': False,
        'with_long_tests': False,
        'with_metrics': False,
        'with_fast_passwords': True,
    }
    flag_map = {
        '--long-tests': ('with_long_tests', True),
        '-L': ('with_long_tests', True),
        '--metrics': ('with_metrics', True),
        '-M': ('with_metrics', True),
        '--coverage': ('with_coverage', True),
        '-C': ('with_coverage', True),
        '--profile': ('with_profile', True),
        '-P': ('with_profile', True),
        '--long-passwords': ('with_fast_passwords', False),
    }

    for flag, kwarg in flag_map.items():
        if flag in new_argv:
            new_argv.remove(flag)
            kwargs[kwarg[0]] = kwarg[1]

    run_tests(new_argv, **kwargs)



