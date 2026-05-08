import pathlib
import logging

__VERSION__ = '0.1.0'

ROOT_DIR = pathlib.Path(__file__).absolute().resolve().parent.parent.parent



def init_pipeman(app_type: str,
                 with_mp_prometheus_default: bool = False,
                 no_mp: bool = False):

    import gcapp.boot as gcboot
    gcboot.boot(
        app_name='pipeman',
        app_components=[app_type],
        extra_config_paths=None if app_type != 'tests' else ['./tests'],
        individual_log_levels={
            'pybufrkit.coder.log': logging.WARNING
        },
        manual_overrides={
            "medsutil.email.DelayedEmailController": "pipeman.delayed_emails.DelayedEmailsQueuer",
            "nodb.interface.NODB": "nodb.controller.NODBPostgresController"
        },
        is_multiprocessing=not no_mp,
        create_local_prom_mp_dir=with_mp_prometheus_default,
        version_no=__VERSION__
    )

def init_for_tests(skip_long_tests: bool = True,
                   disable_metrics: bool = True,
                   fast_password_hashing: bool = True,
                   with_mp_prometheus_default: bool = True):

    # Setup config and logging
    init_pipeman('tests', with_mp_prometheus_default if not disable_metrics else False)

    if disable_metrics:
        # Prevent metrics from being loaded
        from autoinject import injector
        from medsutil.metrics import PromMetrics
        @injector.inject
        def _disable_metrics(pm: PromMetrics = None):
            pm.disable_metrics = True
        _disable_metrics()

    if fast_password_hashing:
        # speed up password hashing for tests only!
        import medsutil.secure as s
        s.DEFAULT_PASSWORD_HASH_ITERATIONS = 1
        s.MINIMUM_ITERATIONS = 2

    # skip long tests unless requested to run (there's a lot of them
    if skip_long_tests:
        import tests.helpers.base_test_case as btc
        btc.SKIP_FLAG.set()
