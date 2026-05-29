import pathlib
import logging

__VERSION__ = '0.1.0'

MY_DIR = pathlib.Path(__file__).absolute().resolve().parent

def init_pipeman(app_type: str,
                 with_mp_prometheus_default: bool = False,
                 no_mp: bool = False):
    import gcapp.boot as gcboot
    gcboot.boot(
        app_name='pipeman',
        app_components=[app_type],
        env_map_files=[
            MY_DIR / '.env_map.yaml',
        ],
        extra_config_paths=None if app_type != 'tests' else ['./tests'],
        individual_log_levels={
            'pybufrkit.coder.log': logging.WARNING
        },

        manual_overrides={
            "medsutil.email.DelayedEmailController": "pipeman.delayed_emails.DelayedEmailsQueuer",
            "nodb.interface.NODB": "nodb.controller.NODBPostgresController"
        },
        version_no=__VERSION__
    )

    # This logger spams A LOT
    if not logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

def init_for_tests(with_long_tests: bool = False,
                   with_metrics: bool = False,
                   with_fast_passwords: bool = True,
                   with_integration_tests: bool = False,
                   with_mp_prometheus_default: bool = True):

    # Setup config and logging
    init_pipeman('tests', with_mp_prometheus_default if not with_metrics else False)

    # Prevent metrics from being loaded
    if not with_metrics:
        import medsutil.metrics as metrics
        metrics.DISABLE_METRICS.set()

    # speed up password hashing for tests only!
    if with_fast_passwords:
        import medsutil.secure as s
        s.DEFAULT_PASSWORD_HASH_ITERATIONS = 1
        s.MINIMUM_ITERATIONS = 2

    # skip long tests unless requested to run (there's a lot of them)
    if not with_long_tests:
        import tests.helpers.base_test_case as btc
        btc.SKIP_LONG_TESTS.set()

    # integration tests are long tests that test the interface between two pieces of software
    if not with_integration_tests:
        import tests.helpers.base_test_case as btc
        btc.SKIP_INTEGRATION_TESTS.set()