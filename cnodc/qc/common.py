from cnodc.nodb import NODBObservation


class QCSkip(Exception):
    pass


class QCError(Exception):
    pass


class QCReview(Exception):

    def __init__(self, msg, code):
        super().__init__(msg)
        self.code = code


class QCDelay(Exception):
    pass


def qc_test(short_name, long_name):

    def _decorator(x: callable):
        def _inner_decorator(obs: NODBObservation, *args, **kwargs):
            obs.metadata['QC_ERRORS'] = []
            # Don't rerun tests
            if obs.qc_test_complete(short_name):
                raise QCSkip(f"Observation {obs.pkey} has already completed test {short_name}")
            try:
                # Call the test function (which raises an exception if it doesn't pass)
                x(*args, **kwargs)
                # Mark it complete
                obs.mark_test_complete(short_name)
            except QCReview as ex:
                if ex.code:
                    obs.add_qc_error_for_review(ex.code)
                raise ex
            except QCSkip as ex:
                obs.mark_test_complete(short_name)
        return _inner_decorator
    return _decorator
