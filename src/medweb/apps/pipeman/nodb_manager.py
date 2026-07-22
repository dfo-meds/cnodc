from autoinject import injector

@injector.injectable
class NODBController:

    def __init__(self):
        ...

    def has_queue_access(self, queue_name) -> bool:
        ...

