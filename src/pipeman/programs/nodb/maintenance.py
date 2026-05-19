from pipeman.processing.scheduled_task import ScheduledTask


class NODBMaintenanceTask(ScheduledTask):

    def __init__(self, **kwargs):
        super().__init__(
            process_name='nodb_maintenance_runner',
            process_version='1.0',
            **kwargs
        )
        self.set_defaults({
            'run_on_boot': True,
        })

    def execute(self):
        with self.nodb as db:
            db.run_maintenance()
