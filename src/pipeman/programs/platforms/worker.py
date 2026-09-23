from pipeman.processing.scheduled_task import ScheduledTask
from pipeman.programs.platforms.oceanops import download_ocean_ops
from pipeman.programs.platforms.ccgs import download_ccgs_ships
from pipeman.programs.platforms.ices import download_ices_codes


all_code_downloads = [
    download_ocean_ops,
    download_ices_codes,
    download_ccgs_ships,
]


class PlatformUpdateTask(ScheduledTask):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="platform_updater",
            process_version="1.0",
            **kwargs
        )

    def execute(self):
        excs = []
        for f in all_code_downloads:
            try:
                f()
            except Exception as e:
                e.add_note(f"Error occurred while calling {f.__name__}")
                excs.append(e)
        if excs:
            raise ExceptionGroup("Exceptions occurred while processing platform update tasks", excs)
