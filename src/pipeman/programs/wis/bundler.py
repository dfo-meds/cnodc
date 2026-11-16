from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from medsutil.storage.interface import FeatureFlag
from nodb.wis import WISIncomingMessage
from pipeman.processing.scheduled_task import ScheduledTask


class WISMessageBundleWorker(ScheduledTask):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="wis_bundler",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            "output_directory": None,
            "maximum_write_size": 1024 * 1024 * 100,
        })

    def execute(self):
        max_size = self.get_config("maximum_write_size", coerce=int, default=1024 * 1024 * 100)
        with self.get_handle(self.get_config("output_directory"), True) as dir_handle:
            if not(dir_handle.exists() and dir_handle.is_dir()):
                raise CodedError("Directory does not exist", 1000, code_space="WIS-BUNDLER")
            temp_file = self.temp_dir() / "bundle"
            current_size = 0
            copied_ids = []
            with self.nodb as db:
                with open(temp_file, "wb") as temp:
                    for idx, unbundled in enumerate(WISIncomingMessage.find_unbundled(db)):
                        temp_unbundle = self.temp_dir() / f"unbundled_{idx}"
                        with self.get_handle(unbundled.path, True) as handle:
                            handle.download(temp_unbundle)
                        with open(temp_unbundle, "rb") as handle:
                            while chunk := handle.read(1024 * 1024):
                                current_size += len(chunk)
                                temp.write(chunk)
                                self.breakpoint()
                        temp.write(b"\n\n")
                        current_size += 2
                        copied_ids.append(unbundled.message_uuid)
                        if current_size >= max_size:
                            break
                file_handle = dir_handle.child(f"gts-{AwareDateTime.utcnow().strftime('%Y%m%d_%H%M%S')}")
                if dir_handle.supports_feature(FeatureFlag.FAST_MOVE):
                    temp_handle = dir_handle.child(".temp-gts")
                    temp_handle.upload(temp_file, allow_overwrite=True)
                    temp_handle.rename(file_handle)
                else:
                    file_handle.upload(temp_file)
