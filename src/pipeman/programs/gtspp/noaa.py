import typing as t
from autoinject import injector

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import ParentRecord
from medsutil.ocproc2.codecs.ocproc import MEDSASCIICodec
from medsutil.storage import StorageController
from nodb.products import ProductDefinition
from pipeman.processing.scheduled_task import ScheduledTask


class NOAAOutputBuildWorker(ScheduledTask):

    storage: StorageController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name="gtpss_noaa_output",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            "product_name": "gtspp_noaa",
            "noaa_directory": None,
        })

    def load_product(self, db) -> ProductDefinition:
        prod = ProductDefinition.find_by_name(db, self.get_config("product_name"))
        if prod is not None:
            return prod
        raise ValueError(f"missing product [{self.get_config("product_name")}]")

    def execute(self):
        with self.storage.handle(self.get_config("noaa_directory", None), halt_flag=self._halt_flag) as handle:
            handle.mkdir(parents=True)
            if not (handle.exists() and handle.is_dir()):
                raise ValueError("Invalid directory handle")
            with self.nodb as db:
                product = self.load_product(db)
                observations = [x for x in product.observations(db, True)]
                if observations:
                    next_file_name = product.get_metadata("next_file_name", None)
                    if next_file_name is None:
                        next_file_name = f"gtpss_{AwareDateTime.now().strftime('%Y%m%d%H%M')}.meds"
                        product.set_metadata("next_file_name", next_file_name)
                        db.update_object(product)
                        db.commit()
                    def _iterator() -> t.Iterable[ParentRecord]:
                        for prod_obs in observations:
                            obs_data = prod_obs.load_observation_data(db)
                            if obs_data is not None:
                                prod_obs.processed = 1
                                yield obs_data.record
                    temp_dir = self.temp_dir()
                    temp_file = temp_dir / "noaa_output.meds"
                    codec = MEDSASCIICodec(halt_flag=self._halt_flag)
                    codec.dump(temp_file, _iterator())
                    real_remote_file = handle / next_file_name
                    self.breakpoint()
                    try:
                        real_remote_file.upload(temp_file, allow_overwrite=True)
                        for obs in observations:
                            if obs.modified_values:
                                db.update_object(obs)
                        product.set_metadata("next_file_name", None)
                        db.update_object(product)
                        db.commit()
                        real_remote_file = None
                    except Exception:
                        if real_remote_file is not None:
                            real_remote_file.remove()
                        raise




