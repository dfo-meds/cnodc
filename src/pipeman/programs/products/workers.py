import datetime
import typing as t

from medsutil.ocproc2 import ParentRecord
from nodb.interface import NODBInstance
from nodb.observations import NODBObservationData
from nodb.products import ProductDefinition, ProductObservation
from pipeman.processing.payload_worker import NewObservationsWorkflowWorker
from pipeman.processing.payloads import NewObservationsPayload, ObservationPayload
from pipeman.processing.queue_worker import QueueItemResult


class ProductTriggerWorker(NewObservationsWorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="product_trigger",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'product_trigger',
            'next_queue': 'workflow_continue',
            'error_queue': 'product_trigger_errors',
        })

    def process_payload(self, payload: NewObservationsPayload) -> t.Optional[QueueItemResult]:
        starter = ProductStarter(self.db)
        successes: list[tuple] = []
        failures: list[tuple] = []
        products: list[ProductDefinition] = [x for x in ProductDefinition.find_all(self.db)]
        for obs_uuid, obs_date, crt in payload.stream_observation_info():
            obs_data = NODBObservationData.find_by_uuid(self.db, obs_uuid, obs_date)
            if obs_data is None:
                failures.append((obs_uuid, obs_date, crt.value))
                continue
            successes.append((obs_uuid, obs_date, crt.value))
            for product in products:
                rule = product.product_rule
                if rule and rule.check(obs_data, t.cast(ParentRecord, obs_data.record), crt):
                    starter.start_product_build(product, obs_uuid, datetime.date.fromisoformat(obs_date), payload)
        if failures:
            self.prevent_default_progression()
            payload1 = NewObservationsPayload(observations=failures)
            payload1.enqueue(self.db, self.get_config("error_queue", "product_trigger_errors"))
            payload2 = NewObservationsPayload(observations=successes)
            payload2.enqueue(self.db, self.get_config("next_queue", "workflow_continue"))


class ProductStarter:

    def __init__(self, db: NODBInstance) -> None:
        self.db = db

    def start_product_build(self,
                            product: ProductDefinition,
                            obs_uuid: str,
                            obs_date: datetime.date,
                            payload: NewObservationsPayload):
        queue_name: str | None = product.queue_name
        if queue_name:
            obs_payload = ObservationPayload(obs_uuid=obs_uuid, received_date=obs_date)
            obs_payload.copy_details_from(payload, False)
            obs_payload.enqueue(self.db, queue_name)
        if product.add_to_db_table:
            existing = ProductObservation.find_by_uuid(self.db, product.product_name or '', obs_uuid, obs_date)
            if existing is None:
                prod_obs = ProductObservation(
                    product_name=product.product_name,
                    obs_uuid=obs_uuid,
                    received_date=obs_date,
                    processed=0
                )
                self.db.insert_object(prod_obs)
