import datetime
import typing as t

import medsutil.datadict as dd
from medsutil.ocproc2 import ParentRecord
from nodb.interface import NODBInstance
from nodb.observations import DataMode, NODBObservationData
from pipeman.programs.nodb.record_manager import CreationResultType
import nodb.base as s


class ProductRule(dd.DataDictObject):
    ...

    def check(self, obs_data: NODBObservationData, record: ParentRecord, res_type: CreationResultType) -> bool:
        raise NotImplementedError


class CreationResultTypeRule(ProductRule):
    restrict_types: set[CreationResultType] = dd.p_enum_set(CreationResultType, required=True)

    def check(self, obs_data: NODBObservationData, record: ParentRecord, res_type: CreationResultType) -> bool:
        return res_type in self.restrict_types


class DataModeRule(ProductRule):
    data_mode: DataMode = dd.p_enum(DataMode, required=True)

    def check(self, obs_data: NODBObservationData, record: ParentRecord, res_type: CreationResultType) -> bool:
        return obs_data.data_mode is self.data_mode


class QualityCheckRule(ProductRule):
    quality_check: int = dd.p_int(default=0)

    def check(self, obs_data: NODBObservationData, record: ParentRecord, res_type: CreationResultType) -> bool:
        return (obs_data.quality_checks & self.quality_check) > 0


class ProfileParameterRule(ProductRule):
    parameter: str = dd.p_str(required=True)

    def check(self, obs_data: NODBObservationData, record: ParentRecord, res_type: CreationResultType) -> bool:
        if "PROFILE" not in record.subrecords:
            return False
        for rs in record.subrecords.record_sets["PROFILE"].values():
            for record in rs.records:
                if self.parameter in record.parameters:
                    return True
        return False


class SurfaceParameterRule(ProductRule):
    parameter: str = dd.p_str(required=True)

    def check(self, obs_data: NODBObservationData, record: ParentRecord, res_type: CreationResultType) -> bool:
        return self.parameter in record.parameters


class OrRule(ProductRule):
    rules: list[ProductRule] = dd.p_object_list(ProductRule)

    def check(self, obs_data: NODBObservationData, record: ParentRecord, res_type: CreationResultType) -> bool:
        return any(x.check(obs_data, record, res_type) for x in self.rules)


class AndRule(ProductRule):
    rules: list[ProductRule] = dd.p_object_list(ProductRule)

    def check(self, obs_data: NODBObservationData, record: ParentRecord, res_type: CreationResultType) -> bool:
        return all(x.check(obs_data, record, res_type) for x in self.rules)


class ProductDefinition(s.MetadataMixin, s.NODBBaseObject):

    TABLE_NAME = "nodb_product_definitions"

    product_name: str | None = s.StringColumn()
    _product_rule: dict = s.JsonDictColumn(managed_name="product_rule")

    @property
    def product_rule(self) -> ProductRule | None:
        if self._product_rule is None:
            return None
        return ProductRule.from_map(self._product_rule)

    @product_rule.setter
    def product_rule(self, value: ProductRule):
        self._product_rule = value.export()

    @property
    def add_to_db_table(self) -> bool:
        return self.get_metadata("add_to_db_table", False)

    @add_to_db_table.setter
    def add_to_db_table(self, value: bool):
        self.set_metadata("add_to_db_table", bool(value))

    @property
    def queue_name(self) -> str | None:
        queue_name = self.get_metadata("queue_name", None)
        if queue_name:
            return str(queue_name)
        return None

    @queue_name.setter
    def queue_name(self, value: str):
        self.set_metadata("queue_name", str(value) if value else None)

    def observations(self, db: NODBInstance, new_only: bool = True, **kwargs) -> t.Iterable[ProductObservation]:
        yield from ProductObservation.find_by_product(db, self.product_name or '', new_only=new_only, **kwargs)

    @classmethod
    def find_by_name(cls, db: NODBInstance, product_name: str, **kwargs) -> ProductDefinition | None:
        return db.load_object(
            cls,
            filters={"product_name": product_name},
            **kwargs
        )


class ProductObservation(s.NODBBaseObject):
    TABLE_NAME = "nodb_product_observation"
    PRIMARY_KEYS = ("product_name", "obs_uuid", "received_date",)

    product_name: str = s.StringColumn()
    obs_uuid: str = s.StringColumn()
    received_date: datetime.date = s.DateColumn()
    processed: int = s.IntColumn(default=0)

    def load_observation_data(self, db: NODBInstance, **kwargs):
        from nodb.observations import NODBObservationData
        return NODBObservationData.find_by_uuid(db, self.obs_uuid, self.received_date, **kwargs)

    @classmethod
    def find_by_uuid(cls,
                     db: NODBInstance,
                     product_name: str,
                     obs_uuid: str,
                     received_date: str | datetime.date,
                     **kwargs) -> ProductObservation | None:
        return db.load_object(
            cls,
            filters={
                "obs_uuid": obs_uuid,
                "received_date": received_date,
                "product_name": product_name,
            },
            **kwargs
        )

    @classmethod
    def find_by_product(cls,
                        db: NODBInstance,
                        product_name: str,
                        new_only: bool = True,
                        **kwargs) -> t.Iterable[ProductObservation]:
        filters: dict[str, str | int] = {"product_name": product_name}
        if new_only:
            filters["processed"] = 0
        yield from db.stream_objects(cls, filters=filters, **kwargs)




