import medsutil.datadict as dd
from medsutil.ocproc2 import ParentRecord
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


class ProductDefinition(s.NODBBaseObject):

    TABLE_NAME = "nodb_product_definitions"

    _product_rule: dict = s.JsonDictColumn(managed_name="product_rule")

    @property
    def product_rule(self) -> ProductRule | None:
        if self._product_rule is None:
            return None
        return ProductRule.from_map(self._product_rule)

    @product_rule.setter
    def product_rule(self, value: ProductRule):
        self._product_rule = value.export()



