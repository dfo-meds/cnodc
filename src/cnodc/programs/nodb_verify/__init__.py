


"""



    def _extract_float_in_units(self, v: AbstractValue, unit_str: str = None) -> t.Optional[float]:
        if v is None:
            return None
        values = []
        work = [v]
        while work:
            val = work.pop()
            if isinstance(val, MultiValue):
                work.extend(val)
            else:
                if val.value is None:
                    continue
                try:
                    if unit_str is None or 'Units' not in val.metadata:
                        values.append(float(val.value))
                    else:
                        values.append(self.converter.convert(float(val.value), val.metadata['Unit'].value, unit_str))
                except ValueError as ex:
                    self.log.exception(f"Error converting value to float")
        if not values:
            return None
        elif len(values) == 1:
            return values[0]
        else:
            return statistics.mean(values)

    def _extract_iso_time(self, v: AbstractValue) -> t.Optional[datetime.datetime]:
        if v is None:
            return None
        values = []
        work = [v]
        while work:
            val = work.pop()
            if isinstance(val, MultiValue):
                work.extend(val)
            else:
                if val.value is None or val.value == "":
                    continue
                try:
                    values.append(datetime.datetime.fromisoformat(val.value))
                except ValueError:
                    self.log.exception(f"Invalid date/time value {val.value}")
        if not values:
            return None
        elif len(values) == 1:
            return values[0]
        else:
            return datetime.datetime.fromtimestamp(statistics.mean([x.timestamp() for x in values]))

    def _populate_observation(self, obs: structures.NODBObservation, record: DataRecord):
        obs.surface_parameters = list(record.parameters.keys()) if record.parameters else None
        obs.profile_parameters = None
        if 'Latitude' not in record.coordinates or 'Longitude' not in record.coordinates or 'Time' not in record.coordinates:
            obs.observation_type = structures.ObservationType.OTHER
        elif 'PROFILE' in record.subrecords:
            obs.obs_time = self._extract_iso_time(record.coordinates['Time'])
            obs.location = f"POINT ({self._extract_float_in_units(record.coordinates['Longitude'])} {self._extract_float_in_units(record.coordinates['Latitude'])})"
            obs.observation_type = structures.ObservationType.PROFILE
            obs.min_depth = None
            obs.max_depth = None
            profile_parameters = set()
            for subrecord in record.iter_subrecords("PROFILE"):
                sr_depth = None
                profile_parameters.update(subrecord.parameters.keys())
                if "Depth" in subrecord.coordinates:
                    sr_depth = self._extract_float_in_units(subrecord.coordinates['Depth'], 'm')
                elif 'Pressure' in subrecord.coordinates:
                    # TODO: should we consider a depth measurement conversion from pressure?
                    pass
                if sr_depth is None:
                    continue
                elif obs.min_depth is None or sr_depth < obs.min_depth:
                    obs.min_depth = sr_depth
                elif obs.max_depth is None or sr_depth > obs.max_depth:
                    obs.max_depth = sr_depth
            obs.profile_parameters = list(profile_parameters)
        elif 'Depth' in record.coordinates:
            obs.observation_type = structures.ObservationType.AT_DEPTH
            obs.min_depth = self._extract_float_in_units(record.coordinates['DEPTH'], 'm')
            obs.max_depth = obs.min_depth
        elif 'Pressure' in record.coordinates:
            obs.observation_type = structures.ObservationType.AT_DEPTH
            # TODO: should we consider a depth measurement conversion from pressure?
        else:
            obs.observation_type = structures.ObservationType.SURFACE
            obs.min_depth = 0
            obs.max_depth = 0
        if 'CNODCStation' in record.metadata:
            obs.station_uuid = record.metadata['CNODCStation'].value
        if (obs.station_uuid is None or obs.station_uuid == '') and 'station_uuid' in self._defaults:
            obs.station_uuid = self._defaults['station_uuid']
        if 'CNODCMission' in record.metadata:
            obs.mission_name = record.metadata['CNODCMission'].value
        if (obs.mission_name is None or obs.mission_name == '') and 'mission_name' in self._defaults:
            obs.mission_name = self._defaults['mission_name']
        if 'CNODCSource' in record.metadata:
            obs.source_name = record.metadata['CNODCSource'].value
        if (obs.source_name is None or obs.source_name == '') and 'source_name' in self._defaults:
            obs.source_name = self._defaults['source_name']
        if 'CNODCInstrumentType' in record.metadata:
            obs.instrument_type = record.metadata['CNODCInstrumentType'].value
        if (obs.instrument_type is None or obs.instrument_type == '') and 'instrument_type' in self._defaults:
            obs.instrument_type = self._defaults['instrument_type']
        if 'CNODCProgram' in record.metadata:
            obs.program_name = record.metadata['CNODCProgram'].value
        if (obs.program_name is None or obs.program_name == '') and 'program_name' in self._defaults:
            obs.program_name = self._defaults['program_name']
        if 'CNODCStatus' in record.metadata:
            try:
                obs.status = structures.ObservationStatus(record.metadata['CNODCStatus'].value)
            except ValueError as ex:
                self.log.warning(f"Ignoring invalid observation status [{record.metadata['CNODCStatus'].value}]")
        if obs.status is None and 'status' in self._defaults:
            obs.status = self._defaults['status']
        if 'CNODCLevel' in record.metadata:
            try:
                obs.processing_level = structures.ObservationStatus(record.metadata['CNODCLevel'].value)
            except ValueError as ex:
                self.log.warning(f"Ignoring invalid processing level [{record.metadata['CNODCLevel'].value}")
        if obs.processing_level is None and 'processing_level' in self._defaults:
            obs.processing_level = self._defaults['processing_level']
        if 'CNODCEmbargoUntil' in record.metadata:
            try:
                obs.embargo_date = datetime.datetime.fromisoformat(record.metadata['CNODCEmbargoUntil'].value)
            except ValueError as ex:
                self.log.warning(f"Ignoring invalid embargo date [{record.metadata['CNODCEmbargoUntil'].value}")
        if obs.embargo_date is None and 'embargo_date' in self._defaults:
            obs.embargo_date = self._defaults['embargo_date']

"""