import itertools
import sys

from medsutil import ocproc2
from medsutil.ocproc2 import SingleElement, MultiElement
from medsutil.ocproc2.refs import ElementType
from pipeman.programs.qc.integrity import NODBIntegrityChecker
from tests.helpers.base_test_case import sub_tests
from tests.helpers.qc_check_base import QCCheckerTestCase


##
## LAST UPDATED FROM elements.csv ON 2026-06-23
##

VALID_UNIT_GROUPS = [
    # Dimensionless
    ("1", "0.1", "0.01", "0.001", "psu", "%", "1e-3", "1e-6", "1e-9", "ppt", "ppm", "ppb"),

    # Mass per mass
    ("g kg-1", "kg kg-1", "kg/kg", "g/kg"),

    # Direction
    ("degree", "arc_degree", "degrees", "arc_degrees"),
    # Coordinates
    ("degree_east", "degrees_east"),
    ("degree_north", "degrees_north"),

    # Frequency (per mass)
    ("Hz", "s-1", "1/s"),

    # Insolation (energy per area)
    ("J m-2", "kJ m-2"),

    # Temperature
    ("K", "degree_C", "degrees_C", "°C", "°F", "degree_F", "degrees_F"),

    # mass per volume
    ("kg L-1", "kg m-3", "mg L-1", "mg m-3", "g cm-3"),

    # mass per area
    ("kg m-2",),

    # length
    ("m", "cm", "nm"),

    # length per time
    ("m s-1", "kts", "m/s"),

    # per length
    ("m-1", "1/m"),

    # area per frequency
    ("m2 Hz-1", "m2 s", "m2/Hz"),

    # area per frequency per radian
    ("m2 s rad-1", "m2 Hz-1 rad-1"),

    # amount per volume
    ("mmol m-3", "umol m-3", "mmol L-1", "umol L-1", "mmol/L", "umol/L"),

    # pressure
    ("Pa", "dbar", "hPa", "kPa"),

    # time
    ("s", "min", "h"),

    # conductivity
    ("S m-1", "S/m"),

    # amount per mass
    ("umol kg-1", "mmol kg-1", "umol g-1", "mmol g-1", "umol/kg", "mmol/kg", "umol/g", "mmol/g"),

    # voltage
    ("V", "mV", "kV"),

    # wattage per area
    ("W m-2",)

]

GARBAGE_UNITS = [
    "foobar",
    "#!$%!@#!",
]

COMPATIBLE_UNITS = []
for sublist in VALID_UNIT_GROUPS:
    COMPATIBLE_UNITS.extend(itertools.product(sublist, repeat=2))

INCOMPATIBLE_UNITS = []
for idx_a, sublist_a in enumerate(VALID_UNIT_GROUPS):
    INCOMPATIBLE_UNITS.extend(itertools.product(GARBAGE_UNITS, sublist_a))
    INCOMPATIBLE_UNITS.extend(itertools.product(sublist_a, GARBAGE_UNITS))
    for idx_b, sublist_b in enumerate(VALID_UNIT_GROUPS):
        if idx_a == idx_b:
            continue
        if idx_a in (0, 1) and idx_b in (0, 1):  # dimensionless and mass/mass are technically compatible
            continue
        if idx_a in (2, 3, 4) and idx_b in (2, 3, 4): # degrees and degrees_N and degrees_E are technically compatible
            continue
        INCOMPATIBLE_UNITS.extend(itertools.product(sublist_a, sublist_b))

GOOD_DATA_TYPES = {
    'datetimestamp': [
        '2015-01-02',
        '2015-01-02T03',
        '2015-02-03T03:04',
        '2015-02-03T03:04:05',
        '2015-01-02T03+00:00',
        '2015-02-03T03:04+00:00',
        '2015-02-03T03:04:05+00:00',
    ],
    'date': [
        '2015-01-02',
        '2015-01-02T03',
        '2015-02-03T03:04',
        '2015-02-03T03:04:05',
        '2015-01-02T03+00:00',
        '2015-02-03T03:04+00:00',
        '2015-02-03T03:04:05+00:00',
    ],
    'integer': [
        0, 1, "0", "1", "-5", -5, 1e9, "1e9",
    ],
    "decimal": [
        0, 1, "0", "1", "-5", -5, 1e9, "1e9",
        "0.0", "1.0", "1.5", 1.5, 1.0, "1e-9", "1.23e-9",
    ],
    "string": [
        0, 1, "0", "1", "-5", -5, 1e9, "1e9",
        "0.0", "1.0", "1.5", 1.5, 1.0, "1e-9", "1.23e-9",
        '2015-01-02',
        '2015-01-02T03',
        '2015-02-03T03:04',
        '2015-02-03T03:04:05',
        '2015-01-02T03+00:00',
        '2015-02-03T03:04+00:00',
        '2015-02-03T03:04:05+00:00',
        "foobar", "model", "major", "GENERAL",
        "",
        "PT3H", "PT3M", "PT3S",
        "P3D", "P3W", "PT-1H", "PT-10M",
    ],
    "list": [
        [0, 1, 2],
        ["0", 1, "2.2"],
    ],
    "duration": [
        "PT3H", "PT3M", "PT3S",
        "P3D", "P3W", "PT-1H", "PT-10M",
    ],
}

BAD_DATA_TYPES = {
    'datetimestamp': [
        0, 1, "0", "1", "-5", -5, 1e9, "1e9",
        "0.0", "1.0", "1.5", 1.5, 1.0, "1e-9", "1.23e-9", "", "foobar",
        "PT3H", "PT3M", "PT3S",
        "P3D", "P3W", "PT-1H", "PT-10M",
        ["1", "2", "3"],
    ],
    'date': [
        0, 1, "0", "1", "-5", -5, 1e9, "1e9",
        "0.0", "1.0", "1.5", 1.5, 1.0, "1e-9", "1.23e-9", "", "foobar",
        "PT3H", "PT3M", "PT3S",
        "P3D", "P3W", "PT-1H", "PT-10M",
        ["1", "2", "3"],
    ],
    'integer': [
        "", "-1.2", "1.0004", "foobar",
        "PT3H", "PT3M", "PT3S",
        "P3D", "P3W", "PT-1H", "PT-10M",
        '2015-01-02',
        '2015-01-02T03',
        '2015-02-03T03:04',
        '2015-02-03T03:04:05',
        '2015-01-02T03+00:00',
        '2015-02-03T03:04+00:00',
        '2015-02-03T03:04:05+00:00',
        ["1", "2", "3"],
    ],
    "decimal": [
        "", "P1H", "P3.12H",
        "PT3H", "PT3M", "PT3S",
        "P3D", "P3W", "PT-1H", "PT-10M",
        '2015-01-02',
        '2015-01-02T03',
        '2015-02-03T03:04',
        '2015-02-03T03:04:05',
        '2015-01-02T03+00:00',
        '2015-02-03T03:04+00:00',
        '2015-02-03T03:04:05+00:00',
        ["1", "2", "3"],
    ],
    "string": [
        ["1", "2", "3"],
    ],
    "list": [
        0, 1, "0", "1", "-5", -5, 1e9, "1e9",
        "0.0", "1.0", "1.5", 1.5, 1.0, "1e-9", "1.23e-9",
        '2015-01-02',
        '2015-01-02T03',
        '2015-02-03T03:04',
        '2015-02-03T03:04:05',
        '2015-01-02T03+00:00',
        '2015-02-03T03:04+00:00',
        '2015-02-03T03:04:05+00:00',
        "foobar", "model", "major", "GENERAL",
        "",
        "PT3H", "PT3M", "PT3S",
        "P3D", "P3W", "PT-1H", "PT-10M",
    ],
    "duration": [
        0, 1, "0", "1", "-5", -5, 1e9, "1e9",
        "0.0", "1.0", "1.5", 1.5, 1.0, "1e-9", "1.23e-9",
        '2015-01-02',
        '2015-01-02T03',
        '2015-02-03T03:04',
        '2015-02-03T03:04:05',
        '2015-01-02T03+00:00',
        '2015-02-03T03:04+00:00',
        '2015-02-03T03:04:05+00:00',
        "foobar", "model", "major", "GENERAL",
        "",
        ["1", "2", "3"],
    ],
    None: [
        0, 1, "0", "1", "-5", -5, 1e9, "1e9",
        "0.0", "1.0", "1.5", 1.5, 1.0, "1e-9", "1.23e-9",
        '2015-01-02',
        '2015-01-02T03',
        '2015-02-03T03:04',
        '2015-02-03T03:04:05',
        '2015-01-02T03+00:00',
        '2015-02-03T03:04+00:00',
        '2015-02-03T03:04:05+00:00',
        "foobar", "model", "major", "GENERAL",
        "",
        ["1", "2", "3"],
        "PT3H", "PT3M", "PT3S",
        "P3D", "P3W", "PT-1H", "PT-10M",
    ]
}

GOOD_DATA_TYPES_EXPANDED = []
for dtype, good_options in GOOD_DATA_TYPES.items():
    GOOD_DATA_TYPES_EXPANDED.extend(
        (dtype, option)
        for option in good_options
    )


BAD_DATA_TYPES_EXPANDED = []
for dtype, bad_options in BAD_DATA_TYPES.items():
    BAD_DATA_TYPES_EXPANDED.extend(
        (dtype, option)
        for option in bad_options
    )


VALID_RANGE_CHECKS: list[tuple[int | float | str, str | None, int | float | None, int | float | None, str | None]] = [
    (0, "m", 0, None, "m"),
    (0 + sys.float_info.epsilon, "m", 0, None, "m"),
    (0 - sys.float_info.epsilon, "m", 0, None, "m"),
    (5, "m", 0, None, "m"),
    (25, "m", 0, None, "m"),
    (50112, "m", 0, None, "m"),
    (5e99, "m", 0, None, "m"),
    (4.2, "m", 0, None, "m"),
    (3.14159, "m", 0, None, "m"),
    (0, "m", 0, 5, "m"),
    (1, "m", 0, 5, "m"),
    (2, "m", 0, 5, "m"),
    (3, "m", 0, 5, "m"),
    (4, "m", 0, 5, "m"),
    (5, "m", 0, 5, "m"),
    (5 + sys.float_info.epsilon, "m", 0, 5, "m"),
    (5 - sys.float_info.epsilon, "m", 0, 5, "m"),
    (5 + 4e-9, "m", 0, 5, "m"),
    (400, "cm", 0, 5, "m"),   
    (0, None, 0, 5, None),
    (1, None, 0, 5, None),
    (2, None, 0, 5, None),
    (3, None, 0, 5, None),
    (4, None, 0, 5, None),
    (5, None, 0, 5, None),
]

INVALID_RANGE_CHECKS: list[tuple[int | float | str, str | None, int | float | None, int | float | None, str | None]] = [
    (5.0 + 6e-9, "m", 0, 5, "m"),
    (0 - 1e-14, "m", 0, 5, "m"),
]

GOOD_ALLOWED_CHECKS = [
    (5, (0,1,2,3,4,5), "integer"),
    (0, (0,1,2,3,4,5), "integer"),
    (1, (0,1,2,3,4,5), "integer"),
    (2, (0,1,2,3,4,5), "integer"),
    (3, (0,1,2,3,4,5), "integer"),
    (4, (0,1,2,3,4,5), "integer"),
    ("5", (0,1,2,3,4,5), "integer"),
    ("0", (0,1,2,3,4,5), "integer"),
    ("1", (0,1,2,3,4,5), "integer"),
    ("2", (0,1,2,3,4,5), "integer"),
    ("3", (0,1,2,3,4,5), "integer"),
    ("4", (0,1,2,3,4,5), "integer"),
    ("foo", ("foo", "bar"), "string"),
    ("bar", ("foo", "bar"), "string"),
]

BAD_ALLOWED_CHECKS = [
    (6, (1,2,3,4,5), "integer"),
    ("6", (1,2,3,4,5), "integer"),
    (0, (1,2,3,4,5), "integer"),
    ("0", (1,2,3,4,5), "integer"),
    ("0", ("foo", "bar"), "string"),
    ("zazz", ("foo", "bar"), "string"),
    ("fo", ("foo", "bar"), "string"),
    ("ar", ("foo", "bar"), "string"),
    ("f", ("foo", "bar"), "string"),
    (5.1, (1.1, 2.2, 5.1), "decimal"),
    ("2015-01-02", ("2015-01-02", "2015-02-03",), "date"),
    ("2015-01-02", ("2015-01-02", "2015-02-03",), "datetime"),
    ("PT3H", ("PT3H", "PT3S"), "duration"),
    ([5], ([5], [6]), "list"),
]


GOOD_RS_TYPES = ["TIME_SERIES", "PROFILE", "SPECTRAL_WAVE", "WAVE_SENSORS", "TRAJECTORY"]
BAD_RS_TYPES = ["PIZZA", "MODERN_MAJOR_GENERAL", "TWELVE_MONKIES", "HELLO_WORLD", "FOOBAR", "", None, 0, 5.1,]

RS_GOOD_COORDINATES = [
    ("TIME_SERIES", {
        "Time": "2015-01-02T00:00:00"
    }),
    ("TIME_SERIES", {
        "TimeOffset": SingleElement("5", Units="seconds")
    }),
    ("PROFILE", {
        "Depth": SingleElement("5", Units="m")
    }),
    ("PROFILE", {
        "Pressure": SingleElement("5", Units="Pa")
    }),
    ("WAVE_SENSORS", {
        "WaveSensor": 1
    }),
    ("SPECTRAL_WAVE", {
        "CentralFrequency": SingleElement("5", Units="Hz")
    }),
    ("TRAJECTORY", {
        "Latitude": SingleElement("5", Units="degrees_north"),
        "Longitude": SingleElement("5", Units="degrees_east"),
        "Time": SingleElement("2015-01-02T00:00:00"),
    }),
    ("TRAJECTORY", {
        "Latitude": SingleElement("5", Units="degrees_north"),
        "Longitude": SingleElement("5", Units="degrees_east"),
        "TimeOffset": SingleElement("5", Units="seconds"),
    }),
]

RS_BAD_COORDINATES = [
    ("TIME_SERIES", {
        "Latitude": SingleElement("5", Units="degrees_north"),
        "Longitude": SingleElement("5", Units="degrees_east"),
    }),
    ("TIME_SERIES", {
        "CentralFrequency": SingleElement("5", Units="Hz")
    }),
    ("TIME_SERIES", {
        "WaveSensor": 1
    }),
    ("TIME_SERIES", {
        "Pressure": SingleElement("5", Units="Pa")
    }),
    ("TIME_SERIES", {
        "Depth": SingleElement("5", Units="m")
    }),
    ("PROFILE", {
        "Latitude": SingleElement("5", Units="degrees_north"),
        "Longitude": SingleElement("5", Units="degrees_east"),
    }),
    ("PROFILE", {
        "CentralFrequency": SingleElement("5", Units="Hz")
    }),
    ("PROFILE", {
        "WaveSensor": 1
    }),
    ("PROFILE", {
        "TimeOffset": SingleElement("5", Units="seconds"),
    }),
    ("PROFILE", {
        "Time": SingleElement("2015-01-02T00:00:00"),
    }),
    ("WAVE_SENSORS", {
        "Time": SingleElement("2015-01-02T00:00:00"),
    }),
    ("WAVE_SENSORS", {
        "TimeOffset": SingleElement("5", Units="seconds"),
    }),
    ("WAVE_SENSORS", {
        "Latitude": SingleElement("5", Units="degrees_north"),
        "Longitude": SingleElement("5", Units="degrees_east"),
    }),
    ("WAVE_SENSORS", {
        "Depth": SingleElement("5", Units="m")
    }),
    ("WAVE_SENSORS", {
        "Pressure": SingleElement("5", Units="Pa")
    }),
    ("WAVE_SENSORS", {
        "CentralFrequency": SingleElement("5", Units="Hz")
    }),
    ("SPECTRAL_WAVE", {
        "Time": SingleElement("2015-01-02T00:00:00"),
    }),
    ("SPECTRAL_WAVE", {
        "TimeOffset": SingleElement("5", Units="seconds"),
    }),
    ("SPECTRAL_WAVE", {
        "Latitude": SingleElement("5", Units="degrees_north"),
        "Longitude": SingleElement("5", Units="degrees_east"),
    }),
    ("SPECTRAL_WAVE", {
        "Depth": SingleElement("5", Units="m")
    }),
    ("SPECTRAL_WAVE", {
        "Pressure": SingleElement("5", Units="Pa")
    }),
    ("SPECTRAL_WAVE", {
        "WaveSensor": 1
    }),
    ("TRAJECTORY", {
        "WaveSensor": 1
    }),
    ("TRAJECTORY", {
        "Pressure": SingleElement("5", Units="Pa")
    }),
    ("TRAJECTORY", {
        "Depth": SingleElement("5", Units="m")
    }),
    ("TRAJECTORY", {
        "Time": SingleElement("2015-01-02T00:00:00"),
    }),
    ("TRAJECTORY", {
        "TimeOffset": SingleElement("5", Units="seconds"),
    }),
    ("TRAJECTORY", {
        "Latitude": SingleElement("5", Units="degrees_north"),
        "Longitude": SingleElement("5", Units="degrees_east"),
    }),
    ("TRAJECTORY", {
        "Longitude": SingleElement("5", Units="degrees_east"),
    }),
    ("TRAJECTORY", {
        "Latitude": SingleElement("5", Units="degrees_north"),
    }),
]
ET = ElementType
ELEMENT_INFO: dict[ElementType, list[str]] = {
    ET.COORDINATES: [
        "Time",
        "Longitude",
        "Latitude",
        "CentralFrequency",
        "Depth",
        "Pressure",
        "TimeOffset",
        "ObservationNumber",
        "WaveSensor",
    ],
    ET.PARAMETERS: [
        x.strip() for x in """AerosolDryAirFraction
            CarbonDioxideDryAirFraction
            CarbonMonoxideDryAirFraction
            CloudAerosolDryAirFraction
            CloudDryAirFraction
            DustDryAirFraction
            FormaldehydeDryAirFraction
            MethaneDryAirFraction
            NitrogenDioxideDryAirFraction
            NitrousOxideDryAirFraction
            OzoneDryAirFraction
            ParticulateMatterDryAirFraction
            ParticulateMatterMaximumSize
            SmokeDryAirFraction
            SulphurDioxideDryAirFraction
            VolcanicAshDryAirFraction
            VolcanicSulphurDioxideDryAirFraction
            WaterDryAirFraction
            PracticalSalinity
            FluorescenceFraction
            LightTransmission
            RelativeHumidity
            SpectralWaveDensityRatio
            ChlorophyllAFluorescence
            PH
            Turbidity
            ColoredDissolvedOrganicMatter
            CurrentDirection
            WaveDirection
            WaveDominantDirection
            WaveDominantSpread
            WaveMeanDirection
            WavePrincipalDirection
            WaveSpread
            WindDirection
            WindGustMaxDirection
            AbsoluteSalinity
            DissolvedOxygenFrequency
            DiffuseSolarRadiation
            DirectSolarRadiation
            GlobalSolarRadiation
            LongwaveRadiation
            NetRadiation
            ShortwaveRadiation
            AirSkinTemperature
            AirTemperature
            DewPointTemperature
            Temperature
            WetBulbTemperature
            Fluorescence
            TotalPrecipitation
            Density
            PotentialDensity
            HorizontalVisibility
            IceThickness
            SeaDepth
            WaveHeight
            WaveMaximumHeight
            WaveSignificantHeight
            CurrentSpeed
            CurrentSpeedEast
            CurrentSpeedNorth
            SoundVelocity
            WindGustMaxSpeed
            WindSpeed
            ParticleBackscatter
            SpectralWaveDensity
            SpectralWaveDensityByFrequency
            SpectralWaveMaximumDensity
            SpectralWaveDirDensityByFrequency
            Fluoride
            ParticulateCarbon
            ChlorophyllA
            ChlorophyllB
            ChlorophyllC
            Pheophytin
            TotalNitrogenMolar
            TotalPhosphorousMolar
            AirPressure
            AirPressureAtSeaLevel
            AirPressureChange
            CarbonDioxidePartialPressure
            SpectralWavePeakPeriod
            WaveAveragePeriod
            WavePeriod
            Conductivity
            SpecificConductance
            SpectralWaveBandWidth
            DissolvedOxygen
            Nitrate
            AlkalinityMolar
            AmmoniaMolar
            CarbonateAlkalinityMolar
            NitrateMolar
            NitriteMolar
            PhosphateMolar
            SilicateMolar
            DissolvedOxygenMolar
            FluorescencePotential
            LongwaveRadiationInstant
            ShortwaveRadiationInstant
            FourierA1
            FourierA2
            FourierB1
            FourierB2
            FourierK
            SpectralWaveFourier1
            SpectralWaveFourier2
            SpectralWaveMaximumDensityBand
            SpectralWaveBandCount
            WMOAirPressureCharacteristic
            WMOSeaIceConcentration
            WMOSeaState""".split("\n")
    ],
    ET.PARENT_METADATA: [
        x.strip() for x in """MissionID
            EndDate
            StartDate
            EndLongitude
            StartLongitude
            EndLatitude
            StartLatitude
            PlatformMissionNumber
            Abstract
            Area
            CNODCCruiseID
            CNODCDeploymentMissionID
            CNODCDeploymentPlatformID
            CruiseName
            DeploymentCruiseID
            LeadInstitution
            MissionInformationLink
            Network
            Notes
            Observatory
            OperatingInstitution
            PlatformFinalStatus
            PrincipalInvestigator
            Program
            Project
            ReferenceStations
            Summary
            Title
            CNODCDuplicateDate
            CNODCWorkingDuplicateDate
            CNODCEmbargoUntil
            CNODCPlatformCandidates
            CNODCDuplicateID
            CNODCID
            CNODCLevel
            CNODCMission
            CNODCOperatorAction
            CNODCPlatform
            CNODCProgram
            CNODCSource
            CNODCStatus
            CNODCWorkingDuplicateID
            GTSHeader
            BUFRMessageTime
            BUFRDataCategory
            BUFRIsObservation
            BUFROriginCentre
            BUFROriginSubcentre
            BUFRSubsetIndex
            BUFRDescriptors
            BUFRInferredMessageType
            BUFRPlatformType
            PlatformServiceEnd
            PlatformServiceStart
            PlatformMaximumDepth
            IMONumber
            WMOAgencyCode
            BatteryDescription
            BatteryType
            DocumentationVersion
            FieldReferences
            FirmwareType
            FirmwareVersion
            PlatformCategory
            PlatformCNODCType
            PlatformCustomization
            PlatformDetails
            PlatformID
            PlatformMake
            PlatformModel
            PlatformName
            PlatformOwner
            PlatformSerial
            ShipC
            WIGOSID
            WMOID
            CreationTime
            DataUpdateInterval
            DOI""".split("\n")
    ],
    ET.CHILD_METADATA: [
        x.strip() for x in """InstrumentManufacturingDate
            XBTHeight
            SatelliteIdentifier
            WMOCommunicationSystem
            WMODrogueStatus
            WMODrogueType
            WMOPlatformType
            WMOQualityLocation
            WMOQualityLocationClass
            WMOQualitySatellite
            WMOStationType
            WMOXBTType
            AlternativeSolutionLatitude
            AlternativeSolutionLongitude
            BuoyEngineeringStatus
            DrogueCableLength
            HydrostaticPressure
            PlatformLastKnownDirection
            PlatformLastKnownSpeed
            ThermistorCableLength
            WMOBuoyType
            WMODataBuoyType
            WMOQualityAirTemperature
            WMOQualityHousekeeping
            WMOQualityPressure
            WMOQualityWaterTemperature
            WMOSurfaceType
            WMOLandStationID
            LastKnownPositionTime
            SubmergenceTime
            PlatformDirection
            PlatformPitch
            PlatformRoll
            PlatformTrueHeading
            DrogueDepth
            PlatformHeight
            PlatformSpeed
            BatteryVoltage
            GliderPhaseCode
            GliderPhaseNumber
            ProfileNumber
            SegmentNumber
            ShipLineNumber
            ShipObservationNumber
            ShipTransectNumber
            WMOSurfaceStationType
            ProfileID
            SamplingScheme
            SoftwareID
            TransmitterID""".split("\n")
    ],
    ET.ELEMENT_METADATA: [
        x.strip() for x in """WMOCurrentMeasurementDuration
            WMOCurrentMeasurementMethod
            WMODepthMethod
            WMODissolvedOxygenSensorType
            WMOPlatformMotionRemovalMethod
            WMOSalinityDepthMeasurementMethod
            WMOSpectralWaveMethod
            WMOTemperatureSalinityMeasurementMethod
            WMOTemperatureSalinityQualifier
            WMOWetBulbMeasurementMethod
            WMOWindInstrumentType
            WMOProfileInstrumentType
            WMOProfileRecorderType
            WMOAnemometerType
            WMOHSPCorrected
            WMOWindSource
            SensorManufactureDate
            CalibrationDate
            DerivationDate
            SensorFrequency
            SensorTemperature
            SensorDepth
            SamplingFrequency
            CalibrationCoefficient
            DerivationCoefficient
            SensorAccuracy
            SensorResolution
            Uncertainty
            ObservationPeriod
            Unadjusted
            BackscatterWavelength
            ObservationPeriodMaximum
            ObservationPeriodMinimum
            Quality
            SensorRank
            WMOAveragingPeriod
            WorkingQuality
            AggregationMethod
            AnemometerType
            CalibrationComment
            CalibrationEquation
            CurrentSensorType
            DerivationComment
            DerivationEquation
            GliderPhaseCodeSource
            Language
            NavigationSatelliteType
            SensorDepthReference
            SensorLocation
            SensorMake
            SensorModel
            SensorOrientation
            SensorSerial
            SensorType
            SpectralWaveDataType
            TemperatureScale
            UncertaintyType
            Units
            VariableName""".split("\n")
    ],
    ET.RECORDSET_METADATA: ["ProfileDirection", "DigitizationMethod"],

}

ALL_ELEMENT_NAMES = set()
for et, elements in ELEMENT_INFO.items():
    ALL_ELEMENT_NAMES.update(elements)

NO_MULTIVALUE = [
    x.strip() for x in """CentralFrequency
ObservationNumber
WaveSensor
WMOCurrentMeasurementDuration
WMOCurrentMeasurementMethod
WMODepthMethod
WMODissolvedOxygenSensorType
WMOPlatformMotionRemovalMethod
WMOSalinityDepthMeasurementMethod
WMOSpectralWaveMethod
WMOTemperatureSalinityMeasurementMethod
WMOTemperatureSalinityQualifier
WMOWetBulbMeasurementMethod
WMOWindInstrumentType
WMOProfileInstrumentType
WMOProfileRecorderType
SensorManufactureDate
SensorFrequency
SensorAccuracy
SensorResolution
ObservationPeriod
BackscatterWavelength
ObservationPeriodMaximum
ObservationPeriodMinimum
Quality
SensorRank
WorkingQuality
AggregationMethod
AnemometerType
CalibrationEquation
CurrentSensorType
DerivationComment
DerivationEquation
GliderPhaseCodeSource
Language
NavigationSatelliteType
SensorDepthReference
SensorLocation
SensorMake
SensorModel
SensorOrientation
SensorSerial
SensorType
TemperatureScale
UncertaintyType
Units
WMOAnemometerType
WMOHSPCorrected
WMOWindSource
SamplingFrequency
WMOAveragingPeriod
SpectralWaveDataType
EndDate
StartDate
EndLongitude
StartLongitude
EndLatitude
StartLatitude
PlatformMissionNumber
CNODCCruiseID
CNODCDeploymentMissionID
CNODCDeploymentPlatformID
DeploymentCruiseID
PlatformFinalStatus
MissionID
CNODCDuplicateDate
CNODCWorkingDuplicateDate
CNODCEmbargoUntil
CNODCPlatformCandidates
CNODCDuplicateID
CNODCID
CNODCLevel
CNODCMission
CNODCOperatorAction
CNODCPlatform
CNODCProgram
CNODCSource
CNODCStatus
CNODCWorkingDuplicateID
GTSHeader
BUFRMessageTime
BUFRDataCategory
BUFRIsObservation
BUFROriginCentre
BUFROriginSubcentre
BUFRSubsetIndex
BUFRDescriptors
BUFRInferredMessageType
BUFRPlatformType
PlatformServiceEnd
PlatformServiceStart
PlatformMaximumDepth
IMONumber
PlatformCategory
PlatformCNODCType
PlatformID
PlatformMake
PlatformModel
PlatformName
PlatformOwner
PlatformSerial
ShipC
WIGOSID
WMOID
CreationTime
DataUpdateInterval
DOI
XBTHeight
WMOPlatformType
WMOQualityLocation
WMOQualityLocationClass
WMOQualitySatellite
WMOStationType
WMOXBTType
GliderPhaseNumber
ProfileNumber
SegmentNumber
ShipLineNumber
ShipObservationNumber
ShipTransectNumber
ProfileID
SamplingScheme
BuoyEngineeringStatus
WMOBuoyType
WMODataBuoyType
WMOQualityAirTemperature
WMOQualityHousekeeping
WMOQualityPressure
WMOQualityWaterTemperature
WMOLandStationID
WMOSurfaceStationType
DigitizationMethod
ProfileDirection""".split("\n")
]

INVALID_PAIRS = {
    ET.PARAMETERS: [ET.COORDINATES, ET.PARENT_METADATA, ET.RECORDSET_METADATA, ET.ELEMENT_METADATA, ET.CHILD_METADATA],
    ET.COORDINATES: [ET.PARAMETERS, ET.PARENT_METADATA, ET.RECORDSET_METADATA, ET.ELEMENT_METADATA, ET.CHILD_METADATA],
    ET.ELEMENT_METADATA: [ET.PARAMETERS, ET.COORDINATES, ET.PARENT_METADATA, ET.RECORDSET_METADATA, ET.CHILD_METADATA],
    ET.PARENT_METADATA: [ET.PARAMETERS, ET.COORDINATES, ET.RECORDSET_METADATA, ET.ELEMENT_METADATA],
    ET.CHILD_METADATA: [ET.PARAMETERS, ET.COORDINATES, ET.RECORDSET_METADATA, ET.ELEMENT_METADATA, ET.PARENT_METADATA],
}

VALID_ELEMENTS = []
for etype, e_list in ELEMENT_INFO.items():
    VALID_ELEMENTS.extend(
        (etype, x)
        for x in e_list
    )
# child element data is also valid for parent metadata
VALID_ELEMENTS.extend((ET.PARENT_METADATA, x) for x in ELEMENT_INFO[ET.CHILD_METADATA])

GARBAGE_ELEMENT_NAMES = [
    "foobar",
    "_not_an_element",
    "",
    "temperature",
    "TEMPERATURE",
]
INVALID_ELEMENTS = []
for etype, invalid_list_type in INVALID_PAIRS.items():
    INVALID_ELEMENTS.extend((etype, x) for x in GARBAGE_ELEMENT_NAMES)
    for invalid_type in invalid_list_type:
        INVALID_ELEMENTS.extend((etype, x) for x in ELEMENT_INFO[invalid_type])

VALID_MULTIVALUE_ELEMENTS = [
    (x, MultiElement((SingleElement(1), SingleElement(2))))
    for x in ALL_ELEMENT_NAMES if x not in NO_MULTIVALUE
]

INVALID_MULTIVALUE_ELEMENTS = [
    (x, MultiElement((SingleElement(1), SingleElement(2))))
    for x in NO_MULTIVALUE
]
INVALID_MULTIVALUE_ELEMENTS.extend(((x, MultiElement([SingleElement(1)])) for x in ALL_ELEMENT_NAMES))
INVALID_MULTIVALUE_ELEMENTS.extend(((x, MultiElement([5, 6], _skip_normalization=True)) for x in ALL_ELEMENT_NAMES))


class TestIntegrityCheck(QCCheckerTestCase):

    def test_missing_units_fail(self):
        x = NODBIntegrityChecker()
        element = SingleElement(1)
        with self.assertFailsQC():
            x.element_compatible_units_check(element, preferred_units="m")

    @sub_tests(COMPATIBLE_UNITS, INCOMPATIBLE_UNITS)
    def test_compatible_units(self, unit_a, unit_b):
        x = NODBIntegrityChecker()
        ref = SingleElement(1, Units=unit_a)
        with self.assertPassesQC():
            x.element_compatible_units_check(ref, preferred_units=unit_b)

    @sub_tests(GOOD_DATA_TYPES_EXPANDED, BAD_DATA_TYPES_EXPANDED)
    def test_data_type_check(self, data_type, option):
        x = NODBIntegrityChecker()
        element = SingleElement(option)
        with self.assertPassesQC():
            x.element_data_type_check(element, data_type)

    @sub_tests(VALID_RANGE_CHECKS, INVALID_RANGE_CHECKS)
    def test_valid_range_check(self, value, value_units, min_value, max_value, units):
        x = NODBIntegrityChecker()
        element = SingleElement(value, Units=value_units)
        with self.assertPassesQC():
            x.element_valid_range_check(element, min_value, max_value, None, units, 'decimal')

    @sub_tests(GOOD_ALLOWED_CHECKS, BAD_ALLOWED_CHECKS)
    def test_valid_range_allowed_values_check(self, value, allowed_values, dtype):
        x = NODBIntegrityChecker()
        with self.assertPassesQC():
            element = SingleElement(value)
            x.element_valid_range_check(element, None, None, allowed_values, None, dtype)

    @sub_tests(GOOD_RS_TYPES, BAD_RS_TYPES)
    def test_recordset_types(self, rs_type: str):
        x = NODBIntegrityChecker()
        with self.assertPassesQC():
            x.recordset_valid_type_check(rs_type)

    @sub_tests(RS_GOOD_COORDINATES, RS_BAD_COORDINATES)
    def test_recordset_coordinates(self, rs_type, coordinates):
        x = NODBIntegrityChecker()
        r = ocproc2.BaseRecord()
        r.coordinates.update(coordinates)
        with self.assertPassesQC():
            x.record_valid_coordinates_for_rs_type_check(r, rs_type)

    @sub_tests(VALID_ELEMENTS, INVALID_ELEMENTS)
    def test_element_name_and_group(self, element_type: ElementType, element_name: str):
        x = NODBIntegrityChecker()
        with self.assertPassesQC():
            x.element_exists_and_proper_group_check(element_name, element_type)

    @sub_tests(VALID_MULTIVALUE_ELEMENTS, INVALID_MULTIVALUE_ELEMENTS)
    def test_multivalue(self, element_name: str, element: MultiElement):
        x = NODBIntegrityChecker()
        with self.assertPassesQC():
            x.multi_element_verify_allowed_and_present(element, element_name)