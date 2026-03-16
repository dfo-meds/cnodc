from __future__ import annotations
import decimal
import enum
import functools
import logging
import typing as t
import datetime

import netCDF4 as nc
import numpy as np

from autoinject import injector

import cnodc
from cnodc.ocproc2 import OCProc2Ontology
from cnodc.util import unnumpy
import cnodc.util.awaretime as awaretime

MultiLanguageString = t.Union[str, dict[str, str]]
NumberLike = t.Union[int, str, float, decimal.Decimal]


def get_bilingual_attribute(attribute_dict, attribute_name, locale_map):
    attr = {}
    for suffix in locale_map.keys():
        if f"{attribute_name}{suffix}" in attribute_dict:
            attr[locale_map[suffix]] = attribute_dict.pop(f"{attribute_name}{suffix}")
    return attr


class Encoding(enum.Enum):

    UTF8 = "utf8"  # strongly recommended
    ISO_8859_1 = "iso-8859-1"
    UTF16 = "utf16"

    @staticmethod
    def from_string(encoding: str):
        encoding = encoding.lower()
        if encoding == 'utf-8':
            return Encoding.UTF8
        return Encoding(encoding)


class Axis(enum.Enum):
    Time = 'T'
    Longitude = 'X'
    Latitude = 'Y'
    Depth = 'Z'

    @staticmethod
    def from_string(s: str):
        return Axis(s.upper())

class NetCDFDataType(enum.Enum):
    String = "String"
    Character = "char"
    Double = "double"
    Float = "float"
    Long = "long"
    LongUnsigned = "ulong"
    Integer = "int"
    IntegerUnsigned = "uint"
    Short = "short"
    ShortUnsigned = "ushort"
    Byte = "byte"
    ByteUnsigned = "ubyte"

    @staticmethod
    def from_string(name):
        if not name:
            return None
        elif isinstance(name, nc.VLType):
            return NetCDFDataType.from_string(name.dtype.name)
        elif name is str:
            return NetCDFDataType.String
        elif name in ('float64', 'f8', 'd'):
            return NetCDFDataType.Double
        elif name in ('float32', 'f4', 'f'):
            return NetCDFDataType.Float
        elif name in ('int64', 'i8'):
            return NetCDFDataType.Long
        elif name in ('int32', 'i4', 'i'):
            return NetCDFDataType.Integer
        elif name in ('int16', 'i2', 's', 'h'):
            return NetCDFDataType.Short
        elif name in ('int8', 'i1', 'b', 'B'):
            return NetCDFDataType.Byte
        elif name in ('uint64', 'u8'):
            return NetCDFDataType.LongUnsigned
        elif name in ('uint32', 'u4'):
            return NetCDFDataType.IntegerUnsigned
        elif name in ('uint16', 'u2'):
            return NetCDFDataType.ShortUnsigned
        elif name in ('uint8', 'u1'):
            return NetCDFDataType.ByteUnsigned
        elif name in ('S1', 'c'):
            return NetCDFDataType.Character
        elif name[0] == 'S' and name[1:].isdigit():
            return NetCDFDataType.String
        else:
            return NetCDFDataType(name)

class Unit(enum.Enum):

    # Latitude and Longitude
    DegreesNorth = "degrees_north"
    DegreesEast = "degrees_east"

    # Angles (0 = North)
    Degrees = "degree"

    # Ratios
    Ratio = "1"
    Percent = "1e-2"
    PartsPerThousand = "1e-3"
    PartsPerMillion = "1e-6"
    PartsPerBillion = "1e-9"

    # Temperature
    Celsius = "°C"
    Kelvin = "K"

    # Time
    Day = "d"
    Hour = "h"
    Minute = "min"
    Second = "s"

    # Mass
    Gram = "g"
    Kilogram = "kg"
    MetricTon = "t"

    # Speed
    Knot = "international_knot"
    MetersPerSecond = "m s-1"
    KilometersPerHour = "km h-1"

    # Distance
    Meter = "m"
    NauticalMile = "nautical_mile"

    # Voltage
    Volt = "V"

    # Pressure
    Pascal = "Pa"
    Hectopascal = "hPa"
    Kilopascal = "kPa"
    Atmosphere = "atm"
    Bar = "bar"
    Hectobar = "hbar"
    Millibar = "mbar"

    # Frequency
    Hertz = "Hz"
    PerSecond = "s-1"

    # Mass Fraction
    GramsPerKilogram = "g kg-1"

    # Mass Flux
    KilogramsPerSquareMeter = "kg m-2"

    # Energy Flux
    JoulesPerSquareMeter = "J m-2"

    # Power Density
    SquareMetersPerHertz = "m2 Hz-1"
    SquareMeterSeconds = "m2 s"

    # Power Density (by angle)
    SquareMeterSecondsPerRadian = "m2 s rad-1"

    # Molality
    MicromolesPerKilogram = "umol kg-1"

    # Molarity
    MicromolesPerCubicMeter = "umol m-3"
    MicromolesPerLitre = "umol L-1"
    MillimolesPerCubicMeter = "mmol m-3"

    # Density
    KilogramsPerCubicMeter = "kg m-3"
    MilligramsPerCubicMeter = "mg m-3"
    MilligramsPerLitre = "mg L-1"

    # Conductivity
    SiemensPerMeter = "S m-1"


class Calendar(enum.Enum):

    Standard = "standard"
    ProlepticGregorian = "proleptic_gregorian"
    Julian = "julian"
    Days365 = "noleap"
    Days366 = "all_leap"
    Days360 = "360_day"
    Nonstandard = "nonstandard"

    @staticmethod
    def from_string(calendar: str):
        if calendar is None or calendar == '':
            return None
        calendar = calendar.lower()
        if calendar == 'gregorian':
            return Calendar.Standard
        return Calendar(calendar)


class Direction(enum.Enum):

    Up = "up"
    Down = "down"


class IOOSCategory(enum.Enum):

    Acidity = "Acidity"
    Bathymetry = "Bathymetry"
    Biology = "Biology"
    BottomCharacter = "Bottom Character"
    CarbonDioxide = "CO2"
    ColoredDissolvedOrganicMatter = "Color Dissolved Organic Matter"
    Contaminants = "Contaminants"
    Currents = "Currents"
    DissolvedNutrients = "Dissolved Nutrients"
    DissolvedOxygen = "Dissolved O2"
    Ecology = "Ecology"
    FishAbundance = "Fish Abundance"
    FishSpecies = "Fish Species"
    HeatFlux = "HeatFlux"
    Hydrology = "Hydrology"
    IceDistribution = "Ice Distribution"
    Identifier = "Identifier"
    Location = "Location"
    Meteorology = "Meteorology"
    OceanColor = "Ocean Color"
    OpticalProperties = "Optical Properties"
    Other = "Other"
    Pathogens = "Pathogens"
    PhytoplanktonSpecies = "PhytoplanktonSpecies"
    Pressure = "Pressure"
    Productivity = "Productivity"
    Quality = "Quality"
    Salinity = "Salinity"
    SeaLevel = "Sea Level"
    Statistics = "Statistics"
    StreamFlow = "Stream Flow"
    SurfaceWaves = "SurfaceWaves"
    Taxonomy = "Taxonomy"
    Temperature = "Temperature"
    Time = "Time"
    TotalSuspendedMatter = "Total Suspended Matter"
    Unknown = "Unknown"
    Wind = "Wind"
    ZooplanktonSpecies = "Zooplankton Species"
    ZooplanktonAbundance = "Zooplankton Abundance"

    @staticmethod
    def from_string(s: str):
        if s is None or s == '':
            return None
        for v in IOOSCategory:
            if v.value.lower().replace(' ', '') == s.lower().replace(' ', ''):
                return v
        raise ValueError(f'Invalid IOOS Category: [{s}]')


class TimePrecision(enum.Enum):
    Month = "month"
    Day = "day"
    Hour = "hour"
    Minute = "minte"
    Second = "second"
    TenthSecond = "tenth_second"
    HundredthSecond = "hundredth_second"
    Millisecond = "millisecond"


class NumericTimeUnits(enum.Enum):

    Years = "years"
    Months = "months"
    Weeks = "weeks"
    Days = "days"
    Hours = "hours"
    Minutes = "minutes"
    Seconds = "seconds"
    Milliseconds = "milliseconds"


class TimeZone(enum.Enum):
    UTC = "Etc/UTC"  # Strongly Recommended
    CanadaEastern = "America/Toronto"
    CanadaMountain = "America/Edmonton"
    CanadaAtlantic = "America/Halifax"
    CanadaCentral = "America/Winnipeg"
    CanadaNewfoundland = "America/St_Johns"
    CanadaPacific = "America/Vancouver"


class ERDDAPVariableRole(enum.Enum):

    ProfileExtra = "profile_extra"
    TimeseriesExtra = "timeseries_extra"
    TrajectoryExtra = "trajectory_extra"


class CFVariableRole(enum.Enum):

    ProfileID = "profile_id"
    TimeseriesID = "timeseries_id"
    TrajectoryID = "trajectory_id"


class CoverageContentType(enum.Enum):

    Auxillary = "auxillaryInformation"
    Coordinate = "coordinate"
    Image = "image"
    ModelResult = "modelResult"
    PhysicalMeasurement = "physicalMeasurement"
    QualityInformation = "qualityInformation"
    ReferenceInformation = "referenceInformation"
    ThematicClassification = "thematicClassification"

    @staticmethod
    def from_string(cc: str):
        if not cc:
            return None
        return CoverageContentType(cc)



class GCAudience(enum.Enum):

    AboriginalPeoples = "aboriginal_peoples"
    Business = "business"
    Children = "children"
    Educators = "educators"
    Employers = "employers"
    FundingApplicants = "funding_applications"
    GeneralPublic = "general_public"
    Government = "government"
    Immigrants = "immigrants"
    JobSeekers = "job_seekers"
    Media = "media"
    NonCanadians = "noncanadians"
    NGOs = "nongovernmental_organizations"
    Parents = "parents"
    PersonsWithDisabilities = "persons_with_disabilities"
    RuralCommunity = "rural_community"
    Seniors = "seniors"
    Scientists = "scientists"
    Students = "students"
    Travellers = "travellers"
    Veterans = "veterans"
    Visitors = "visitors_to_canada"
    Women = "women"
    Youth = "youth"


class GCCollectionType(enum.Enum):
    NonSpatial = "primary"
    Geospatial = "geogratis"
    OpenMaps = "fgp"
    Publications = "publication"


class GCSubject(enum.Enum):
    Oceanography = "oceanography"


PROVINCES = {
    'BC': 'British Columbia',
    'ON': 'Ontario',
    'NS': 'Nova Scotia',
    'NL': 'Newfoundland and Labrador',
    'NB': 'New Brunswick',
    'AB': 'Alberta',
    'MB': 'Manitoba',
    'QC': 'Quebec',
    'PE': 'Prince Edward Island',
    'SK': 'Saskatchewan',
    'NT': 'Northwest Territories',
    'NU': 'Nunavut',
    'YT': 'Yukon',
}

class GCPlace(enum.Enum):
    Canada = "canada"  # General
    Burlington = "ontario_-_halton"  # CCIW
    Ottawa = "ontario_-_ottawa"  # NCR
    Dartmouth = "nova_scotia_-_halifax"  # BIO
    Moncton = "nova_scotia_-_westmorland"  # GFC
    Montjoli = "quebec_-_la_mitis"  # IML
    Nanaimo = "british_columbia_-_nanaimo"  # PBS
    Sidney = "british_columbia_-_capital"  # IOS
    StJohns = "newfoundland_and_labrador_-_division_no._1"  # NAFC

    @staticmethod
    def from_string(s):
        if s is None or s == '':
            return None
        s = s.strip()
        if ',' in s:
            city, province = s.split(',', maxsplit=1)
            province = province.strip()
            if province.upper() in PROVINCES:
                province = PROVINCES[province.upper()]
            s = f"{province} - {city.strip()}"
        while '  ' in s:
            s = s.replace('  ', ' ')
        s = s.lower().replace(' ', '_')
        return GCPlace(s)


class ERDDAPDatasetType(enum.Enum):

    DSGTable = "EDDTableFromNcCFFiles"  # Use this one for files following CF's DSG conventions
    MultiDimDSGMTable = "EDDTableFromMultidimNcFile"  # Multi-dimensional CF DSG files
    OtherNetCDFTable = "EDDTableFromNcFiles"  # All other netcdf formats
    ASCIITable = "EDDTableFromAsciiFiles"  # ASCII files
    NetCDFGrid = "EDDGridFromNcFiles"  # Gridded NetCDF files


class CommonDataModelType(enum.Enum):
    Point = "Point"  # (x, y, t[, d])
    Profile = "Profile"  # (x, y, t) and (d)
    TimeSeries = "TimeSeries"  # station:(x, y[, d]) and (t)
    TimeSeriesProfile = "TimeSeriesProfile"  # station:(x, y) and (t, d)
    Trajectory = "Trajectory"  # station: () and (x, y, t[, d])
    TrajectoryProfile = "TrajectoryProfile"  # station: () and (x, y, t) and (d)

    # These are non-standard but recognized by ERDDAP
    Grid = "Grid"  # fixed (x, y[, t][, d]) grid
    MovingGrid = "MovingGrid"  # grid but (x,y[,d]) may vary over time
    RadialSweep = "RadialSweep"  # e.g. radial / gate, azimuth/distance, etc
    Swath = "Swath"

    Other = "Other"  # data that does not have geographical coordinates

    @staticmethod
    def from_string(attr_value):
        if not attr_value:
            return None
        lower_attr = attr_value.lower()
        if lower_attr == 'point':
            return CommonDataModelType.Point
        elif lower_attr == 'timeseries':
            return CommonDataModelType.TimeSeries
        elif lower_attr == 'trajectory':
            return CommonDataModelType.Trajectory
        elif lower_attr == 'profile':
            return CommonDataModelType.Profile
        elif lower_attr == 'timeseriesprofile':
            return CommonDataModelType.TimeSeriesProfile
        elif lower_attr == 'trajectoryprofile':
            return CommonDataModelType.TrajectoryProfile
        elif lower_attr == 'grid':
            return CommonDataModelType.Grid
        elif lower_attr == 'movinggrid':
            return CommonDataModelType.MovingGrid
        elif lower_attr == 'radialsweep':
            return CommonDataModelType.RadialSweep
        elif lower_attr == 'swath':
            return CommonDataModelType.Swath
        else:
            return CommonDataModelType.Other


class StandardName(enum.Enum):

    AirPressure = "air_pressure"


class EssentialOceanVariable(enum.Enum):

    Oxygen = "oxygen"
    Nutrients = "nutrients"
    InorganicCarbon = "inorganicCarbon"
    DissolvedOrganicCarbon = "dissolvedOrganicCarbon"
    TransientTracers = "transientTracers"
    ParticulateMatter = "particulateMatter"
    NitrousOxide = "nitrousOxide"
    StableCarbonIsotopes = "stableCarbonIsotopes"
    PhytoplanktonBiomassAndDiversity = "phytoplanktonBiomassAndDiversity"
    ZooplanktonBiomassAndDiversity = "zooplanktonBiomassAndDiversity"
    FishAbundanceAndDistribution = "fishAbundanceAndDistribution"
    MarineTurtlesBirdsMammalsAbundanceAndDistribution = "marineTurtlesBirdsMammalsAbundanceAndDistribution"
    HardCoralCoverAndComposition = "hardCoralCoverAndComposition"
    SeagrassCoverAndComposition = "seagrassCoverAndComposition"
    MacroalgalCanopyCoverAndComposition = "macroalgalCanopyCoverAndComposition"
    InvertebrateAbundanceAndDistribution = "invertebrateAbundanceAndDistribution"
    MicrobeBiomassAndDiversity = "microbeBiomassAndDiversity"

    OceanColour = "oceanColour"
    OceanSound = "oceanSound"
    MarineDebris = "marineDebris"

    SurfaceHeight = "seaSurfaceHeight"
    Ice = "seaIce"
    State = "seaState"
    SurfaceSalinity = "seaSurfaceSalinity"
    SurfaceTemperature = "seaSurfaceTemperature"
    SurfaceCurrents = "surfaceCurrents"
    SubSurfaceSalinity = "subSurfaceSalinity"
    SubSurfaceTemperature = "subSurfaceTemperature"
    SubSurfaceCurrents = "subSurfaceCurrents"
    HeatFlux = "oceanSurfaceHeatFlux"
    SurfaceStress = "oceanSurfaceStress"
    BottomPressure = "oceanBottomPressure"
    Other = "other"


class MaintenanceFrequency(enum.Enum):

    Annually = "annually"
    Quarterly = "quarterly"
    Monthly = "monthly"
    Daily = "daily"
    Weekly = "weekly"

    TwiceAnnually = "biannually"
    EveryTwoYears = "biennially"
    EveryTwoWeeks = "fortnightly"
    TwiceMonthly = "semimonthly"

    Periodic = "periodic"
    AsNeeded = "asNeeded"
    Continual = "continual"
    Irregular = "irregular"
    NotPlanned = "notPlanned"
    Unknown = "unknown"


class TopicCategory(enum.Enum):

    Biota = "biota"
    Boundaries = "boundaries"
    ClimatologyMeteorologyAtmosphere = "climatologyMeteorologyAtmosphere"
    Disaster = "disaster"
    Economy = "economy"
    Elevation = "elevation"
    Environment = "environment"
    ExtraTerrestrial = "extraTerrestrial"
    Farming = "farming"
    GeoscientificInformation = "geoscientificInformation"
    Health = "health"
    ImageryBaseMapsEarthCover = "imageryBaseMapsEarthCover"
    IntelligenceMilitary = "intelligenceMilitary"
    InlandWaters = "inlandWaters"
    Location = "location"
    Oceans = "oceans"
    PlanningCadastre = "planningCadastre"
    Society = "society"
    Structure = "structure"
    Transportation = "transportation"
    UtilitiesCommunication = "utilitiesCommunication"


class SpatialRepresentation(enum.Enum):

    Grid = "grid"
    Stereographic = "stereoModel"
    TextTable = "textTable"
    TIN = "tin"
    Vector = "vector"
    Video = "video"


class StatusCode(enum.Enum):

    Accepted = "accepted"
    """ The dataset has been accepted but not formalized. """

    Completed = "completed"
    """ The dataset is completed. """

    Deprecated = "deprecated"
    """ The dataset is deprecated. """

    Final = "final"
    """ The dataset is final and shouldn't be changed. """

    Historical = "historicalArchive"
    """ The dataset is a historical archive. """

    NotAccepted = "notAccepted"
    """ The dataset was not accepted. """

    Obsolete = "obsolete"
    """ The dataset is considered obsolete. """

    OnGoing = "onGoing"
    """ The dataset is ongoing, new data is still being added. """

    Pending = "pending"
    """ The dataset is pending. """

    Planned = "planned"
    """ The dataset is planned, but not yet being collected. """

    Proposed = "proposed"
    """ The dataset has been proposed but is not yet accepted. """

    Required = "required"
    """ The dataset is required. """

    Retired = "retired"
    """ The dataset is retired. """

    Superseded = "superseded"
    """ The dataset has been superseded by another. """

    Tentative = "tentative"
    """ The dataset is tentative and subject to further review. """

    UnderDevelopment = "underDevelopment"
    """ The dataset is under development. """

    Valid = "valid"
    """ The dataset is valid. """

    Withdrawn = "withdrawn"
    """ The dataset has been withdrawn. """


class CoordinateReferenceSystem(enum.Enum):

    NAD27 = {
        '_guid': 'nad27',
    }
    ''' EPSG 4267: the NAD27 datum '''

    WGS84 = {
        "_guid": "wgs84",
    }
    ''' EPSG 4326: the WGS84 datum '''

    MSL_Depth = {
        "_guid": "msl_depth",
    }
    ''' EPSG 5715: depth below a non-specific mean sea level (depth positive) '''

    MSL_Heights = {
        "_guid": "msl_height",
    }
    ''' EPSG 5714: height above a non-specific mean sea level (depth negative) '''

    Instant_Depth = {
        "_guid": "instant_depth",
    }
    ''' EPSG 5831: depth below current instantaneous sea level (depth positive) '''

    Instant_Heights = {
        "_guid": "instant_heights",
    }
    ''' EPGS 5829: altitude above current instantaneous sea level (depth negative) '''

    Gregorian = {
        "_guid": "gregorian",
    }
    ''' Standard Gregorian calendar '''

    @staticmethod
    def from_string(value: str):
        if value is None or value == '':
            return None
        value = str(value).upper().replace(" ", "")
        if value in ('4326', 'EPSG:4326', 'WGS84'):
            return CoordinateReferenceSystem.WGS84
        elif value in ('4267', 'EPSG:4267', 'NAD27'):
            return CoordinateReferenceSystem.NAD27
        elif value in ('5829', 'EPSG:5829'):
            return CoordinateReferenceSystem.Instant_Heights
        elif value in ('5831', 'EPSG:5831'):
            return CoordinateReferenceSystem.Instant_Depth
        elif value in ('5715', 'EPSG:5715', 'MSLD'):
            return CoordinateReferenceSystem.MSL_Depth
        elif value in ('5714', 'EPSG:5714', 'MSLH'):
            return CoordinateReferenceSystem.MSL_Heights
        elif value in ('STANDARD', 'GREGORIAN'):
            return CoordinateReferenceSystem.Gregorian
        raise ValueError(f'Unknown CRS: [{value}]')




class MaintenanceScope(enum.Enum):

    Dataset = "dataset"
    Metadata = "metadata"


class ResourceType(enum.Enum):

    Auto = "_autodetect"
    File = "file"
    WebPage = "http"
    SecureWebPage = "https"
    FTP = "ftp"
    ERDDAPGrid = "ERDDAP:griddap"
    ERDDAPTable = "ERDDAP:tabledap"
    Git = "GIT"


class GCContentFormat(enum.Enum):

    Auto = "_autodetect"

    VideoAVI = "AVI"
    VideoMOV = "MOV"
    VideoMPEG = "MPEG"

    AudioMP3 = "MP3"

    ImageBMP = "BMP"
    ImageGIF = "GIF"
    ImageJPG = "JPG"
    ImagePNG = "PNG"
    ImageSVG = "SVG"
    ImageTIFF = "TIFF"

    DataCSV = "CSV"
    DataJSON = "JSON"
    DataNetCDF = "NetCDF"
    DataXLS = "XLS"
    DataXLSX = "XLSX"
    DataXML = "XML"

    DocumentDOC = "DOC"
    DocumentDOCX = "DOCX"
    DocumentPDF = "PDF"
    DocumentPDFA1 = "PDF/A-1"
    DocumentPDFA2 = "PDF/A-2"

    SlidesPPT = "PPT"
    SlidesPPTX = "PPTX"

    Hypertext = "HTML"

    WebApplication = "Web App"

    OGCWFS = "WFS"
    OGCWMS = "WMS"
    OGCWMTS = "WMTS"

    ArchiveZIP = "ZIP"
    ArchiveTAR = "TAR"
    ArchiveGZIP = "GZIP"
    ArchiveTARGZIP = "TAR.GZ"


class ResourcePurpose(enum.Enum):

    Information = "information"
    """ The resource provides more information about the object. """

    BrowseText = "browsing"
    """ The resource is text data to be browsed. """

    BrowseGraphic = "browseGraphic"
    """ The resource is graphical data to be browsed. """

    Search = "search"
    """ The resource is a search function for data. """

    Upload = "upload"
    """ The resource is for people to upload data. """

    CompleteMetadata = "completeMetadata"
    """ The resource is a complete metadata record for the data. """

    EmailRequest = "emailService"
    """ The resource is an email address to make requests for data to. """

    Download = "download"
    """ The resource is a location to download data from. """

    FileAccess = "fileAccess"
    """ The resource is access to data files to browse/download. """

    OfflineAccess = "offlineAccess"
    """ The resource is a method to access data offline. """

    OnlineOrder = "order"
    """ The resource is a website where you can order data. """


class GCContentType(enum.Enum):

    Dataset = "dataset"
    """ The resource is a dataset. """

    WebService = "web_service"
    """ The resource is a web service. """

    API = "api"
    """ The resource is an API that is not a web service. """

    SupportingDocumentation = "support_doc"
    """ The resource is supporting documentation. """

    Application = "application"
    """ The resource is an application other than a web service or API. """


class GCLanguage(enum.Enum):

    NoLanguage = []
    """ The resource has no language component. """

    English = ["ENG"]
    """ The resource is provided in English. """

    French = ["FRA"]
    """ The resource is provided in French. """

    Bilingual = ["ENG", "FRA"]
    """ The resource is provided in both language. """


class ContactRole(enum.Enum):

    Author = "author"
    """ A person who solely wrote the document."""

    CoAuthor = "coAuthor"
    """ A person who wrote the document in collaboration with others. """

    Collaborator = "collaborator"
    """ A person who helped make the document in some other way. """

    Contributor = "contributor"
    """ A person who contributed to the document in some way. """

    Custodian = "custodian"
    """ A person who is responsible for safeguarding the document. """

    Distributor = "distributor"
    """ A person who is responsible for distributing the document. """

    Editor = "editor"
    """ A person who helped edit the document. """

    Funder = "funder"
    """ A person who helped fund the creation of the document. """

    Mediator = "mediator"

    Originator = "originator"
    """ A person who originated data to support building the document. """

    Owner = "owner"
    """ A person who owns the document. """

    ContactPoint = "pointOfContact"
    """ A person who is the point of contact for the document and can answer questions. """

    PrincipalInvestigator = "principalInvestigator"
    """ A person who is the principal investigator of a research project. """

    Processor = "processor"
    """ A person who processed the data. """

    Publisher = "publisher"
    """ A person who published the data. """

    ResourceProvider = "resourceProvider"
    """ A person who provides the resource to others. """

    RightsHolder = "rightsHolder"
    """ A person who owns the rights to the document. """

    Sponsor = "sponsor"
    """ A person who sponsored the creation of the document. """

    Stakeholder = "stakeholder"
    """ A person who is a stakeholder in the creation of the document. """

    User = "user"
    """ A person who uses the data. """

    @staticmethod
    def from_string(value: str):
        # W08 approximate mapping
        if value == 'CONT0001':
            return ContactRole.Stakeholder
        elif value == 'CONT0002':
            return ContactRole.Owner
        elif value == 'CONT0003':
            return ContactRole.Originator  # not great sorry
        elif value == 'CONT0004':
            return ContactRole.PrincipalInvestigator
        elif value == 'CONT0005':
            return ContactRole.Stakeholder
        elif value == 'CONT0006':
            return ContactRole.Processor
        elif value == 'CONT0007':
            return ContactRole.Stakeholder
        # ISO mappings
        elif value in ContactRole:
            return ContactRole(value)
        else:
            return None


class IDSystem(enum.Enum):

    DOI = {
        "_guid": "DOI",
    }
    """ Digital Object Identifier system - codes should just be the DOI without any prefix. """

    ROR = {
        "_guid": "ROR",
    }
    """ Research Organization Registry system - codes should just be the ROR without any prefix. """

    ORCID = {
        "_guid": "ORCID",
    }
    """ Open Researcher and Contributor ID system - codes should just be the ORCID without any prefix. """

    VesselIMO = {
        "_guid": "IMONumber",
    }
    """ IMO vessel numbers. """

    EPSG = {
        "_guid": "EPSG",
    }


class TelephoneType(enum.Enum):

    Fax = "fax"
    Cell = "cell"
    Voice = "voice"


class Country(enum.Enum):

    Canada = "CAN"
    UnitedStates = "USA"


class Locale(enum.Enum):

    CanadianEnglish = {
        '_guid': 'canadian_english_utf8',
    }
    """ Canadian English using UTF-8 text encoding. """

    CanadianFrench = {
        '_guid': 'canadian_french_utf8',
    }
    """ Canadian French using UTF-8 text encoding. """


class DistanceUnit(enum.Enum):

    Meters = "m"
    Kilometers ="km"


class AngularUnit(enum.Enum):

    Radians = "radian"
    ArcDegrees = "arc_degree"
    ArcMinutes = "arc_minute"
    ArcSeconds = "arc_second"


class RestrictionCode(enum.Enum):

    Unrestricted = "unrestricted"
    """ Material is not covered by a license, copyright, patent, trademark, or any other restrictions. 
        It may be freely used and distributed by all. Use for public domain works. 
    """

    Copyright = "copyright"
    """ Material is covered by a copyright and requires permission from the rightsholder. """

    Patent = "patent"
    """ Material is covered under an existing patent. """

    PatentPending = "patentPending"
    """ Material is covered under a patent that is pending approval. """

    Trademark = "trademark"
    """ Material is covered under a trademark. """

    License = "licence"
    """ Material requires the acceptance of a formal license. """

    LicenseToDistribute = "licenceDistributor"
    """ Material requires a formal license to sell or distribute. """

    LicenseEndUser = "licenseEndUser"
    """ Material requires a formal license to use. """

    LicenseUnrestricted = "licenseUnrestricted"
    """ Material is covered under a license, but no formal permission required (e.g. the Open Canada License). """

    Restricted = "restricted"
    """ Material includes sensitive information and should not be distributed, but not classified or protected. """

    Confidential = "confidential"
    """ Material includes sensitive information. Use if the data is classified or protected and add a SecurityConstraint as well. """

    Statutory = "statutory"
    """ Access, use, or distribution is restricted by law. """

    Other = "otherRestrictions"
    """ Material is covered under other restrictions or conditions, as detailed in the use constraint (add a description or citation at least). """


class ClassificationCode(enum.Enum):

    Unclassified = "unclassified"
    """ Material is not protected or classified. """

    LimitedDistribution = "limitedDistribution"
    """ Material is not widely distributed, but not specifically protected or classified. """

    OfficialUseOnly = "forOfficialUseOnly"
    """ Material is not protected or classified, but should not be used except for official purposes. """

    Protected = "protected"
    """ Material is Protected A, B, or C. """

    Confidential = "confidential"
    """ Material is classified as Confidential"""

    Secret = "secret"
    """ Material is classified as Secret. """

    TopSecret = "topSecret"
    """ Material is classified as Top Secret. """


class GCPublisher(enum.Enum):

    MEDS = {
        "_guid": "meds",
    }


class KeywordType(enum.Enum):

    DataCenter = "dataCentre"
    Discipline = "discipline"
    FeatureType = "featureType"
    Instrument = "instrument"
    Place = "place"
    Platform = "platform"
    Process = "process"
    Product = "product"
    Project = "project"
    Service = "service"
    Stratum = "stratum"
    SubTheme = "subTopicCategory"
    Taxon = "taxon"
    Temporal = "temporal"
    Theme = "theme"


class EntityRef:

    def __init__(self,
                 guid: t.Optional[str] = None,
                 display_name: t.Optional[MultiLanguageString] = None,
                 **kwargs):
        self._guid = None
        self._display_name = None
        self._metadata = {}
        self._children: dict[str, t.Union[t.Optional[EntityRef], list[EntityRef]]] = {}
        self.guid = guid
        self.display_name = display_name
        for kwarg in kwargs:
            setattr(self, kwarg, kwargs[kwarg])

    @property
    def guid(self):
        return self._guid

    @guid.setter
    def guid(self, guid):
        self._guid = guid

    @property
    def display_name(self):
        return self._display_name

    @display_name.setter
    def display_name(self, display_name: MultiLanguageString):
        self._display_name = EntityRef.format_multilingual_text(display_name)

    def build_request_body(self):
        d = {}
        if self._guid is not None:
            d['_guid'] = self._guid
        if self._display_name is not None:
            d['_display_names'] = self._display_name
        EntityRef._clean_dict(self._metadata)
        d.update(self._metadata)
        for key in self._children.keys():
            if not self._children[key]:
                continue
            elif isinstance(self._children[key], EntityRef):
                d[key] = self._children[key].build_request_body()
            else:
                d[key] = [
                    x.build_request_body()
                    for x in self._children[key]
                    if x is not None
                ]
        return d

    @staticmethod
    def _clean_dict(d):
        if isinstance(d, dict):
            for key in list(d.keys()):
                if d[key] is None or d[key] == '':
                    del d[key]
                elif isinstance(d[key], (list, tuple, set, dict)):
                    if len(d[key]) == 0:
                        del d[key]
                    else:
                        EntityRef._clean_dict(d[key])
            return d
        elif isinstance(d, (list, tuple, set)):
            return [
                EntityRef._clean_dict(x) if isinstance(x, (dict, list, tuple, set)) else x
                for x in d
            ]

    @staticmethod
    def format_multilingual_text(text: t.Optional[MultiLanguageString]):
        if text is None:
            return None
        if isinstance(text, dict):
            return text
        return {'und': text}

    @staticmethod
    def format_date(d: t.Optional[t.Union[datetime.date, datetime.datetime, str]]):
        if d is None:
            return None
        if isinstance(d, str):
            if len(d) == 10:
                d = datetime.date.fromisoformat(d)
            else:
                d = awaretime.utc_from_isoformat(d)
        return d.isoformat()

    def get(self, metadata_key, coerce=None):
        value = self._metadata.get(metadata_key, None)
        if coerce:
            return coerce(value)
        return value

    def set(self, value, metadata_key, coerce=None):
        self._metadata[metadata_key] = value if not coerce else coerce(value)

    def get_children(self, metadata_key):
        if metadata_key not in self._children:
            self._children[metadata_key] = []
        return self._children[metadata_key]

    def get_child(self, child_type: str):
        return self._children.get(child_type, None)

    def set_child(self, v: EntityRef, child_type: str):
        self._children[child_type] = v

    def set_id_and_system(self, code, system: IDSystem, remove_prefixes: t.Optional[list[str]]):
        if remove_prefixes:
            for prefix in remove_prefixes:
                if code.startswith(prefix):
                    code = code[len(prefix):]
                    break
        self.id_code = code
        self.id_system = system

    def get_id_code(self):
        return self.id_code

    @staticmethod
    def make_id_property(system: IDSystem, remove_prefixes=None):
        return property(
            functools.partial(EntityRef.get_id_code),
            functools.partial(EntityRef.set_id_and_system, system=system, remove_prefixes=remove_prefixes)
        )

    @staticmethod
    def make_property(metadata_key, coerce=None):
        return property(
            functools.partial(EntityRef.get, metadata_key=metadata_key),
            functools.partial(EntityRef.set, metadata_key=metadata_key, coerce=coerce)
        )

    @staticmethod
    def make_child_property(metadata_key):
        return property(
            functools.partial(EntityRef.get_child, child_type=metadata_key),
            functools.partial(EntityRef.set_child, child_type=metadata_key)
        )

    @staticmethod
    def make_children_property(metadata_key):
        return property(
            functools.partial(EntityRef.get_children, metadata_key=metadata_key)
        )

    @staticmethod
    def make_enum_property(metadata_key: str, enum_type: type, coerce_list: bool = False):
        return property(
            functools.partial(EntityRef.get, metadata_key=metadata_key, coerce=functools.partial(EntityRef._coerce_to_enum, enum_type=enum_type, coerce_list=coerce_list)),
            functools.partial(EntityRef.set, metadata_key=metadata_key, coerce=functools.partial(EntityRef._coerce_from_enum, enum_type=enum_type, coerce_list=coerce_list))
        )

    @staticmethod
    def _coerce_from_enum(v, enum_type, coerce_list: bool = False):
        if not isinstance(v, enum_type):
            if isinstance(v, (list, tuple, set)) and coerce_list:
                return [EntityRef._coerce_from_enum(x, enum_type) for x in v]
            elif v is None or v == '':
                if coerce_list:
                    return [None]
                return None
            elif hasattr(enum_type, 'from_string'):
                v = enum_type.from_string(v)
            else:
                v = enum_type(v)
        return [v.value] if coerce_list else v.value

    @staticmethod
    def _coerce_to_enum(v, enum_type, coerce_list: bool = False):
        if v is None:
            return None
        if not isinstance(v, enum_type):
            if isinstance(v, list) and coerce_list:
                return [EntityRef._coerce_to_enum(x, enum_type) for x in v]
            return enum_type(v)
        return v


class Variable(EntityRef):

    cnodc_name = EntityRef.make_property('cnodc_name')
    axis = EntityRef.make_enum_property('axis', Axis)
    units = EntityRef.make_property('units')
    actual_min = EntityRef.make_property('actual_min', unnumpy)
    actual_max = EntityRef.make_property('actual_max', unnumpy)
    positive_direction = EntityRef.make_enum_property('positive', Direction)
    encoding = EntityRef.make_enum_property('encoding', Encoding)
    source_name = EntityRef.make_property('source_name')
    source_data_type = EntityRef.make_enum_property('source_data_type', NetCDFDataType)
    destination_name = EntityRef.make_property('destination_name')
    destination_data_type = EntityRef.make_enum_property('destination_data_type', NetCDFDataType)
    dimensions = EntityRef.make_property('dimensions', lambda x: ','.join(x))
    long_name = EntityRef.make_property('long_name', EntityRef.format_multilingual_text)
    standard_name = EntityRef.make_property('standard_name')
    time_precision = EntityRef.make_enum_property('time_precision', TimePrecision)
    calendar = EntityRef.make_enum_property('calendar', Calendar)
    time_zone = EntityRef.make_enum_property('time_zone', TimeZone)
    missing_value = EntityRef.make_property('missing_value', unnumpy)
    scale_factor = EntityRef.make_property('scale_factor', unnumpy)
    add_offset = EntityRef.make_property('add_offset', unnumpy)
    ioos_category = EntityRef.make_enum_property('ioos_category', IOOSCategory)
    valid_min = EntityRef.make_property('valid_min', unnumpy)
    valid_max = EntityRef.make_property('valid_max', unnumpy)
    allow_subsets = EntityRef.make_property('allow_subsets', bool)
    cf_role = EntityRef.make_enum_property('cf_role', CFVariableRole)
    erddap_role = EntityRef.make_enum_property('erddap_role', ERDDAPVariableRole)
    comment = EntityRef.make_property('comment')
    references = EntityRef.make_property('references')
    source = EntityRef.make_property('source')
    coverage_content_type = EntityRef.make_enum_property('coverage_content_type', CoverageContentType)
    variable_order = EntityRef.make_property('variable_order', int)
    is_axis = EntityRef.make_property('is_axis', bool)
    is_altitude_proxy = EntityRef.make_property('altitude_proxy', bool)

    @property
    def additional_properties(self):
        return self._metadata.get('custom_metadata', {})

    @additional_properties.setter
    def additional_properties(self, properties: dict):
        self._metadata['custom_metadata'] = {}
        for key in properties:
            val = unnumpy(properties[key])
            if val is not None and val != '':
                self._metadata['custom_metadata'][key] = val

    def set_time_units(self, base_units: NumericTimeUnits, epoch: datetime.datetime):
        """
        :param base_units: The basic units of duration (e.g. "seconds since")
        :param epoch: The reference time
        """
        self._metadata['units'] = f"{base_units.value} since {epoch.isoformat()}"

    @staticmethod
    def build_from_netcdf(ds_var: nc.Variable, locale_map) -> Variable:
        var_attributes = {x: ds_var.getncattr(x) for x in ds_var.ncattrs()}
        var = Variable(
            source_name=ds_var.name,
            source_data_type=ds_var.dtype,
            dimensions=ds_var.dimensions,
            long_name=get_bilingual_attribute(var_attributes, 'long_name', locale_map),
            display_name={"und": ds_var.name},
            guid=ds_var.name,
            comment=var_attributes.pop('comment', None),
            references=var_attributes.pop('references', None),
            source=var_attributes.pop('source', None),
            coverage_content_type=var_attributes.pop('coverage_content_type', None),
            units=var_attributes.pop('units', None),
            valid_min=var_attributes.pop('valid_min', None),
            valid_max=var_attributes.pop('valid_max', None),
            standard_name=var_attributes.pop('standard_name', None),
            calendar=var_attributes.pop('calendar', None),
            positive_direction=var_attributes.pop('positive', None),
            scale_factor=var_attributes.pop('scale_factor', None),
            add_offset=var_attributes.pop('add_offset', None),
            time_precision=var_attributes.pop('time_precision', None),
            time_zone=var_attributes.pop('time_zone', None),
            cf_role=var_attributes.pop('cf_role', None),
            axis=var_attributes.pop('axis', None),
            cnodc_name=var_attributes.pop('cnodc_standard_name', None)
        )
        if var.source_data_type not in (NetCDFDataType.String, NetCDFDataType.Character):
            values = ds_var[:]
            if values.size > 0:
                var.actual_min = np.min(values)
                var.actual_max = np.max(values)
        var_attributes.pop('actual_min', None)
        var_attributes.pop('actual_max', None)
        if 'missing_value' in var_attributes:
            var.missing_value = var_attributes.pop('missing_value')
            var_attributes.pop("_FillValue", "")
        elif '_FillValue' in var_attributes:    # pragma: no coverage (fallback)
            var.missing_value = var_attributes.pop('_FillValue')
        if '_Encoding' in var_attributes:
            var.encoding = var_attributes.pop('_Encoding')
        elif var.source_data_type in (NetCDFDataType.String, NetCDFDataType.Character):
            var.encoding = Encoding.UTF8
        var.additional_properties = var_attributes
        return var



class MaintenanceRecord(EntityRef):

    date = EntityRef.make_property('date', EntityRef.format_date)
    notes = EntityRef.make_property('notes', EntityRef.format_multilingual_text)
    scope = EntityRef.make_enum_property('scope', MaintenanceScope)


class QuickWebPage(EntityRef):

    name = EntityRef.make_property('name', EntityRef.format_multilingual_text)
    description = EntityRef.make_property('description', EntityRef.format_multilingual_text)
    link_purpose = EntityRef.make_enum_property('function', ResourcePurpose)

    @property
    def url(self):
        return self.get('url')

    @url.setter
    def url(self, url: MultiLanguageString):
        self.set(EntityRef.format_multilingual_text(url), 'url')
        if url:
            self._auto_update_from_url()

    def _auto_update_from_url(self):
        if self.resource_type is None:
            self.resource_type = ResourceType.Auto

    @property
    def resource_type(self):
        return self.get('protocol')

    @resource_type.setter
    def resource_type(self, res_type: ResourceType):
        if res_type != ResourceType.Auto:
            self.set(res_type.value if res_type else None, 'protocol')
        else:
            self.set(Resource.autodetect_resource_type(self.url), 'protocol')

    @staticmethod
    def autodetect_resource_type(full_url: t.Optional[dict]):
        if full_url is None:
            return None
        url = ""
        if 'und' in full_url:
            url = full_url['und']
        elif 'en' in full_url:
            url = full_url['en']
        else:
            for key in full_url.keys():
                url = full_url[key]
                break
        if url.startswith("https://"):
            return "https"
        elif url.startswith("http://"):
            return "http"
        elif url.startswith(("ftp://", "ftps://", "ftpse://")):
            return "ftp"
        elif url.startswith("git://"):
            return 'git'
        elif url.startswith("file://"):
            return 'file'
        return None


class Resource(QuickWebPage):

    additional_request_info = EntityRef.make_property('protocol_request', EntityRef.format_multilingual_text)
    additional_app_info = EntityRef.make_property('app_profile', EntityRef.format_multilingual_text)
    gc_content_type = EntityRef.make_enum_property('goc_content_type', GCContentType)
    gc_language = EntityRef.make_enum_property('goc_languages', GCLanguage)

    def _auto_update_from_url(self):
        super()._auto_update_from_url()
        if self.gc_content_format is None:
            self.gc_content_format = GCContentFormat.Auto

    @property
    def gc_content_format(self):
        gc_format = self.get('goc_formats', None)
        if not gc_format:
            return None
        return GCContentFormat(gc_format[0]) if gc_format[0] is not None else None

    @gc_content_format.setter
    def gc_content_format(self, content_format: GCContentFormat = GCContentFormat.Auto):
        if content_format != GCContentFormat.Auto:
            self.set([content_format.value if content_format else None], 'goc_formats')
        else:
            self.set([Resource.autodetect_gc_content_format(self.url)], 'goc_formats')

    @staticmethod
    def autodetect_gc_content_format(full_url: t.Optional[dict]):
        if full_url is None:
            return None
        url = ""
        if 'und' in full_url:
            url = full_url['und']
        elif 'en' in full_url:
            url = full_url['en']
        else:
            for key in full_url.keys():
                url = full_url[key]
                break
        if url.upper().endswith(".TAR.GZ"):
            return GCContentFormat.ArchiveTARGZIP.value
        else:
            pos = None
            if "/" in url:
                pos = url.rfind("/")
            if "\\" in url:
                pos = url.rfind("\\") if pos is None else max(pos, url.rfind("\\"))
            filename = url if pos is None else url[pos+1:]
            if "." in filename:
                extension = filename[filename.rfind(".")+1:].upper()
                if any(x.value == extension for x in GCContentFormat):
                    return extension
            if url.startswith("https://") or url.startswith("http://"):
                return GCContentFormat.Hypertext.value
        return None


class _Contact(EntityRef):

    email = EntityRef.make_property('email', EntityRef.format_multilingual_text)
    service_hours = EntityRef.make_property('service_hours', EntityRef.format_multilingual_text)
    instructions = EntityRef.make_property('instructions', EntityRef.format_multilingual_text)
    resources = EntityRef.make_children_property('web_resources')
    id_description = EntityRef.make_property('id_description', EntityRef.format_multilingual_text)
    id_system = EntityRef.make_enum_property('id_system', IDSystem)
    id_code = EntityRef.make_property('id_code')

    def add_telephone_number(self, tel_type: TelephoneType, tel_num: str):
        """
        :param tel_type: The type of telephone number
        :param tel_num: The telephone number where this contact can be reached
        """
        if 'phone' not in self._metadata:
            self._metadata['phone'] = []
        self._metadata['phone'].append({
            'phone_number_type': tel_type.value,
            'phone_number': tel_num,
        })

    def set_address(self,
                    address_line: MultiLanguageString,
                    city: str,
                    province_state: MultiLanguageString,
                    country: Country,
                    postal_code: str):
        """
        :param address_line: Typically the street name and number along with any other additional information that would appear before the city. Use the new line character ("\n") to separate lines
        :param city: The city where the address is located
        :param province_state: The province, state, or other administrative area within a country where the city is located
        :param country: The country where the cityi s located
        """
        self._metadata.update({
            'delivery_point': EntityRef.format_multilingual_text(address_line),
            'city': city,
            'admin_area': EntityRef.format_multilingual_text(province_state),
            'country': country.value,
            'postal_code': postal_code
        })

    def set_web_page(self,
                     url: MultiLanguageString,
                     name: t.Optional[MultiLanguageString] = None,
                     description: t.Optional[MultiLanguageString] = None,
                     purpose: ResourcePurpose = ResourcePurpose.Information,
                     res_type: ResourceType = ResourceType.Auto):
        """
        :param url: The main URL where you can find out more information about the contact
        :param name: The name of the web page
        :param description: A description of the web page
        :param purpose: The purpose of the web page (defaults to Information)
        :param res_type: The type of resource (defaults to Auto to detect a web page or secure web page as appropriate - only change if you're using a weird protocol)
        """
        actual_url = EntityRef.format_multilingual_text(url)
        self._metadata['web_page'] = {
            'url': actual_url,
            'name': EntityRef.format_multilingual_text(name),
            'description': EntityRef.format_multilingual_text(description),
            'function': purpose.value,
            'protocol': res_type.value if res_type != ResourceType.Auto else Resource.autodetect_resource_type(actual_url)
        }



class Individual(_Contact):

    name = EntityRef.make_property('individual_name')
    orcid = EntityRef.make_id_property(IDSystem.ORCID, ['https://orcid.org/', 'http://orcid.org/'])


class Organization(_Contact):

    name = EntityRef.make_property('organization_name', EntityRef.format_multilingual_text)
    ror = EntityRef.make_id_property(IDSystem.ROR, ['https://ror.org/', 'http://ror.org/'])
    individuals = EntityRef.make_children_property('individuals')


class Position(_Contact):

    name = EntityRef.make_property('position_name', EntityRef.format_multilingual_text)


class _ResponsibleParty(EntityRef):

    def __init__(self, role: ContactRole, contact: _Contact):
        super().__init__()
        self._metadata['role'] = role.value
        self._children['contact'] = contact


class Citation(EntityRef):

    title = EntityRef.make_property('title', EntityRef.format_multilingual_text)
    alt_title = EntityRef.make_property('alt_title', EntityRef.format_multilingual_text)
    details = EntityRef.make_property('details', EntityRef.format_multilingual_text)
    edition = EntityRef.make_property('edition', EntityRef.format_multilingual_text)
    publication_date = EntityRef.make_property('publication_date', EntityRef.format_date)
    revision_date = EntityRef.make_property('revision_date', EntityRef.format_date)
    creation_date = EntityRef.make_property('creation_date', EntityRef.format_date)
    edition_date = EntityRef.make_property('edition_date', EntityRef.format_date)
    isbn = EntityRef.make_property('isbn')
    issn = EntityRef.make_property('issn')
    resource = EntityRef.make_child_property('resource')
    id_code = EntityRef.make_property('id_code')
    id_system = EntityRef.make_enum_property('id_system', IDSystem)
    id_description = EntityRef.make_property('id_description', EntityRef.format_multilingual_text)
    responsibles = EntityRef.make_children_property('responsibles')

    def add_contact(self, role: ContactRole, contact: _Contact):
        """
        :param role: The role the person plays for this citation
        :param contact: Their contact information
        """
        self.responsibles.append(_ResponsibleParty(role, contact))


class GeneralUseConstraint(EntityRef):

    description = EntityRef.make_property('description', EntityRef.format_multilingual_text)
    plain_text_version = EntityRef.make_property('plain_text', EntityRef.format_multilingual_text)
    citations = EntityRef.make_children_property('reference')
    responsibles = EntityRef.make_children_property('responsibles')

    def add_contact(self, role: ContactRole, contact: _Contact):
        """
        :param role: The role the contact plays for this use constraint.
        :param contact: The contact information
        """
        self.responsibles.append(_ResponsibleParty(role, contact))


class LegalConstraint(GeneralUseConstraint):

    access_constraints = EntityRef.make_enum_property('access_constraints', RestrictionCode, coerce_list=True)
    use_constraints = EntityRef.make_enum_property('use_constraints', RestrictionCode, coerce_list=True)
    other_constraints = EntityRef.make_property('other_constraints', EntityRef.format_multilingual_text)


class SecurityConstraint(GeneralUseConstraint):


    classification = EntityRef.make_enum_property('classification', ClassificationCode)
    user_notes = EntityRef.make_property('user_notes', EntityRef.format_multilingual_text)
    classification_system = EntityRef.make_property('classification_system', EntityRef.format_multilingual_text)


class ERDDAPServer(EntityRef):

    base_url = EntityRef.make_property('base_url')
    responsibles = EntityRef.make_children_property('responsibles')

    def add_contact(self, role: ContactRole, contact: _Contact):
        """
        :param role: The role the contact plays
        :param contact: The contact
        """
        self.responsibles.append(_ResponsibleParty(role, contact))


class Thesaurus(EntityRef):

    keyword_type = EntityRef.make_enum_property('type', KeywordType)
    prefix = EntityRef.make_property('prefix')
    citation = EntityRef.make_child_property('citation')


class Keyword(EntityRef):

    text = EntityRef.make_property('keyword', EntityRef.format_multilingual_text)
    description = EntityRef.make_property('description', EntityRef.format_multilingual_text)
    thesaurus = EntityRef.make_child_property('thesaurus')


class DistributionChannel(EntityRef):

    description = EntityRef.make_property('description', EntityRef.format_multilingual_text)
    primary_link = EntityRef.make_child_property('primary_web_link')
    links = EntityRef.make_children_property('links')
    responsibles = EntityRef.make_children_property('responsibles')

    def add_contact(self, role: ContactRole, contact: _Contact):
        self.responsibles.append(_ResponsibleParty(role, contact))


class DatasetMetadata(EntityRef):

    ontology: cnodc.ocproc2.ontology.OCProc2Ontology = None

    REPRESENTATION_MAP = {
        CommonDataModelType.TrajectoryProfile: SpatialRepresentation.TextTable,
        CommonDataModelType.Profile: SpatialRepresentation.TextTable,
        CommonDataModelType.TimeSeries: SpatialRepresentation.TextTable,
        CommonDataModelType.Trajectory: SpatialRepresentation.TextTable,
        CommonDataModelType.TimeSeriesProfile: SpatialRepresentation.TextTable,
        CommonDataModelType.Point: SpatialRepresentation.TextTable,
        CommonDataModelType.Grid: SpatialRepresentation.Grid,
        CommonDataModelType.MovingGrid: SpatialRepresentation.Grid,
    }


    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._authority: t.Optional[str] = None
        self._act_workflow: t.Optional[str] = None
        self._pub_workflow: t.Optional[str] = None
        self._security_level: t.Optional[str] = None
        self._org_name: t.Optional[str] = None
        self._users: set[str] = set()
        self._profiles: set[str] = set()
        self._profiles.add('cnodc')
        self._log = logging.getLogger("cnodc.dmd.metadata")

    erddap_servers = EntityRef.make_children_property('erddap_servers')
    distributors = EntityRef.make_children_property('distributors')
    variables = EntityRef.make_children_property('variables')
    custom_keywords = EntityRef.make_children_property('custom_keywords')
    goc_publisher = EntityRef.make_enum_property('goc_publisher', GCPublisher)
    data_constraints = EntityRef.make_children_property('licenses')
    metadata_constraints = EntityRef.make_children_property('metadata_licenses')
    responsibles = EntityRef.make_children_property('responsibles')
    metadata_owner = EntityRef.make_child_property('metadata_owner')
    publisher = EntityRef.make_child_property('publisher')
    parent_metadata = EntityRef.make_child_property('parent_metadata')
    alt_metadata_citations = EntityRef.make_children_property('alt_metadata')
    metadata_standards = EntityRef.make_children_property('metadata_standards')
    metadata_profiles = EntityRef.make_children_property('metadata_profiles')
    additional_docs = EntityRef.make_children_property('additional_docs')
    maintenance_records = EntityRef.make_children_property('iso_maintenance')
    canon_urls = EntityRef.make_children_property('canon_urls')
    title = EntityRef.make_property('title', EntityRef.format_multilingual_text)
    institution = EntityRef.make_property('institution')
    program = EntityRef.make_property('program')
    project = EntityRef.make_property('project')
    conventions = EntityRef.make_property('conventions', lambda x: ','.join(x))
    cf_standard_name_vocab = EntityRef.make_property('standard_name_vocab')
    credit = EntityRef.make_property('acknowledgement', EntityRef.format_multilingual_text)
    comment = EntityRef.make_property('comment', EntityRef.format_multilingual_text)
    id_code = EntityRef.make_property('dataset_id_code')
    id_system = EntityRef.make_enum_property('dataset_id_system', IDSystem)
    id_description = EntityRef.make_property('dataset_id_description', EntityRef.format_multilingual_text)
    doi = EntityRef.make_id_property(IDSystem.DOI, ['https://doi.org/', 'http://doi.org/', 'doi:'])
    processing_level = EntityRef.make_property('processing_level')
    processing_description = EntityRef.make_property('processing_description', EntityRef.format_multilingual_text)
    processing_environment = EntityRef.make_property('processing_environment', EntityRef.format_multilingual_text)
    processing_system = EntityRef.make_enum_property('processing_system', IDSystem)
    purpose = EntityRef.make_property('purpose', EntityRef.format_multilingual_text)
    references = EntityRef.make_property('references', EntityRef.format_multilingual_text)
    source = EntityRef.make_property('source')
    abstract = EntityRef.make_property('summary')
    geospatial_lat_min = EntityRef.make_property('geospatial_lat_min', unnumpy)
    geospatial_lon_min = EntityRef.make_property('geospatial_lat_max', unnumpy)
    geospatial_lat_max = EntityRef.make_property('geospatial_lon_min', unnumpy)
    geospatial_lon_max = EntityRef.make_property('geospatial_lon_max', unnumpy)
    geospatial_bounds = EntityRef.make_property('geospatial_bounds')
    geospatial_crs = EntityRef.make_enum_property('geospatial_bounds_crs', CoordinateReferenceSystem)
    geospatial_vertical_min = EntityRef.make_property('geospatial_vertical_min', unnumpy)
    geospatial_vertical_max = EntityRef.make_property('geospatial_vertical_max', unnumpy)
    geospatial_vertical_crs = EntityRef.make_enum_property('geospatial_bounds_vertical_crs', CoordinateReferenceSystem)
    time_coverage_start = EntityRef.make_property('time_coverage_start', EntityRef.format_date)
    time_coverage_end = EntityRef.make_property('time_coverage_end', EntityRef.format_date)
    is_ongoing = EntityRef.make_property('is_ongoing', bool)
    temporal_crs = EntityRef.make_enum_property('temporal_crs', CoordinateReferenceSystem)
    date_issued = EntityRef.make_property('date_issued', EntityRef.format_date)
    date_created = EntityRef.make_property('date_created', EntityRef.format_date)
    date_modified = EntityRef.make_property('date_modified', EntityRef.format_date)
    primary_data_locale = EntityRef.make_enum_property('data_locale', Locale)
    secondary_data_locales = EntityRef.make_enum_property('data_extra_locales', Locale, coerce_list=True)
    primary_metadata_locale = EntityRef.make_enum_property('metadata_locale', Locale)
    secondary_metadata_locales = EntityRef.make_enum_property('metadata_extra_locales', Locale, coerce_list=True)
    metadata_maintenance_frequency = EntityRef.make_enum_property('metadata_maintenance_frequency', MaintenanceFrequency)
    data_maintenance_frequency = EntityRef.make_enum_property('resource_maintenance_frequency', MaintenanceFrequency)
    topic_category = EntityRef.make_enum_property('topic_category', TopicCategory)
    status = EntityRef.make_enum_property('status', StatusCode)
    spatial_representation = EntityRef.make_enum_property('spatial_representation_type', SpatialRepresentation)
    file_storage_location = EntityRef.make_property('file_storage_location', EntityRef.format_multilingual_text)
    internal_notes = EntityRef.make_property('internal_notes', EntityRef.format_multilingual_text)
    is_available_via_meds_request_form = EntityRef.make_property('via_meds_request_form', bool)
    goc_publication_places = EntityRef.make_enum_property('goc_publication_place', GCPlace, coerce_list=True)
    goc_audiences = EntityRef.make_enum_property('goc_audience', GCAudience, coerce_list=True)
    goc_collection = EntityRef.make_enum_property('goc_collection_type', GCCollectionType)
    goc_subject = EntityRef.make_enum_property('goc_subject', GCSubject)

    @property
    def feature_type(self):
        return CommonDataModelType.from_string(self.get('feature_type', ''))

    @feature_type.setter
    def feature_type(self, feature_type: t.Union[str,CommonDataModelType]):
        if isinstance(feature_type, str):
            feature_type = CommonDataModelType.from_string(feature_type)
        self.set(feature_type.value if feature_type else None, 'feature_type')
        if feature_type in DatasetMetadata.REPRESENTATION_MAP and self.spatial_representation is None:
            self.spatial_representation = DatasetMetadata.REPRESENTATION_MAP[feature_type]

    @property
    def cf_standard_names(self):
        if 'cf_standard_names' not in self._metadata:
            self._metadata['cf_standard_names'] = []
        return self.get('cf_standard_names')

    def set_spatial_resolution(self,
                               scale: t.Optional[int] = None,
                               level_of_detail: t.Optional[MultiLanguageString] = None,
                               horizontal: t.Optional[NumberLike] = None,
                               vertical: t.Optional[NumberLike] = None,
                               angular: t.Optional[NumberLike] = None,
                               horizontal_units: DistanceUnit = DistanceUnit.Meters,
                               vertical_units: DistanceUnit = DistanceUnit.Meters,
                               angular_units: AngularUnit = AngularUnit.ArcDegrees,
                               ):
        """
        :param scale: The spatial resolution in 1:[SCALE] format (e.g. use 10000 for a 1:10000 scale)
        :param level_of_detail: A text description of the level of detail
        :param horizontal: The horizontal resolution of the dataset in the given units
        :param vertical: The vertical resolution of the dataset in the given units
        :param angular: The angular resolution of the dataset in the given units
        :param horizontal_units: Units for horizontal resolution (defaults to metres)
        :param vertical_units: Units for the vertical resolution (defaults to metres)
        :param angular_units: Units for the angular resolution (defaults to arc degrees)
        """
        self._metadata['spatial_resolution'] = {}
        if scale is not None:
            self._metadata['spatial_resolution']['scale'] = scale
        if level_of_detail is not None:
            self._metadata['spatial_resolution']['level_of_detail'] = EntityRef.format_multilingual_text(level_of_detail)
        if horizontal is not None:
            self._metadata['spatial_resolution']['distance'] = str(horizontal)
            self._metadata['spatial_resolution']['distance_units'] = horizontal_units.value
        if vertical is not None:
            self._metadata['spatial_resolution']['vertical'] = str(vertical)
            self._metadata['spatial_resolution']['vertical_units'] = vertical_units.value
        if angular is not None:
            self._metadata['spatial_resolution']['angular'] = str(angular)
            self._metadata['spatial_resolution']['angular_units'] = angular_units.value

    def add_cf_standard_name(self, keyword: t.Union[str, StandardName]):
        """
        :param keyword: A keyword to add
        """
        if hasattr(keyword, 'value'):
            keyword = keyword.value
        if 'cf_standard_names' not in self._metadata:
            self._metadata['cf_standard_names'] = list()
        if keyword not in self._metadata['cf_standard_names']:
            self._metadata['cf_standard_names'].append(keyword)

    def set_time_resolution_from_iso(self, iso_duration: str):
        tcr = iso_duration.replace("-", "").replace(":", "").upper()
        if tcr[0] != 'P':
            raise ValueError('ISO formats begin with a P')
        else:
            parts = [0, 0, 0, 0, 0, 0]
            weeks = 0
            buffer = ''
            in_time = False
            used_alt_format = False
            for i in range(1, len(tcr)):
                if tcr[i].isdigit():
                    buffer += tcr[i]
                elif tcr[i] == 'T':
                    in_time = True
                    if buffer:
                        if len(buffer) != 8:
                            raise ValueError(f'Invalid alternate duration date length')
                        parts[0] = int(buffer[0:4])
                        parts[1] = int(buffer[4:6])
                        parts[2] = int(buffer[6:8])
                        buffer = ''
                        used_alt_format = True
                elif tcr[i] == 'Y':
                    parts[0] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'M':
                    parts[4 if in_time else 1] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'D':
                    parts[2] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'H':
                    parts[3] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'S':
                    parts[5] = int(buffer)
                    buffer = ''
                elif tcr[i] == 'W':
                    weeks = int(buffer)
                    buffer = ''
                else:
                    raise ValueError(f'Invalid character found at position [{i}] in [{tcr}]')
            if buffer and used_alt_format:
                if len(buffer) in (2, 4, 6):
                    parts[3] = int(buffer[0:2])
                else:
                    raise ValueError(f'Invalid alternate duration time length [{buffer}]')
                if len(buffer) in (4, 6):
                    parts[4] = int(buffer[2:4])
                if len(buffer) == 6:
                    parts[5] = int(buffer[4:6])
            if weeks > 0:
                if any(x > 0 for x in parts):
                    raise ValueError('Cannot specify weeks and other time parts')
                self.set_time_resolution(0, 0, weeks * 7)
            else:
                self.set_time_resolution(*parts)

    def set_time_resolution(self,
                            years: t.Optional[int] = None,
                            months: t.Optional[int] = None,
                            days: t.Optional[int] = None,
                            hours: t.Optional[int] = None,
                            minutes: t.Optional[int] = None,
                            seconds: t.Optional[int] = None):
        """
        """
        self._metadata['temporal_resolution'] = {
            'years': years or None,
            'months': months or None,
            'days': days or None,
            'hours': hours or None,
            'minutes': minutes or None,
            'seconds': int(seconds) if seconds else None
        }

    def add_essential_ocean_variable(self, keyword: t.Union[str, EssentialOceanVariable]):
        """
        :param keyword: The essential ocean variable to add to this dataset.
        """
        if hasattr(keyword, 'value'):
            keyword = keyword.value
        if 'cioos_eovs' not in self._metadata:
            self._metadata['cioos_eovs'] = list()
        if keyword not in self._metadata['cioos_eovs']:
            self._metadata['cioos_eovs'].append(keyword)

    def set_erddap_info(self, server: t.Union[ERDDAPServer, list[ERDDAPServer]], dataset_id: str, dataset_type: ERDDAPDatasetType, file_path: t.Optional[str] = None, file_pattern: t.Optional[str] = None):
        """
        :param server: The server(s) that will host the dataset
        :param dataset_id: The ID of the dataset as it should be used in ERDDAP (must be unique)
        :param file_path: The path of the files on the ERDDAP server
        :param file_pattern: If multiple files are stored in that path, the file pattern to match (otherwise all files are used)
        """
        self.add_profile('erddap')
        if isinstance(server, ERDDAPServer):
            self.erddap_servers.append(server)
        else:
            self.erddap_servers.extend(server)
        self._metadata['erddap_data_file_path'] = file_path
        self._metadata['erddap_data_file_pattern'] = file_pattern
        self._metadata['erddap_dataset_id'] = dataset_id
        self._metadata['erddap_dataset_type'] = dataset_type.value

    def set_meds_defaults(self):
        self.metadata_constraints.append(Common.Constraint_Unclassified)
        self.metadata_constraints.append(Common.Constraint_OpenGovernmentLicense)
        self.data_constraints.append(Common.Constraint_Unclassified)
        self.data_constraints.append(Common.Constraint_OpenGovernmentLicense)
        self.metadata_profiles.append(Common.MetadataProfile_CIOOS)
        self.metadata_standards.append(Common.MetadataStandard_ISO19115)
        self.metadata_standards.append(Common.MetadataStandard_ISO191151)
        self.metadata_owner = Common.Contact_CNODC
        self.publisher = Common.Contact_CNODC
        self.metadata_maintenance_frequency = MaintenanceFrequency.NotPlanned
        self.data_maintenance_frequency = MaintenanceFrequency.NotPlanned
        self.status = StatusCode.Final
        self.topic_category = TopicCategory.Oceans
        self.set_activation_workflow("cnodc_activation")
        self.set_publication_workflow("cnodc_publish")
        self.cf_standard_name_vocab = "CF 1.13"
        self._security_level = 'unclassified'
        self.goc_publisher = GCPublisher.MEDS
        self.goc_subject = GCSubject.Oceanography
        self.goc_publication_places = [GCPlace.Ottawa]
        self.goc_audiences = [GCAudience.Scientists]
        self.goc_collection = GCCollectionType.Geospatial

    def set_from_netcdf_file(self, dataset: nc.Dataset, default_lang: str = 'en'):
        attrs = { x: dataset.getncattr(x) for x in dataset.ncattrs()}
        locale_map = self._set_locales_from_netcdf(attrs, default_lang)
        depths = self._identify_levels(dataset)
        for ds_var in dataset.variables:
            self.add_variable(Variable.build_from_netcdf(dataset.variables[ds_var], locale_map), depths)
        title = get_bilingual_attribute(attrs, 'title', locale_map)
        self.title = title
        self.display_name = title
        self.program = attrs.pop('program', None)
        self.project = attrs.pop('project', None)
        self.institution = attrs.pop('institution', None)
        self.guid = attrs.pop('id', None)
        self.feature_type = attrs.pop('featureType', None)
        self.processing_level = attrs.pop('processing_level', None)
        self.geospatial_bounds = attrs.pop('geospatial_bounds', None)
        self.conventions = attrs.pop('Conventions', "").split(",")
        self.processing_description = get_bilingual_attribute(attrs, 'processing_description', locale_map)
        self.processing_environment = get_bilingual_attribute(attrs, 'processing_environment', locale_map)
        self.credit = get_bilingual_attribute(attrs, 'acknowledgement', locale_map)
        self.comment = get_bilingual_attribute(attrs, 'comment', locale_map)
        self.references = get_bilingual_attribute(attrs, 'references', locale_map)
        self.source = get_bilingual_attribute(attrs, 'source', locale_map)
        self.abstract = get_bilingual_attribute(attrs, 'summary', locale_map)
        self.purpose = get_bilingual_attribute(attrs, 'purpose', locale_map)
        if 'standard_name_vocabulary' in attrs and attrs['standard_name_vocabulary']:
            self.cf_standard_name_vocab = attrs.pop('standard_name_vocabulary')
        if 'date_issued' in attrs and attrs['date_issued']:
            self.date_issued = awaretime.utc_from_isoformat(attrs.pop('date_issued'))
        if 'date_created' in attrs and attrs['date_created']:
            self.date_created = awaretime.utc_from_isoformat(attrs.pop('date_created'))
        if 'date_modified' in attrs and attrs['date_modified']:
            self.date_modified =awaretime.utc_from_isoformat(attrs.pop('date_modified'))
        if 'data_maintenance_frequency' in attrs and attrs['data_maintenance_frequency']:
            self.data_maintenance_frequency = attrs.pop('data_maintenance_frequency')
        if 'metadata_maintenance_frequency' in attrs and attrs['metadata_maintenance_frequency']:
            self.metadata_maintenance_frequency = attrs.pop('metadata_maintenance_frequency')
        if 'status' in attrs and attrs['status']:
            self.status = attrs.pop('status')
        if 'topic_category' in attrs and attrs['topic_category']:
            self.topic_category = attrs.pop('topic_category')
        if 'gc_audiences' in attrs and attrs['gc_audiences']:
            self.goc_audiences = attrs.pop('gc_audiences', '').split(';')
        if 'gc_subject' in attrs and attrs['gc_subject']:
            self.goc_subject = attrs.pop('gc_subject')
        if 'gc_publication_places' in attrs and attrs['gc_publication_places']:
            self.goc_publication_places = attrs.pop('gc_publication_places', '').split(';')
        info_url = get_bilingual_attribute(attrs, 'infoUrl', locale_map)
        if info_url:
            self.set_info_link(info_url)
        if 'doi' in attrs and attrs['doi']:
            self.doi = attrs.pop('doi', '')
        md_link = get_bilingual_attribute(attrs, 'metadata_link', locale_map)
        if md_link:
            cit = Citation()
            res = Resource(
                url=md_link,
                link_purpose=ResourcePurpose.CompleteMetadata,
                gc_content_type = GCContentType.SupportingDocumentation
            )
            cit.resource = res
            self.alt_metadata_citations.append(cit)
        if 'time_coverage_resolution' in attrs and attrs['time_coverage_resolution']:
            try:
                self.set_time_resolution_from_iso(attrs.pop('time_coverage_resolution'))
            except ValueError as ex:
                self._log.exception("Invalid value for time_coverage_resolution")
        if 'geospatial_bounds_crs' in attrs and attrs['geospatial_bounds_crs']:
            self.geospatial_crs = attrs.pop('geospatial_bounds_crs')
        if 'geospatial_bounds_vertical_crs' in attrs and attrs['geospatial_bounds_vertical_crs']:
            self.geospatial_vertical_crs = attrs.pop('geospatial_bounds_vertical_crs')
        self._build_from_netcdf_contacts(locale_map, attrs, 'creator', contact_default_role=ContactRole.Originator, contact_default_type='individual')
        self._build_from_netcdf_contacts(locale_map, attrs, 'publisher', contact_default_role=ContactRole.Publisher, contact_default_type='individual')
        self._build_from_netcdf_contacts(locale_map, attrs, 'contributor', contact_default_role=ContactRole.Contributor, contact_default_type='individual')
        self._build_from_netcdf_contacts(locale_map, attrs, 'contributing_institutions', contact_default_role=ContactRole.Contributor, contact_default_type='institution')
        self.update_additional_properties(attrs)

    def _set_locales_from_netcdf(self, attrs: dict, default_lang: str):
        locale_map = {}
        primary_locale = Locale.CanadianEnglish
        secondary_locales = []
        if 'default_locale' in attrs:
            default_locale = attrs.pop('default_locale')
            if '-' in default_locale:
                default_locale = default_locale[:default_locale.find('-')]
            locale_map[''] = default_locale
            if default_locale.startswith("fr"):
                primary_locale = Locale.CanadianFrench
                secondary_locales.append(Locale.CanadianEnglish)
            else:
                secondary_locales.append(Locale.CanadianFrench)
        else:
            locale_map[''] = default_lang
            if default_lang == 'fr':
                primary_locale = Locale.CanadianFrench
                secondary_locales.append(Locale.CanadianEnglish)
            else:
                secondary_locales.append(Locale.CanadianFrench)
        self.primary_data_locale = primary_locale
        self.primary_metadata_locale = primary_locale
        self.secondary_metadata_locales = secondary_locales
        self.secondary_data_locales = secondary_locales
        if 'locales' in attrs:
            for locale in attrs.pop('locales').split(','):
                suffix, bcptag = locale.split(':', maxsplit=1)
                if '-' in bcptag:
                    bcptag, _ = bcptag.split('-', maxsplit=1)
                locale_map[suffix.strip()] = bcptag.strip()
        else:
            locale_map['_en'] = 'en'
            locale_map['_fr'] = 'fr'
        return locale_map

    def _identify_levels(self, dataset: nc.Dataset):
        depths = []
        for var_name in dataset.variables:
            ds_var = dataset.variables[var_name]
            if hasattr(ds_var, 'axis') and ds_var.axis == 'Z':
                vals: np.array = ds_var[:]
                if vals.size == 0:
                    continue
                min_z, max_z = unnumpy(np.min(vals)), unnumpy(np.max(vals))
                if min_z is None and max_z is None:
                    continue
                depths = ['seaSurface']
                if hasattr(ds_var, 'positive') and ds_var.positive == 'up':
                    min_z, max_z = max_z * -1, min_z * -1
                if min_z > 0:
                    depths = ['subSurface']
                elif max_z > 0:
                    depths.append('subSurface')
                break
        return depths

    def _build_from_netcdf_contacts(self, locale_map, attrs: dict, prefix: str, **kwargs):
        self._add_netcdf_contacts(
            **kwargs,
            names=get_bilingual_attribute(attrs, f"{prefix}_name", locale_map),
            emails=get_bilingual_attribute(attrs, f"{prefix}_email", locale_map),
            ids=attrs.pop(f"{prefix}_id", ""),
            urls=get_bilingual_attribute(attrs, f"{prefix}_url", locale_map),
            institutions=get_bilingual_attribute(attrs, f"{prefix}_institution", locale_map),
            specific_roles=attrs.pop(f"{prefix}_role", ""),
            specific_types=attrs.pop(f"{prefix}_type", ""),
            id_vocabulary=attrs.pop(f"{prefix}_id_vocabulary", ""),
        )

    def _add_netcdf_contacts(self,
                             contact_default_role: ContactRole,
                             contact_default_type: str,
                             names: dict[str, str],
                             emails: dict[str, str],
                             ids: str,
                             urls: dict[str, str],
                             institutions: dict[str, str],
                             specific_roles: str,
                             specific_types: str,
                             id_vocabulary: str):
        def split_multilingual_attribute(attr: dict[str, str], split_on: str = ",") -> list[dict[str, str]]:
            split_attrs = {
                key: attr[key].split(split_on)
                for key in attr
            }
            values = []
            if split_attrs:
                # TODO: double quote extraction.
                for x in range(0, max(len(split_attrs[key]) for key in split_attrs)):
                    values.append({
                        key: split_attrs[key][x]
                        for key in split_attrs
                        if x < len(split_attrs[key]) and split_attrs[key][x]
                    })

            return values

        names = split_multilingual_attribute(names)
        emails = split_multilingual_attribute(emails)
        ids = ids.split(",")
        urls = split_multilingual_attribute(urls)
        institutions = split_multilingual_attribute(institutions)
        specific_roles = specific_roles.split(",")
        specific_types = specific_types.split(",")
        id_vocabulary = id_vocabulary.split(',') if ',' in id_vocabulary else [id_vocabulary for _ in names]
        for idx in range(0, len(names)):
            self._add_netcdf_contact(
                name=names[idx],
                email=emails[idx] if idx < len(emails) else None,
                contact_id=ids[idx] if idx < len(ids) else None,
                url=urls[idx] if idx < len(urls) else None,
                institution=institutions[idx] if idx < len(institutions) else None,
                role=specific_roles[idx] if idx < len(specific_roles) and specific_roles[idx] else contact_default_role,
                contact_type=specific_types[idx] if idx < len(specific_types) and specific_types[idx] else contact_default_type,
                id_vocabulary=id_vocabulary[idx]
            )

    def _add_netcdf_contact(self,
                            name: MultiLanguageString,
                            email: t.Optional[MultiLanguageString],
                            contact_id: t.Optional[str],
                            url: t.Optional[MultiLanguageString],
                            institution: t.Optional[MultiLanguageString],
                            role: t.Union[str, ContactRole],
                            contact_type: str,
                            id_vocabulary: str):
        if contact_type == 'institution' or contact_type == 'group':
            contact = Organization(name=name)
            if contact_id is not None and contact_id != "":
                if id_vocabulary is None or id_vocabulary == "" or id_vocabulary.lower().startswith("https://ror.org"):
                    contact.ror = contact_id
                else:
                    self._log.warning(f"Unknown ID vocabulary for organization: {id_vocabulary}")
        elif contact_type == 'position':
            contact = Position(name=name)
            if contact_id:
                self._log.warning(f"ID provided for position: {contact_id} [{id_vocabulary}]")
        else:
            if isinstance(name, dict):
                if 'und' in name and name['und']:
                    name = name['und']
                elif 'en' in name and name['en']:
                    name = name['en']
                else:
                    for k in name:
                        if name[k]:
                            name = name[k]
                            break
                    else:
                        name = None
            contact = Individual(name=name)
            if contact_id is not None and contact_id != "":
                if id_vocabulary is None or id_vocabulary == "" or id_vocabulary.lower().startswith("https://orcid.org"):
                    contact.orcid = contact_id
                else:
                    self._log.warning(f"Unknown ID vocabulary for individual: {id_vocabulary}")
        if url is not None:
            contact.set_web_page(url)
        if email is not None:
            if isinstance(email, str):
                contact.guid = email
            elif 'und' in email:
                contact.guid = email['und']
            elif 'en' in email:
                contact.guid = email['en']
            elif 'fr' in email:
                contact.guid = email['fr']
            contact.email = email
        # There's no ISO support for institution, but maybe we should add one?
        if isinstance(role, str):
            role = ContactRole.from_string(role)
        if role is None:
            self._log.warning(f"Missing contact role for [{name}]")
        else:
            self.add_contact(role, contact)

    def add_variable(self, var: Variable, eov_prefixes: t.Optional[list[str]] = None):
        """
        :param var: A variable to add to the dataset
        :param eov_prefixes: Provide either or both of "seaSurface" or "subSurface" to set the EOVs properly. Otherwise you'll need to set the EOVs manually depending on the depths sampled.
        """
        self.variables.append(var)
        cnodc_name = var.cnodc_name
        if cnodc_name:
            if '/' in cnodc_name:
                pieces = cnodc_name.rsplit("/")
                cnodc_name = pieces[-1]
            if self.ontology.exists(cnodc_name):
                element = self.ontology.info(cnodc_name)
                if not element.ioos_category:
                    ioos_cat = IOOSCategory.Other
                else:
                    ioos_cat = IOOSCategory.from_string(element.ioos_category)
                var.ioos_category = ioos_cat
                if element.essential_ocean_vars:
                    if len(element.essential_ocean_vars) == 1:
                        self.add_essential_ocean_variable(EssentialOceanVariable(list(element.essential_ocean_vars)[0]))
                    elif eov_prefixes:
                        for eov in element.essential_ocean_vars:
                            if any(eov.startswith(x) for x in eov_prefixes):
                                self.add_essential_ocean_variable(EssentialOceanVariable(eov))
            else:
                var.ioos_category = IOOSCategory.Other
        if var.standard_name is not None:
            self.add_cf_standard_name(var.standard_name)
        axis = var.axis
        if axis == Axis.Longitude:
            self.geospatial_lon_min = var.actual_min
            self.geospatial_lon_max = var.actual_max
        elif axis == Axis.Latitude:
            self.geospatial_lat_min = var.actual_min
            self.geospatial_lat_max = var.actual_max
        elif axis == Axis.Depth:
            self.geospatial_vertical_min = var.actual_min
            self.geospatial_vertical_max = var.actual_max
        elif axis == Axis.Time:
            units = var.units
            if ' since ' in units.lower():
                pieces = units.lower().split(' ')
                try:
                    epoch = awaretime.utc_from_isoformat(pieces[2].upper())
                    match pieces[0]:
                        case 'seconds':
                            self.time_coverage_start = epoch + datetime.timedelta(seconds=var.actual_min)
                            self.time_coverage_end = epoch + datetime.timedelta(seconds=var.actual_max)
                        case 'minutes':
                            self.time_coverage_start = epoch + datetime.timedelta(minutes=var.actual_min)
                            self.time_coverage_end = epoch + datetime.timedelta(minutes=var.actual_max)
                        case 'hours':
                            self.time_coverage_start = epoch + datetime.timedelta(hours=var.actual_min)
                            self.time_coverage_end = epoch + datetime.timedelta(hours=var.actual_max)
                        case 'days':
                            self.time_coverage_start = epoch + datetime.timedelta(days=var.actual_min)
                            self.time_coverage_end = epoch + datetime.timedelta(days=var.actual_max)
                        case _:
                            self._log.warning(f"Unsupported time increment {units}")
                except Exception as ex:
                    self._log.exception(f"Exception handling time units: [{units}]: {type(ex)}: {str(ex)}")
            else:
                self._log.warning(f"Unrecognized time units {units}")

    def set_info_link(self,
                      url: MultiLanguageString,
                      name: t.Optional[MultiLanguageString] = None,
                      description: t.Optional[MultiLanguageString] = None,
                      function: ResourcePurpose = ResourcePurpose.Information,
                      protocol: ResourceType = ResourceType.Auto):
        self._children['info_link'] = QuickWebPage(
            url=url,
            name=name,
            description=description,
            purpose=function,
            resource_type=protocol
        )

    def update_additional_properties(self, attrs: dict):
        if 'custom_metadata' not in self._metadata:
            self._metadata['custom_metadata'] = {}
        for key in attrs:
            val = unnumpy(attrs[key])
            if val is not None and val != '':
                self._metadata['custom_metadata'][key] = val

    def add_contact(self, role: ContactRole, contact: _Contact):
        """
        :param role: The nature of the relationship
        :param contact: A person who has some relationship to this dataset
        """
        self.responsibles.append(_ResponsibleParty(role, contact))

    def add_profile(self, profile_name):
        """
        :param profile_name: The name of a profile in DMD.
        """
        self._profiles.add(profile_name)

    def set_activation_workflow(self, workflow_name):
        """
        :param workflow_name: The name of an activation workflow in DMD.
        """
        self._act_workflow = workflow_name

    def set_publication_workflow(self, workflow_name):
        """
        :param workflow_name: The name of a publication workflow in DMD.
        """
        self._pub_workflow = workflow_name

    def add_user(self, username: str):
        """
        :param username: A username from DMD.
        """
        self._users.add(username)

    def set_parent_organization(self, org_name: str):
        """
        :param org_name: The short name of an organization in DMD.
        """
        self._org_name = org_name

    def build_request_body(self) -> dict:
        body = {}
        body['metadata'] = super().build_request_body()
        for key in ('_guid', '_display_names'):
            if key in body['metadata']:
                body[key[1:]] = body['metadata'][key]
                del body['metadata'][key]
        body.update({
            'authority': self._authority,
            'profiles': list(self._profiles),
            'org_name': self._org_name,
            'users': list(self._users),
            'activation_workflow': self._act_workflow,
            'publication_workflow': self._pub_workflow,
            'security_level': self._security_level,
        })
        return body


class Common:

    Constraint_OpenGovernmentLicense = LegalConstraint(guid="open_government_license")
    Constraint_Unclassified = SecurityConstraint(guid="unclassified_data")

    Contact_CNODC = Organization(guid="cnodc")
    Contact_DFO = Organization(guid="dfo")

    ERDDAP_Primary = ERDDAPServer(guid="cnodc_primary")

    MetadataStandard_ISO19115 = Citation(guid="metadata_standard_iso19115")
    MetadataStandard_ISO191151 = Citation(guid="metadata_standard_iso19115-1")

    MetadataProfile_CIOOS = Citation(guid="metadata_profile_cioos")




