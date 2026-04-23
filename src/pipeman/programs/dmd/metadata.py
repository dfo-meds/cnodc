import functools
import logging
import typing as t
import datetime

import netCDF4 as nc
import numpy as np
import enum

from autoinject import injector

from medsutil.first import first_i18n
from medsutil.sanitize import unnumpy
import medsutil.awaretime as awaretime
import medsutil.datadict as dd
from medsutil.multienum import MultiValuedEnum, variants
from medsutil.frozendict import FrozenDict
from medsutil.types import *
from medsutil.ocproc2.ontology import OCProc2Ontology

def get_bilingual_attribute(attribute_dict, attribute_name, locale_map):
    attr = {}
    for suffix in locale_map.keys():
        if f"{attribute_name}{suffix}" in attribute_dict:
            attr[locale_map[suffix]] = attribute_dict.pop(f"{attribute_name}{suffix}")
    return attr


class Encoding(MultiValuedEnum):

    UTF8 = "utf8", "utf-8"
    ISO_8859_1 = "iso-8859-1"
    UTF16 = "utf16", "utf-16"


class Axis(MultiValuedEnum):

    Time = 'T'
    Longitude = 'X'
    Latitude = 'Y'
    Depth = 'Z'

    @classmethod
    def _convert_value(cls, v):
        return str(v).upper()


class NetCDFDataType(MultiValuedEnum):
    String = "String"
    Character = "char", 'c', 'S1'
    Double = "double", "float64", "d", "f8"
    Float = "float", "float32", "f4", "f"
    Long = "long", "i8", "int64"
    LongUnsigned = "ulong", "u8", "uint64"
    Integer = "int", "i4", "i", "int32"
    IntegerUnsigned = "uint", "u4", "uint32"
    Short = "short", "i2", "int16", "s", "h"
    ShortUnsigned = "ushort", "u2", "uint16"
    Byte = "byte", "i1", "b", "B", "int8"
    ByteUnsigned = "ubyte", "u1", "uint8"

    @classmethod
    def _convert_value(cls, v):
        if v is str:
            v = "String"
        if isinstance(v, np.dtype):
            v = v.name
        elif isinstance(v, nc.VLType):
            v = v.dtype.name
        if isinstance(v, str) and len(v) >= 2 and v[0] == 'S' and v[1:].isdigit():
            v = 'char' if v[1:] == '1' else 'String'
        return v


class Calendar(MultiValuedEnum):

    Standard = "standard", "gregorian"
    ProlepticGregorian = "proleptic_gregorian"
    Julian = "julian"
    Days365 = "noleap"
    Days366 = "all_leap"
    Days360 = "360_day"
    Nonstandard = "nonstandard"

    @classmethod
    def _convert_value(cls, v):
        return str(v).lower()


class Direction(MultiValuedEnum):

    Up = "up"
    Down = "down"


class IOOSCategory(MultiValuedEnum):

    Acidity = variants("Acidity")
    Bathymetry = variants("Bathymetry")
    Biology = variants("Biology")
    BottomCharacter = variants("Bottom Character")
    CarbonDioxide = variants("CO2")
    ColoredDissolvedOrganicMatter = variants("Color Dissolved Organic Matter")
    Contaminants = variants("Contaminants")
    Currents = variants("Currents")
    DissolvedNutrients = variants("Dissolved Nutrients")
    DissolvedOxygen = variants("Dissolved O2")
    Ecology = variants("Ecology")
    FishAbundance = variants("Fish Abundance")
    FishSpecies = variants("Fish Species")
    HeatFlux = variants("Heat Flux")
    Hydrology = variants("Hydrology")
    IceDistribution = variants("Ice Distribution")
    Identifier = variants("Identifier")
    Location = variants("Location")
    Meteorology = variants("Meteorology")
    OceanColor = variants("Ocean Color")
    OpticalProperties = variants("Optical Properties")
    Other = variants("Other")
    Pathogens = variants("Pathogens")
    PhytoplanktonSpecies = variants("PhytoplanktonSpecies")
    Pressure = variants("Pressure")
    Productivity = variants("Productivity")
    Quality = variants("Quality")
    Salinity = variants("Salinity")
    SeaLevel = variants("Sea Level")
    Statistics = variants("Statistics")
    StreamFlow = variants("Stream Flow")
    SurfaceWaves = variants("SurfaceWaves")
    Taxonomy = variants("Taxonomy")
    Temperature = variants("Temperature")
    Time = variants("Time")
    TotalSuspendedMatter = variants("Total Suspended Matter")
    Unknown = variants("Unknown")
    Wind = variants("Wind")
    ZooplanktonSpecies = variants("Zooplankton Species")
    ZooplanktonAbundance = variants("Zooplankton Abundance")



class TimePrecision(MultiValuedEnum):
    Month = "month"
    Day = "day"
    Hour = "hour"
    Minute = "minte"
    Second = "second"
    TenthSecond = "tenth_second"
    HundredthSecond = "hundredth_second"
    Millisecond = "millisecond"


class NumericTimeUnits(MultiValuedEnum):

    Years = "years"
    Months = "months"
    Weeks = "weeks"
    Days = "days"
    Hours = "hours"
    Minutes = "minutes"
    Seconds = "seconds"
    Milliseconds = "milliseconds"


class TimeZone(MultiValuedEnum):
    UTC = "Etc/UTC"  # Strongly Recommended
    CanadaEastern = "America/Toronto"
    CanadaMountain = "America/Edmonton"
    CanadaAtlantic = "America/Halifax"
    CanadaCentral = "America/Winnipeg"
    CanadaNewfoundland = "America/St_Johns"
    CanadaPacific = "America/Vancouver"


class ERDDAPVariableRole(MultiValuedEnum):

    ProfileExtra = "profile_extra"
    TimeseriesExtra = "timeseries_extra"
    TrajectoryExtra = "trajectory_extra"


class CFVariableRole(MultiValuedEnum):

    ProfileID = "profile_id"
    TimeseriesID = "timeseries_id"
    TrajectoryID = "trajectory_id"


class CoverageContentType(MultiValuedEnum):

    Auxillary = "auxillaryInformation"
    Coordinate = "coordinate"
    Image = "image"
    ModelResult = "modelResult"
    PhysicalMeasurement = "physicalMeasurement"
    QualityInformation = "qualityInformation"
    ReferenceInformation = "referenceInformation"
    ThematicClassification = "thematicClassification"


class GCAudience(MultiValuedEnum):

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


class GCCollectionType(MultiValuedEnum):
    NonSpatial = "primary"
    Geospatial = "geogratis"
    OpenMaps = "fgp"
    Publications = "publication"


class GCSubject(MultiValuedEnum):
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

class GCPlace(MultiValuedEnum):

    Canada = "canada"  # General
    Burlington = "ontario_-_halton"  # CCIW
    Ottawa = "ontario_-_ottawa"  # NCR
    Dartmouth = "nova_scotia_-_halifax"  # BIO
    Moncton = "nova_scotia_-_westmorland"  # GFC
    Montjoli = "quebec_-_la_mitis"  # IML
    Nanaimo = "british_columbia_-_nanaimo"  # PBS
    Sidney = "british_columbia_-_capital"  # IOS
    StJohns = "newfoundland_and_labrador_-_division_no._1"  # NAFC

    @classmethod
    def _convert_value(cls, s: str):
        s = s.strip()
        if ',' in s:
            city, province = s.split(',', maxsplit=1)
            province = province.strip()
            if province.upper() in PROVINCES:
                province = PROVINCES[province.upper()]
            s = f"{province} - {city.strip()}"
        while '  ' in s:
            s = s.replace('  ', ' ')
        return s.lower().replace(' ', '_')


class ERDDAPDatasetType(MultiValuedEnum):

    DSGTable = "EDDTableFromNcCFFiles"  # Use this one for files following CF's DSG conventions
    MultiDimDSGMTable = "EDDTableFromMultidimNcFile"  # Multi-dimensional CF DSG files
    OtherNetCDFTable = "EDDTableFromNcFiles"  # All other netcdf formats
    ASCIITable = "EDDTableFromAsciiFiles"  # ASCII files
    NetCDFGrid = "EDDGridFromNcFiles"  # Gridded NetCDF files


class CommonDataModelType(MultiValuedEnum):
    Point = "Point", "point"  # (x, y, t[, d])
    Profile = "Profile", "profile"  # (x, y, t) and (d)
    TimeSeries = "TimeSeries", "timeseries"  # station:(x, y[, d]) and (t)
    TimeSeriesProfile = "TimeSeriesProfile", "timeseriesprofile"  # station:(x, y) and (t, d)
    Trajectory = "Trajectory", "trajectory"  # station: () and (x, y, t[, d])
    TrajectoryProfile = "TrajectoryProfile", "trajectoryprofile"  # station: () and (x, y, t) and (d)

    # These are non-standard but recognized by ERDDAP
    Grid = "Grid", "grid"  # fixed (x, y[, t][, d]) grid
    MovingGrid = "MovingGrid", "movinggrid"  # grid but (x,y[,d]) may vary over time
    RadialSweep = "RadialSweep", "radialsweep"  # e.g. radial / gate, azimuth/distance, etc
    Swath = "Swath", "swath"

    Other = "Other", "other"  # data that does not have geographical coordinates


class EssentialOceanVariable(MultiValuedEnum):

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


class MaintenanceFrequency(MultiValuedEnum):

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


class TopicCategory(MultiValuedEnum):

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


class SpatialRepresentation(MultiValuedEnum):

    Grid = "grid"
    Stereographic = "stereoModel"
    TextTable = "textTable"
    TIN = "tin"
    Vector = "vector"
    Video = "video"


class StatusCode(MultiValuedEnum):

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


class CoordinateReferenceSystem(MultiValuedEnum):

    NAD27 = FrozenDict(_guid='nad27'), '4267', 'NAD27'
    ''' EPSG 4267: the NAD27 datum '''

    WGS84 = FrozenDict(_guid='wgs84'), '4326', 'WGS84'
    ''' EPSG 4326: the WGS84 datum '''

    MSL_Depth = FrozenDict(_guid='msl_depth'), 'MSLD', '5715'
    ''' EPSG 5715: depth below a non-specific mean sea level (depth positive) '''

    MSL_Heights = FrozenDict(_guid='msl_height'), 'MSLH', '5714'
    ''' EPSG 5714: height above a non-specific mean sea level (depth negative) '''

    Instant_Depth = FrozenDict(_guid='instant_depth'), '5831'
    ''' EPSG 5831: depth below current instantaneous sea level (depth positive) '''

    Instant_Heights = FrozenDict(_guid='instant_heights'), '5829'
    ''' EPGS 5829: altitude above current instantaneous sea level (depth negative) '''

    Gregorian = FrozenDict(_guid='gregorian'), 'GREGORIAN', 'STANDARD'
    ''' Standard Gregorian calendar '''

    @classmethod
    def _convert_value(cls, value: str):
        value = str(value).upper()
        value = value.replace(' ', '')
        if value.startswith('EPSG:'):
            value = value[5:]
        return value


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

    NoLanguage = tuple()
    """ The resource has no language component. """

    English = tuple(["ENG"])
    """ The resource is provided in English. """

    French = tuple(["FRA"])
    """ The resource is provided in French. """

    Bilingual = ("ENG", "FRA")
    """ The resource is provided in both language. """


class ContactRole(MultiValuedEnum):

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

    Originator = "originator", "CONT0003"
    """ A person who originated data to support building the document. """

    Owner = "owner", "CONT0002"
    """ A person who owns the document. """

    ContactPoint = "pointOfContact"
    """ A person who is the point of contact for the document and can answer questions. """

    PrincipalInvestigator = "principalInvestigator", "CONT0004"
    """ A person who is the principal investigator of a research project. """

    Processor = "processor", "CONT0006"
    """ A person who processed the data. """

    Publisher = "publisher"
    """ A person who published the data. """

    ResourceProvider = "resourceProvider"
    """ A person who provides the resource to others. """

    RightsHolder = "rightsHolder"
    """ A person who owns the rights to the document. """

    Sponsor = "sponsor"
    """ A person who sponsored the creation of the document. """

    Stakeholder = "stakeholder", "CONT0001", "CONT0005", "CONT0007"
    """ A person who is a stakeholder in the creation of the document. """

    User = "user"
    """ A person who uses the data. """



class IDSystem(enum.Enum):

    DOI = FrozenDict(_guid="DOI")
    """ Digital Object Identifier system - codes should just be the DOI without any prefix. """

    ROR = FrozenDict(_guid="ROR")
    """ Research Organization Registry system - codes should just be the ROR without any prefix. """

    ORCID = FrozenDict(_guid="ORCID")
    """ Open Researcher and Contributor ID system - codes should just be the ORCID without any prefix. """

    VesselIMO = FrozenDict(_guid="IMONumber")
    """ IMO vessel numbers. """

    EPSG = FrozenDict(_guid="EPSG")
    """ EPSG coordinate reference systems """


class TelephoneType(enum.Enum):

    Fax = "fax"
    Cell = "cell"
    Voice = "voice"


class Country(enum.Enum):

    Canada = "CAN"
    UnitedStates = "USA"


class Locale(enum.Enum):

    CanadianEnglish = FrozenDict(_guid='canadian_english_utf8')
    """ Canadian English using UTF-8 text encoding. """

    CanadianFrench = FrozenDict(_guid='canadian_french_utf8')
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

    MEDS = FrozenDict(_guid="meds")


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


class EntityRef(dd.DataDictObject):

    guid: str = dd.p_str(managed_name='_guid')
    display_name: LanguageDict = dd.p_i18n_text(managed_name='_display_names')


class Variable(EntityRef):

    cnodc_name: str = dd.p_str()
    axis: Axis = dd.p_enum(Axis)
    units: str = dd.p_str()
    actual_min: float | int = dd.p_nonumpy()
    actual_max: float | int = dd.p_nonumpy()
    positive: Direction = dd.p_enum(Direction)
    encoding: Encoding = dd.p_enum(Encoding)
    source_name: str = dd.p_str()
    source_data_type: NetCDFDataType = dd.p_enum(NetCDFDataType)
    destination_name: str = dd.p_str()
    destination_data_type: NetCDFDataType = dd.p_enum(NetCDFDataType)
    dimensions: set[str] = dd.p_set(value_coerce=str)
    long_name: LanguageDict = dd.p_i18n_text()
    standard_name: str = dd.p_str()
    time_precision: TimePrecision = dd.p_enum(TimePrecision)
    calendar: Calendar = dd.p_enum(Calendar)
    time_zone: TimeZone = dd.p_enum(TimeZone)
    missing_value: float | int = dd.p_nonumpy()
    scale_factor: float | int = dd.p_nonumpy()
    add_offset: float | int = dd.p_nonumpy()
    ioos_category: IOOSCategory = dd.p_enum(IOOSCategory)
    valid_min: float | int = dd.p_nonumpy()
    valid_max: float | int = dd.p_nonumpy()
    allow_subsets: bool = dd.p_bool()
    cf_role: CFVariableRole = dd.p_enum(CFVariableRole)
    erddap_role: ERDDAPVariableRole = dd.p_enum(ERDDAPVariableRole)
    comment: str = dd.p_str()
    references: str = dd.p_str()
    source: str = dd.p_str()
    coverage_content_type: CoverageContentType = dd.p_enum(CoverageContentType)
    variable_order: int = dd.p_int()
    is_axis: bool = dd.p_bool()
    is_altitude_proxy: bool = dd.p_bool(managed_name='altitude_proxy')
    additional_properties: dict[str, SupportsExtendedJson] = dd.p_dict(managed_name='custom_metadata')

    def set_time_units(self, base_units: NumericTimeUnits, epoch: datetime.datetime):
        """
        :param base_units: The basic units of duration (e.g. "seconds since")
        :param epoch: The reference time
        """
        self.units = f"{base_units.value} since {epoch.isoformat()}"

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
            positive=var_attributes.pop('positive', None),
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
                var.actual_min = t.cast(float, np.min(values))
                var.actual_max = t.cast(float, np.max(values))
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
        var.additional_properties = {
            x: unnumpy(var_attributes[x])
            for x in var_attributes
        }
        return var


class MaintenanceRecord(EntityRef):

    date: datetime.date = dd.p_date()
    notes: LanguageDict = dd.p_i18n_text()
    scope: MaintenanceScope = dd.p_enum(MaintenanceScope)


class QuickWebPage(EntityRef):

    name: LanguageDict | None = dd.p_i18n_text()
    description: LanguageDict | None = dd.p_i18n_text()
    purpose: ResourcePurpose | None = dd.p_enum(ResourcePurpose, managed_name='function')
    url: LanguageDict | None = dd.p_i18n_text()
    resource_type: ResourceType | None = dd.p_enum(ResourceType, managed_name='protocol')

    def after_set(self, managed_name: str, value: t.Any, original: t.Any = None):
        super().after_set(managed_name, value, original)
        if managed_name == 'url' and value and self.resource_type is None:
            self.resource_type = self.autodetect_resource_type(value)
        elif managed_name == 'purpose' and value is ResourceType.Auto:
            self.resource_type = self.autodetect_resource_type(self.url)

    @staticmethod
    def autodetect_resource_type(full_url: t.Optional[dict]) -> ResourceType | None:
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
            return ResourceType.SecureWebPage
        elif url.startswith("http://"):
            return ResourceType.WebPage
        elif url.startswith(("ftp://", "ftps://", "ftpse://")):
            return ResourceType.FTP
        elif url.startswith("git://"):
            return ResourceType.Git
        elif url.startswith("file://"):
            return ResourceType.File
        return None


class Resource(QuickWebPage):

    additional_request_info: LanguageDict = dd.p_i18n_text(managed_name='protocol_request')
    additional_app_info: LanguageDict = dd.p_i18n_text(managed_name='app_profile')
    goc_content_type: GCContentType = dd.p_enum(GCContentType)
    goc_languages: GCLanguage = dd.p_enum(GCLanguage)
    goc_format: GCContentFormat = dd.p_enum(GCContentFormat)

    def after_set(self, managed_name: str, value: t.Any, original: t.Any = None):
        super().after_set(managed_name, value)
        if managed_name == 'url' and value and self.goc_format is None:
            self._data['goc_format'] = self.autodetect_gc_content_format(value)
        if managed_name == 'goc_formats' and value and value is GCContentFormat.Auto:
            self._data['goc_format'] = self.autodetect_gc_content_format(self.url)

    @staticmethod
    def autodetect_gc_content_format(full_url: t.Optional[dict]) -> GCContentFormat | None:
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
            return GCContentFormat.ArchiveTARGZIP
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
                    return GCContentFormat(extension)
            if url.startswith("https://") or url.startswith("http://"):
                return GCContentFormat.Hypertext
        return None


class TelephoneNumber(EntityRef):

    phone_number: str = dd.p_str()
    phone_number_type: TelephoneType = dd.p_enum(TelephoneType)

class _IdentifierMixin:

    id_description: LanguageDict = dd.p_i18n_text()
    id_system: IDSystem = dd.p_enum(IDSystem)
    id_code: str = dd.p_str()

    def set_id_and_system(self, id_code: str, id_system: IDSystem, remove_prefixes: list[str] = None):
        if remove_prefixes:
            for prefix in remove_prefixes:
                if id_code.startswith(prefix):
                    id_code = id_code[len(prefix):]
        self.id_code = id_code
        self.id_system = id_system

    def _get_id(self) -> t.Optional[str]:
        return self.id_code

    @staticmethod
    def id_property(system: IDSystem, remove_prefixes: t.Iterable[str]):
        return property(
            functools.partial(_IdentifierMixin._get_id),
            functools.partial(_IdentifierMixin.set_id_and_system, id_system=system, remove_prefixes=remove_prefixes),
        )


class _Contact(EntityRef, _IdentifierMixin):

    email: LanguageDict = dd.p_i18n_text()
    service_hours: LanguageDict = dd.p_i18n_text()
    instructions: LanguageDict = dd.p_i18n_text()
    resources: list[Resource] = dd.p_object_list(Resource, managed_name='web_resources')
    phone_numbers: list[TelephoneNumber] = dd.p_object_list(TelephoneNumber, managed_name='phone')
    address: LanguageDict = dd.p_i18n_text(managed_name='delivery_point')
    city: str = dd.p_str()
    province: LanguageDict = dd.p_i18n_text(managed_name='admin_area')
    country: Country = dd.p_enum(Country)
    postal_code: str = dd.p_str()
    web_page: t.Optional[QuickWebPage] = dd.p_ddo(QuickWebPage)


class Individual(_Contact):

    name: str = dd.p_str(managed_name='individual_name')
    orcid: str = _IdentifierMixin.id_property(IDSystem.ORCID, ('https://orcid.org/', 'http://orcid.org/'))


class Organization(_Contact):

    name: LanguageDict = dd.p_i18n_text(managed_name='organization_name')
    individuals: list[_Contact] = dd.p_object_list(_Contact)
    ror: str = _IdentifierMixin.id_property(IDSystem.ROR, ('https://ror.org/', 'http://ror.org/'))


class Position(_Contact):

    name: LanguageDict = dd.p_i18n_text(managed_name='position_name')


class _ResponsibleParty(EntityRef):

    role: ContactRole = dd.p_enum(ContactRole)
    contact: _Contact = dd.p_ddo(_Contact)


class _ResponsiblesMixin:

    responsibles: list[_ResponsibleParty] = dd.p_object_list(_ResponsibleParty)

    def add_contact(self, role: ContactRole, contact: _Contact):
        """
        :param role: The role the person plays for this citation
        :param contact: Their contact information
        """
        self.responsibles.append(_ResponsibleParty(role=role, contact=contact))


class Citation(EntityRef, _ResponsiblesMixin, _IdentifierMixin):

    title: LanguageDict = dd.p_i18n_text()
    alt_title: LanguageDict = dd.p_i18n_text()
    details: LanguageDict = dd.p_i18n_text()
    edition: LanguageDict = dd.p_i18n_text()
    publication_date: datetime.date = dd.p_date()
    revision_date: datetime.date = dd.p_date()
    creation_date: datetime.date = dd.p_date()
    edition_date: datetime.date = dd.p_date()
    isbn: str = dd.p_str()
    issn: str = dd.p_str()
    resource: t.Optional[Resource] = dd.p_ddo(Resource)


class GeneralUseConstraint(EntityRef, _ResponsiblesMixin):

    description: LanguageDict = dd.p_i18n_text()
    plain_text: LanguageDict = dd.p_i18n_text()
    citations: list[Citation] = dd.p_object_list(Citation, managed_name='reference')


class LegalConstraint(GeneralUseConstraint):

    access_constraints: set[RestrictionCode] = dd.p_enum_set(RestrictionCode)
    use_constraints: set[RestrictionCode] = dd.p_enum_set(RestrictionCode)
    other_constraints: LanguageDict = dd.p_i18n_text()


class SecurityConstraint(GeneralUseConstraint):

    classification: ClassificationCode = dd.p_enum(ClassificationCode)
    user_notes: LanguageDict = dd.p_i18n_text()
    classification_system: LanguageDict = dd.p_i18n_text()


class ERDDAPServer(EntityRef, _ResponsiblesMixin):

    base_url: LanguageDict = dd.p_str()


class Thesaurus(EntityRef):

    keyword_type: KeywordType = dd.p_enum(KeywordType)
    prefix: str = dd.p_str()
    citation: t.Optional[Citation] = dd.p_ddo(Citation)


class Keyword(EntityRef):

    text: LanguageDict = dd.p_i18n_text(managed_name='keyword')
    description: LanguageDict = dd.p_i18n_text()
    thesaurus: t.Optional[Thesaurus] = dd.p_ddo(Thesaurus)


class DistributionChannel(EntityRef, _ResponsiblesMixin):

    description: LanguageDict = dd.p_i18n_text()
    primary_link: t.Optional[QuickWebPage] = dd.p_ddo(QuickWebPage, managed_name='primary_web_link')
    links: list[Resource] = dd.p_object_list(Resource)


class SpatialResolution(EntityRef):

    scale: int = dd.p_int()
    level_of_detail: LanguageDict = dd.p_i18n_text()
    horizontal_resolution: int | float = dd.p_nonumpy(managed_name='distance')
    vertical_resolution: int | float = dd.p_nonumpy(managed_name='vertical')
    angular_resolution: int | float = dd.p_nonumpy(managed_name='angular')
    horizontal_units: DistanceUnit = dd.p_enum(DistanceUnit, managed_name='distance_units')
    vertical_units: DistanceUnit = dd.p_enum(DistanceUnit)
    angular_units: AngularUnit = dd.p_enum(AngularUnit)


class TemporalResolution(EntityRef):

    years: int = dd.p_int()
    months: int = dd.p_int()
    days: int = dd.p_int()
    hours: int = dd.p_int()
    minutes: int = dd.p_int()
    seconds: int = dd.p_int()

    @classmethod
    def from_iso_format(cls, iso_duration: str) -> t.Self:
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
                return cls(days=weeks*7)
            else:
                return cls(years=parts[0] or None, months=parts[1] or None, days=parts[2] or None, hours=parts[3] or None, minutes=parts[4] or None, seconds=int(parts[5]) or None)



class DatasetMetadata(EntityRef, _ResponsiblesMixin):

    ontology: OCProc2Ontology = None

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

    distributors: list[DistributionChannel] = dd.p_object_list(DistributionChannel)
    variables: list[Variable] = dd.p_object_list(Variable)
    custom_keywords: list[Keyword] = dd.p_object_list(Keyword)
    alt_metadata: list[Citation] = dd.p_object_list(Citation)
    maintenance_records: list[MaintenanceRecord] = dd.p_object_list(MaintenanceRecord, managed_name='iso_maintenance')
    metadata_constraints: list[GeneralUseConstraint] = dd.p_object_list(GeneralUseConstraint, managed_name='metadata_licenses')
    data_constraints: list[GeneralUseConstraint] = dd.p_object_list(GeneralUseConstraint, managed_name='licenses')
    metadata_standards: list[Citation] = dd.p_object_list(Citation)
    metadata_profiles: list[Citation] = dd.p_object_list(Citation)
    additional_docs: list[Citation] = dd.p_object_list(Citation)
    canon_urls: list[Citation] = dd.p_object_list(Citation)

    spatial_resolution: t.Optional[SpatialResolution] = dd.p_ddo(SpatialResolution)
    temporal_resolution: t.Optional[TemporalResolution] = dd.p_ddo(TemporalResolution)
    metadata_owner: t.Optional[_Contact] = dd.p_ddo(_Contact)
    publisher: t.Optional[_Contact] = dd.p_ddo(_Contact)
    parent_metadata: t.Optional[Citation] = dd.p_ddo(Citation)
    info_link: t.Optional[QuickWebPage] = dd.p_ddo(QuickWebPage)

    institution: str = dd.p_str()
    program: str = dd.p_str()
    project: str = dd.p_str()
    cf_standard_name_vocab: str = dd.p_str(managed_name='standard_name_vocab')
    processing_level: str = dd.p_str()
    geospatial_bounds: str = dd.p_str()

    id_code: str = dd.p_str(managed_name='dataset_id_code')
    id_system: IDSystem = dd.p_enum(IDSystem, managed_name='dataset_id_system')
    id_description: LanguageDict = dd.p_i18n_text(managed_name='dataset_id_description')
    doi = _IdentifierMixin.id_property(IDSystem.DOI, ('https://doi.org/', 'http://doi.org/', 'doi:'))

    geospatial_lat_min: int | float = dd.p_nonumpy()
    geospatial_lon_min: int | float = dd.p_nonumpy()
    geospatial_lat_max: int | float = dd.p_nonumpy()
    geospatial_lon_max: int | float = dd.p_nonumpy()
    geospatial_vertical_min: int | float = dd.p_nonumpy()
    geospatial_vertical_max: int | float = dd.p_nonumpy()

    date_issued: datetime.date = dd.p_date()
    date_created: datetime.date = dd.p_date()
    date_modified: datetime.date = dd.p_date()

    time_coverage_start: awaretime.AwareDateTime = dd.p_awaretime()
    time_coverage_end: awaretime.AwareDateTime = dd.p_awaretime()

    title: LanguageDict = dd.p_i18n_text()
    comment: LanguageDict = dd.p_i18n_text()
    processing_description: LanguageDict = dd.p_i18n_text()
    processing_environment: LanguageDict = dd.p_i18n_text()
    purpose: LanguageDict = dd.p_i18n_text()
    references: LanguageDict = dd.p_i18n_text()
    file_storage_location: LanguageDict = dd.p_i18n_text()
    internal_notes: LanguageDict = dd.p_i18n_text()
    source: LanguageDict = dd.p_i18n_text()
    abstract: LanguageDict = dd.p_i18n_text(managed_name='summary')
    credit: LanguageDict = dd.p_i18n_text(managed_name='acknowledgement')

    goc_publisher: GCPublisher = dd.p_enum(GCPublisher)
    processing_system: IDSystem = dd.p_enum(IDSystem)
    geospatial_crs: CoordinateReferenceSystem = dd.p_enum(CoordinateReferenceSystem, managed_name='geospatial_bounds_crs')
    geospatial_vertical_crs: CoordinateReferenceSystem = dd.p_enum(CoordinateReferenceSystem, managed_name='geospatial_bounds_vertical_crs')
    temporal_crs: CoordinateReferenceSystem = dd.p_enum(CoordinateReferenceSystem)
    primary_data_locale: Locale = dd.p_enum(Locale, managed_name='data_locale')
    primary_metadata_locale: Locale = dd.p_enum(Locale, managed_name='metadata_locale')
    metadata_maintenance_frequency: MaintenanceFrequency = dd.p_enum(MaintenanceFrequency)
    data_maintenance_frequency: MaintenanceFrequency = dd.p_enum(MaintenanceFrequency, managed_name='resource_maintenance_frequency')
    topic_category: TopicCategory = dd.p_enum(TopicCategory)
    status: StatusCode = dd.p_enum(StatusCode)
    spatial_representation: SpatialRepresentation = dd.p_enum(SpatialRepresentation, managed_name='spatial_representation_type')
    goc_collection: GCCollectionType = dd.p_enum(GCCollectionType, managed_name='goc_collection_type')
    goc_subject: GCSubject = dd.p_enum(GCSubject)

    is_ongoing: bool = dd.p_bool(default=False)
    is_available_via_meds_request_form: bool = dd.p_bool(managed_name='via_meds_request_form', default=False)

    secondary_data_locales: set[Locale] = dd.p_enum_set(Locale, managed_name='data_extra_locales')
    secondary_metadata_locales: set[Locale] = dd.p_enum_set(Locale, managed_name='metadata_extra_locales')
    goc_publication_places: set[GCPlace] = dd.p_enum_set(GCPlace, managed_name='goc_publication_place')
    goc_audiences: set[GCAudience] = dd.p_enum_set(GCAudience, managed_name='goc_audience')
    essential_ocean_variables: set[EssentialOceanVariable] = dd.p_enum_set(EssentialOceanVariable, managed_name='cioos_eovs')

    conventions: set[str] = dd.p_set(value_coerce=str)
    cf_standard_names: set[str] = dd.p_set(value_coerce=str)

    feature_type: CommonDataModelType = dd.p_enum(CommonDataModelType)

    authority: str | None = dd.p_str(managed_name='_authority')
    activation_workflow: str = dd.p_str(managed_name='_activation_workflow')
    publication_workflow: str = dd.p_str(managed_name='_publication_workflow')
    organization_name: str = dd.p_str(managed_name='_org_name')
    security_level: str = dd.p_str(managed_name='_security_level')
    profiles: set[str] = dd.p_set(managed_name='_profiles', value_coerce=str)
    users: set[str] = dd.p_set(managed_name='_users', value_coerce=str)

    erddap_servers: list[ERDDAPServer] = dd.p_object_list(ERDDAPServer)
    erddap_data_file_path: str = dd.p_str()
    erddap_data_file_pattern: str = dd.p_str()
    erddap_dataset_id: str = dd.p_str()
    erddap_dataset_type: ERDDAPDatasetType = dd.p_enum(ERDDAPDatasetType)

    custom_metadata: dict[str, SupportsExtendedJson] = dd.p_dict(value_coerce=unnumpy)

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._log = logging.getLogger("cnodc.dmd.metadata")
        self.profiles.add('cnodc')

    def after_set(self, managed_name: str, value: t.Any, original: t.Any = None):
        super().after_set(managed_name, value, original)
        if managed_name == 'feature_type' and value and self.spatial_representation is None and value in DatasetMetadata.REPRESENTATION_MAP:
            self.spatial_representation = DatasetMetadata.REPRESENTATION_MAP[value]
        if managed_name.startswith('erddap') and value:
            self.profiles.add('erddap')

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
        self.activation_workflow = "cnodc_activation"
        self.publication_workflow = "cnodc_publish"
        self.cf_standard_name_vocab = "CF 1.13"
        self.security_level = 'unclassified'
        self.goc_publisher = GCPublisher.MEDS
        self.goc_subject = GCSubject.Oceanography
        self.goc_publication_places.add(GCPlace.Ottawa)
        self.goc_audiences.add(GCAudience.Scientists)
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
        self.program = attrs.pop('program', '')
        self.project = attrs.pop('project', '')
        self.institution = attrs.pop('institution', '')
        self.guid = attrs.pop('id', '')
        self.feature_type = attrs.pop('featureType', '')
        self.processing_level = attrs.pop('processing_level', '')
        self.geospatial_bounds = attrs.pop('geospatial_bounds', '')
        self.conventions = attrs.pop('Conventions', "").split(",")
        self.processing_description = get_bilingual_attribute(attrs, 'processing_description', locale_map)
        self.processing_environment = get_bilingual_attribute(attrs, 'processing_environment', locale_map)
        self.credit = get_bilingual_attribute(attrs, 'acknowledgement', locale_map)
        self.comment = get_bilingual_attribute(attrs, 'comment', locale_map)
        self.references = get_bilingual_attribute(attrs, 'references', locale_map)
        self.source = get_bilingual_attribute(attrs, 'source', locale_map)
        self.abstract = get_bilingual_attribute(attrs, 'summary', locale_map)
        self.purpose = get_bilingual_attribute(attrs, 'purpose', locale_map)
        self.is_ongoing = attrs.pop('is_ongoing', '') == 'Y'
        if 'standard_name_vocabulary' in attrs and attrs['standard_name_vocabulary']:
            self.cf_standard_name_vocab = attrs.pop('standard_name_vocabulary')
        if 'date_issued' in attrs and attrs['date_issued']:
            self.date_issued = awaretime.utc_from_isoformat(attrs.pop('date_issued'))
        if 'date_created' in attrs and attrs['date_created']:
            self.date_created = awaretime.utc_from_isoformat(attrs.pop('date_created'))
        if 'date_modified' in attrs and attrs['date_modified']:
            self.date_modified = awaretime.utc_from_isoformat(attrs.pop('date_modified'))
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
            self.info_link = QuickWebPage(url=info_url, purpose=ResourcePurpose.Information)
        if 'doi' in attrs and attrs['doi']:
            self.doi = attrs.pop('doi', '')
        md_link = get_bilingual_attribute(attrs, 'metadata_link', locale_map)
        if md_link:
            cit = Citation()
            res = Resource(
                url=md_link,
                purpose=ResourcePurpose.CompleteMetadata,
                goc_content_type = GCContentType.SupportingDocumentation
            )
            cit.resource = res
            self.alt_metadata.append(cit)
        if 'time_coverage_resolution' in attrs and attrs['time_coverage_resolution']:
            try:
                self.temporal_resolution = TemporalResolution.from_iso_format(attrs.pop('time_coverage_resolution', ''))
            except ValueError:
                self._log.exception("Invalid value for time_coverage_resolution")
        if 'geospatial_bounds_crs' in attrs and attrs['geospatial_bounds_crs']:
            self.geospatial_crs = attrs.pop('geospatial_bounds_crs')
        if 'geospatial_bounds_vertical_crs' in attrs and attrs['geospatial_bounds_vertical_crs']:
            self.geospatial_vertical_crs = attrs.pop('geospatial_bounds_vertical_crs')
        self._build_from_netcdf_contacts(locale_map, attrs, 'creator', contact_default_role=ContactRole.Originator, contact_default_type='individual')
        self._build_from_netcdf_contacts(locale_map, attrs, 'publisher', contact_default_role=ContactRole.Publisher, contact_default_type='individual')
        self._build_from_netcdf_contacts(locale_map, attrs, 'contributor', contact_default_role=ContactRole.Contributor, contact_default_type='individual')

        self._add_netcdf_contacts(
            names=get_bilingual_attribute(attrs, f"contributing_institutions", locale_map),
            guids=attrs.pop('contributing_institutions_cnodc_guid', ''),
            contact_default_role=ContactRole.Contributor,
            contact_default_type='institution',
            emails={},
            ids='',
            urls={},
            institutions={},
            specific_roles='',
            specific_types='',
            id_vocabulary=''
        )
        for extent_attr in ('geospatial_lat_min', 'geospatial_lat_max', 'geospatial_lon_min', 'geospatial_lon_max',
                           'geospatial_vertical_min', 'geospatial_vertical_max', 'time_coverage_start',
                           'time_coverage_end'):
            value = attrs.pop(extent_attr, None)
            if value is not None and getattr(self, extent_attr) is None:
                setattr(self, extent_attr, value)
        if 'naming_authority' in attrs:
            self.authority = attrs.pop('naming_authority', None) or None
        self.custom_metadata = {
            x: unnumpy(attrs[x])
            for x in attrs
        }

    def _set_locales_from_netcdf(self, attrs: dict[str, str], default_lang: str) -> dict[str, str]:
        locale_map = {}
        primary_locale = Locale.CanadianEnglish
        secondary_locales = []
        if 'locale_default' in attrs:
            default_locale = attrs.pop('locale_default')
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
        if 'locale_others' in attrs:
            for locale in attrs.pop('locale_others').split(','):
                suffix, bcptag = locale.split(':', maxsplit=1)
                if '-' in bcptag:
                    bcptag, _ = bcptag.split('-', maxsplit=1)
                locale_map[suffix.strip()] = bcptag.strip()
        else:
            locale_map['_en'] = 'en'
            locale_map['_fr'] = 'fr'
        return locale_map

    def _identify_levels(self, dataset: nc.Dataset) -> list[str]:
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
            guids=attrs.pop(f"{prefix}_cnodc_guid", "")
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
                             id_vocabulary: str,
                             guids: str):
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

        names: list[dict[str, str]] = split_multilingual_attribute(names)
        emails: list[dict[str, str]] = split_multilingual_attribute(emails)
        ids: list[str] = ids.split(",")
        urls: list[dict[str, str]] = split_multilingual_attribute(urls)
        institutions: list[dict[str, str]] = split_multilingual_attribute(institutions)
        specific_roles: list[str] = specific_roles.split(",")
        specific_types: list[str] = specific_types.split(",")
        id_vocabulary: list[str] = id_vocabulary.split(',') if ',' in id_vocabulary else [id_vocabulary for _ in names]
        guids: list[str] = guids.split(',')
        for idx in range(0, len(names)):
            self._add_netcdf_contact(
                name=names[idx],
                email=emails[idx] if idx < len(emails) else None,
                contact_id=ids[idx] if idx < len(ids) else None,
                url=urls[idx] if idx < len(urls) else None,
                institution=institutions[idx] if idx < len(institutions) else None,
                role=specific_roles[idx] if idx < len(specific_roles) and specific_roles[idx] else contact_default_role,
                contact_type=specific_types[idx] if idx < len(specific_types) and specific_types[idx] else contact_default_type,
                id_vocabulary=id_vocabulary[idx] if idx < len(id_vocabulary) else None,
                guid=guids[idx] if idx < len(guids) else None
            )

    def _add_netcdf_contact(self,
                            name: t.Optional[ dict[str, str] | str],
                            email: t.Optional[dict[str, str] | str],
                            contact_id: t.Optional[str],
                            url: t.Optional[dict[str, str] | str],
                            institution: t.Optional[dict[str, str] | str],
                            role: t.Union[str, ContactRole],
                            contact_type: str,
                            id_vocabulary: str | None,
                            guid: str | None):
        if contact_type == 'institution' or contact_type == 'group':
            contact = Organization(guid=guid or None, name=name)
            if contact_id is not None and contact_id != "":
                if id_vocabulary is None or id_vocabulary == "" or id_vocabulary.lower().startswith("https://ror.org"):
                    contact.ror = contact_id
                else:
                    self._log.warning(f"Unknown ID vocabulary for organization: {id_vocabulary}")
        elif contact_type == 'position':
            contact = Position(guid=guid or None, name=name)
            if contact_id:
                self._log.warning(f"ID provided for position: {contact_id} [{id_vocabulary}]")
        else:
            contact = Individual(guid=guid or None, name=first_i18n(name))
            if contact_id is not None and contact_id != "":
                if id_vocabulary is None or id_vocabulary == "" or id_vocabulary.lower().startswith("https://orcid.org"):
                    contact.orcid = contact_id
                else:
                    self._log.warning(f"Unknown ID vocabulary for individual: {id_vocabulary}")
        if url is not None:
            contact.web_page = QuickWebPage(url=url, purpose=ResourcePurpose.Information)
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
            role = ContactRole(role)
        if role is None:
            self._log.warning(f"Missing contact role for [{name}]")
        else:
            self.add_contact(ContactRole(role) if isinstance(role, str) else role, contact)

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
                    ioos_cat = IOOSCategory(element.ioos_category)
                var.ioos_category = ioos_cat
                if element.essential_ocean_vars:
                    if len(element.essential_ocean_vars) == 1:
                        self.essential_ocean_variables.add(EssentialOceanVariable(list(element.essential_ocean_vars)[0]))
                    elif eov_prefixes:
                        for eov in element.essential_ocean_vars:
                            if any(eov.startswith(x) for x in eov_prefixes):
                                self.essential_ocean_variables.add(EssentialOceanVariable(eov))
            else:
                var.ioos_category = IOOSCategory.Other
        if var.standard_name is not None:
            self.cf_standard_names.add(var.standard_name)
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
                    if var.actual_min is not None:
                        self.time_coverage_start = epoch + datetime.timedelta(**{pieces[0]: var.actual_min})
                    if var.actual_max is not None:
                        self.time_coverage_end = epoch + datetime.timedelta(**{pieces[0]: var.actual_max})
                except Exception as ex:
                    self._log.exception(f"Exception handling time units: [{units}]: {type(ex)}: {str(ex)}")
            else:
                self._log.warning(f"Unrecognized time units {units}")

    def build_request_body(self) -> dict:
        body = {
            'metadata': super().export()
        }
        DatasetMetadata.clean_for_request_body(body['metadata'])
        for key in list(body['metadata'].keys()):
            if key[0] == '_':
                body[key[1:]] = body['metadata'][key]
                del body['metadata'][key]

        return body

    def add_file_direct_link(self,
                             file_url: AcceptAsLanguageDict,
                             file_name: dict[str, str]):
        dist = DistributionChannel(
            guid='direct_link_channel',
            display_name={"en": "Direct Link", "fr": "Lien direct"}
        )
        dist.primary_link = Resource(
            guid='direct_link_resource',
            url=file_url,
            display_name=file_name,
            name=file_name,
            goc_languages=GCLanguage.Bilingual,
            goc_content_type=GCContentType.Dataset,
            goc_format=GCContentFormat.DataNetCDF,
            purpose=ResourcePurpose.FileAccess,
            resource_type=ResourceType.File
        )
        self.distributors.append(dist)

    @staticmethod
    def clean_for_request_body(d):
        if isinstance(d, dict):
            for key in list(d.keys()):
                if d[key] is None or (isinstance(d[key], str) and (d[key] == '' or key == '_cls_')):
                    del d[key]
                elif isinstance(d[key], (list, tuple, set, dict)):
                    if len(d[key]) == 0:
                        del d[key]
                    else:
                        DatasetMetadata.clean_for_request_body(d[key])
            return d
        elif isinstance(d, (list, tuple, set)):
            return [
                DatasetMetadata.clean_for_request_body(x) if isinstance(x, (dict, list, tuple, set)) else x
                for x in d
            ]
        else:
            return d


class Common:

    Constraint_OpenGovernmentLicense = LegalConstraint(guid="open_government_license")
    Constraint_Unclassified = SecurityConstraint(guid="unclassified_data")

    Contact_CNODC = Organization(guid="cnodc")
    Contact_DFO = Organization(guid="dfo")

    ERDDAP_Primary = ERDDAPServer(guid="cnodc_primary")

    MetadataStandard_ISO19115 = Citation(guid="metadata_standard_iso19115")
    MetadataStandard_ISO191151 = Citation(guid="metadata_standard_iso19115-1")

    MetadataProfile_CIOOS = Citation(guid="metadata_profile_cioos")




