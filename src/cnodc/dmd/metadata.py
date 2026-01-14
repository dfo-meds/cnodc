import decimal
import enum
import typing as t
import datetime
from xml.dom.minidom import Entity

MultiLanguageString = t.Union[str, dict[str, str]]
NumberLike = t.Union[int, str, float, decimal.Decimal]


class Encoding(enum.Enum):

    UTF8 = "utf8"  # strongly recommended
    ISO_8859_1 = "iso-8859-1"
    UTF16 = "utf16"


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
    Grid = "Grid"  # fixed (x, y[, t][, d]) grid
    MovingGrid = "MovingGrid"  # grid but (x,y[,d]) may vary over time
    RadialSweep = "RadialSweep"  # e.g. radial / gate, azimuth/distance, etc
    Swath = "Swath"
    Other = "Other"  # data that does not have geographical coordinates


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
    Completed = "completed"
    Deprecated = "deprecated"
    Final = "final"
    Historical = "historicalArchive"
    NotAccepted = "notAccepted"
    Obsolete = "obsolete"
    OnGoing = "onGoing"
    Pending = "pending"
    Planned = "planned"
    Proposed = "proposed"
    Required = "required"
    Retired = "retired"
    Superseded = "superseded"
    Tentative = "tentative"
    UnderDevelopment = "underDevelopment"
    Valid = "valid"
    Withdrawn = "withdrawn"


class CoordinateReferenceSystem(enum.Enum):

    WGS84 = {
        "_guid": "wgs84",
        "code": "4326",
        "description": "WGS84 - World Geodetic System 1984",
        "system_type": "geodeticGeographic2D",
        "id_system": {
            "_guid": "epsg",
            "code_space": "EPSG:",
            "version": "",
        }
    }

    NAD27 = {
        "_guid": "nad27",
        "code": "4267",
        "description": "NAD27 - North American Datum 1927",
        "system_type": "geodeticGeographic2D",
        "id_system": {
            "_guid": "epsg",
            "code_space": "EPSG:",
            "version": "",
        }
    }

    MSL_Depth = {
        "_guid": "msl_depth",
        "code": "5715",
        "description": "Depth (positive down) below mean sea level without specified datum",
        "system_type": "vertical",
        "id_system": {
            "_guid": "epsg",
            "code_space": "EPSG:",
            "version": "",
        }
    }

    Gregorian = {
        "_guid": "gregorian",
        "system_type": "temporal",
        "code": "gregorian",
        "description": "ISO timestamp in a given timezone",
        "id_system": {
            "_guid": "standard_calendars",
            "description": "Standard calendars"
        },
    }


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

    BrowseText = "browsing"
    BrowseGraphic = "browseGraphic"
    Search = "search"

    Upload = "upload"

    CompleteMetadata = "completeMetadata"

    EmailRequest = "emailService"
    Download = "download"
    FileAccess = "fileAccess"
    OfflineAccess = "offlineAccess"
    OnlineOrder = "order"


class GCContentType(enum.Enum):

    Dataset = "dataset"
    WebService = "web_service"
    API = "api"
    SupportingDocumentation = "support_doc"
    Application = "application"


class GCLanguage(enum.Enum):

    NoLanguage = []
    English = ["ENG"]
    French = ["FRA"]
    Bilingual = ["ENG", "FRA"]

class ContactRole(enum.Enum):

    Author = "author"
    CoAuthor = "coAuthor"
    Collaborator = "collaborator"
    Contributor = "contributor"
    Custodian = "custodian"
    Distributor = "distributor"
    Editor = "editor"
    Funder = "funder"
    Mediator = "mediator"
    Originator = "originator"
    Owner = "owner"
    ContactPoint = "pointOfContact"
    PrincipalInvestigator = "principalInvestigator"
    Processor = "processor"
    Publisher = "publisher"
    ResourceProvider = "resourceProvider"
    RightsHolder = "rightsHolder"
    Sponsor = "sponsor"
    Stakeholder = "stakeholder"
    User = "user"


class IDSystem:

    DOI = {
        "_guid": "DOI",
        "_display_name": {"und": "DOI"},
        "code_space": 'https://doi.org/',
        "version": '',
    }

    ROR = {
        "_guid": "ROR",
        "_display_name": {"und": "ROR"},
        "code_space": "https://ror.org/",
        "version": "",
    }

    ORCID = {
        "_guid": "ORCID",
        "_display_name": {"und": "ORCID"},
        "code_space": "https://orcid.org/",
        "version": "",
    }

    VesselIMO = {
        "_guid": "IMONumber",
        "_display_name": {"und": "IMO"},
        "code_space": "",
        "version": "",
    }



class EntityRef:

    def __init__(self):
        self._guid = None
        self._display_name = {}
        self._metadata = {}
        self._children: dict[str, t.Union[t.Optional[EntityRef], list[EntityRef]]] = {}

    def set_guid(self, guid):
        """
        :param guid: A unique identifier for this entity
        """
        self._guid = guid

    def set_display_name(self, display_name: MultiLanguageString):
        """
        :param display_name: A string or multi-language dict containing the internal display name of the entity.
        """
        if isinstance(display_name, str):
            self._display_name = {
                'und': display_name,
            }
        else:
            self._display_name = display_name

    def set_english_display_name(self, display_name: str):
        """
        :param display_name: The English display name
        """
        self._display_name['en'] = display_name

    def set_french_display_name(self, display_name: str):
        """
        :param display_name: The French display name
        """
        self._display_name['fr'] = display_name

    def build_request_body(self):
        d = {
            '_guid': self._guid,
            '_display_name': self._display_name
        }
        d.update(self._metadata)
        for key in self._children.keys():
            if self._children[key] is None:
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
    def format_multilingual_text(text: t.Optional[MultiLanguageString]):
        if text is None:
            return None
        if isinstance(text, dict):
            return text
        return {'und': text}

    @staticmethod
    def format_date(d: t.Optional[t.Union[datetime.date, datetime.datetime]]):
        if d is None:
            return None
        return d.isoformat('T')


class Variable(EntityRef):

    def __init__(self, var_name: str, var_data_type: NetCDFDataType):
        super().__init__()
        self.set_source_name(var_name)
        self.set_source_data_type(var_data_type)

    def set_encoding(self, enc: Encoding):
        """
        :param enc: The text encoding for this text field
        """
        self._metadata['encoding'] = enc.value

    def set_source_name(self, name: str):
        """
        :param name: The name of this variable as it appears in the NetCDF file
        """
        self._metadata['source_name'] = name

    def set_source_data_type(self, data_type: NetCDFDataType):
        """
        :param data_type: The type of the data as it is stored in a NetCDF file
        """
        self._metadata['source_data_type'] = data_type.value

    def set_destination_data_type(self, data_type: NetCDFDataType):
        """
        :param data_type: The type of data once unpacked.
        """
        self._metadata['destination_data_type'] = data_type.value

    def set_destination_name(self, name: str):
        """
        :param name: The name of the variable as it should appear in ERDDAP
        """
        self._metadata['destination_name'] = name

    def set_dimensions(self, dims: list[str]):
        """
        :param dims: A list of dimensions that apply to this variable
        """
        self._metadata['dimensions'] = ",".join(dims)

    def set_long_name(self, long_name: MultiLanguageString):
        """
        :param long_name: A string or multilanguage dict with the long name of the variable
        """
        self._metadata['long_name'] = EntityRef.format_multilingual_text(long_name)

    def set_standard_name(self, standard_name: StandardName):
        """
        :param standard_name: The standard name of the variable
        """
        self._metadata['standard_name'] = standard_name.value

    def get_standard_name(self) -> t.Optional[str]:
        if 'standard_name' in self._metadata:
            return self._metadata['standard_name']
        return None

    def set_units(self, units: Unit):
        """
        :param units: The units for this variable
        """
        self._metadata['units'] = units.value

    def set_time_precision(self,time_precision: TimePrecision):
        """
        :param time_precision: The precision of the time variable (e.g. is it every hour, every day, etc.)
        """
        self._metadata['time_precision'] = time_precision.value

    def set_calendar(self, calendar: Calendar = Calendar.Standard):
        """
        :param calendar: The calendar used (the mixed Gregorian/Julian calendar typically used is the default)
        """
        self._metadata['calendar'] = calendar.value

    def set_time_zone(self, time_zone: TimeZone = TimeZone.UTC):
        """
        :param time_zone: The time zone used (the default standard is UTC - caution: ERDDAP does not work well with numeric non-UTC times)
        """
        self._metadata['time_zone'] = time_zone.value

    def set_numeric_time_units(self, base_units: NumericTimeUnits, epoch: datetime.datetime):
        """
        :param base_units: The basic units of duration (e.g. "seconds since")
        :param epoch: The reference time
        """
        self._metadata['units'] = f"{base_units.value} since {epoch.isoformat("T")}"

    def set_missing_value(self, missing_value: str):
        """
        :param missing_value: What should null values look like in the NetCDF file
        """
        self._metadata['missing_value'] = missing_value

    def set_conversion(self, scale_factor: t.Optional[NumberLike] = "", add_offset: t.Optional[NumberLike] = ""):
        """
        :param scale_factor: The value to multiply the stored value by (default 1)
        :param add_offset: The value to add to the stored value (default 0)
        """
        self._metadata['scale_factor'] = scale_factor
        self._metadata['add_offset'] = add_offset

    def set_ioos_category(self, category: IOOSCategory):
        """
        :param category: The IOOS category that best describes this variable
        """
        self._metadata['ioos_category'] = category.value

    def set_actual_range(self, min_val: t.Optional[NumberLike] = None, max_val: t.Optional[NumberLike] = None):
        """
        :param min_val: The actual minimum value in the dataset
        :param max_val: The actual maximum value in the dataset
        """
        self._metadata['actual_min'] = min_val
        self._metadata['actual_max'] = max_val

    def set_valid_range(self, min_val: t.Optional[NumberLike] = None, max_val: t.Optional[NumberLike] = None):
        """
        :param min_val: The minimum possible valid value in the dataset
        :param max_val: The maximum possible valid value in the dataset
        """
        self._metadata['valid_min'] = min_val
        self._metadata['valid_max'] = max_val

    def set_allow_subsets(self, subsets: bool):
        """
        :param subsets: Whether to let users build subsets of data using this variable (recommended only for variables with a limited number of options)
        """
        self._metadata['allow_subsets'] = not not subsets

    def set_role(self, role: t.Union[ERDDAPVariableRole, CFVariableRole]):
        """
        :param role: The role the variable plays in the dataset
        """
        if isinstance(role, ERDDAPVariableRole):
            self._metadata['erddap_role'] = role.value
        else:
            self._metadata['cf_role'] = role.value

    def set_comment(self, comment: str):
        """
        :param comment: Additional information on the variable
        """
        self._metadata['comment'] = comment

    def set_references(self, references: str):
        """
        :param references: References for the variable
        """
        self._metadata['references'] = references

    def set_source(self, source: str):
        """
        :param source: Where does the variable come from
        """
        self._metadata['source'] = source

    def set_coverage_content_type(self, content_type: CoverageContentType):
        """
        :param content_type: Information about what the variable represents
        """
        self._metadata['coverage_content_type'] = content_type.value

    def set_variable_order(self, order: int):
        """
        :param order: Higher numbers means the variable appears lower in the list of variables
        """
        self._metadata['variable_order'] = order

    def set_is_axis(self, is_axis: bool):
        """
        :param is_axis: Set to true only in gridded datasets and only when the value is one of the axes
        """
        self._metadata['is_axis'] = not not is_axis

    def set_is_altitude_proxy(self, is_proxy: bool):
        """
        :param is_proxy: Set to true if the variable is not altitude or depth but it can be a stand-in for them (like pressure)
        """
        self._metadata['altitude_proxy'] = not not is_proxy


class MaintenanceRecord(EntityRef):

    def __init__(self, date: datetime, notes: str, scope: MaintenanceScope = MaintenanceScope.Dataset):
        super().__init__()
        self.set_date(date)
        self.set_scope(scope)
        self.set_notes(notes)

    def set_date(self, date: datetime):
        """
        :param date: The date the change was made
        """
        self._metadata['date'] = EntityRef.format_date(date)

    def set_notes(self, notes: MultiLanguageString):
        """
        :param notes: Describe what was changed about the dataset or metadata
        """
        self._metadata['notes'] = EntityRef.format_multilingual_text(notes)

    def set_scope(self, scope: MaintenanceScope):
        """
        :param scope: Whether this is a maintenance record for the dataset or the metadata
        """
        self._metadata['scope'] = scope.value


class Resource(EntityRef):

    def __init__(self, url):
        super().__init__()
        self.set_url(url)

    def set_url(self, url: str):
        """
        :param url: The URL of the resource
        """
        self._metadata['url'] = url or ""
        if url:
            if 'protocol' not in self._metadata or not self._metadata['protocol']:
                self.set_resource_type()
            if 'goc_formats' not in self._metadata or not self._metadata['goc_formats']:
                self.set_gc_content_format()

    def set_resource_type(self, res_type: ResourceType = ResourceType.Auto):
        """
        :param res_type: The type of resource (i.e. the protocol used to access it)
        """
        url = self._metadata['url']
        if res_type != ResourceType.Auto:
            self._metadata['protocol'] = res_type.value
        elif url.startswith("https://"):
            self._metadata['protocol'] = "https"
        elif url.startswith("http://"):
            self._metadata['protocol'] = "http"
        elif url.startswith("ftp://"):
            self._metadata['protocol'] = "ftp"
        elif url.startswith("git://"):
            self._metadata['protocol'] = 'git'
        elif url.startswith("file://"):
            self._metadata['protocol'] = 'file'

    def set_gc_content_format(self, content_format: GCContentFormat = GCContentFormat.Auto):
        """
        :param content_format: The format of the content according to GC
        """
        url = self._metadata['url']
        if content_format != GCContentFormat.Auto:
            self._metadata['goc_formats'] = [content_format.value]
        elif url.upper().endswith(".TAR.GZ"):
            self._metadata['goc_formats'] = [GCContentFormat.ArchiveTARGZIP.value]
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
                    self._metadata['goc_formats'] = [extension]

    def set_name(self, name: MultiLanguageString):
        """
        :param name: The name of the resource
        """
        self._metadata['name'] = EntityRef.format_multilingual_text(name)

    def set_description(self, desc: MultiLanguageString):
        """
        :param desc: A description of the resource
        """
        self._metadata['description'] = EntityRef.format_multilingual_text(desc)

    def set_additional_request_info(self, info: MultiLanguageString):
        """
        :param info: Any additional information needed to make requests
        """
        self._metadata['protocol_request'] = EntityRef.format_multilingual_text(info)

    def set_additional_app_info(self, info: MultiLanguageString):
        """
        :param info: Any additional information needed to open the URL
        """
        self._metadata['app_profile'] = EntityRef.format_multilingual_text(info)

    def set_link_purpose(self, purpose: ResourcePurpose):
        """
        :param purpose:  Why would somebody want to use this resource?
        """
        self._metadata['function'] = purpose.value

    def set_gc_content_type(self, content_type: GCContentType):
        """
        :param content_type: Why would somebody want to use this resource, but more complicated
        """
        self._metadata['goc_content_type'] = content_type.value

    def set_gc_language(self, language: GCLanguage):
        """
        :param language: What languages is this resource available in?
        """
        self._metadata['goc_languages'] = language.value


class Contact(EntityRef):
    pass


class _ResponsibleParty(EntityRef):

    def __init__(self, role: ContactRole, contact: Contact):
        super().__init__()
        self._metadata['role'] = role
        self._children['contact'] = contact


class Citation(EntityRef):

    def __init__(self, title: MultiLanguageString):
        super().__init__()
        self._children['responsibles'] = []
        self._children['resource'] = None
        self.set_title(title)

    def add_responsible_party(self, role: ContactRole, contact: Contact):
        """
        :param role: The role the person plays for this citation
        :param contact: Their contact information
        """
        self._children['responsibles'].append(_ResponsibleParty(role, contact))

    def set_title(self, title: MultiLanguageString):
        """
        :param title: The title of the citation
        """
        self._metadata['title'] = EntityRef.format_multilingual_text(title)

    def set_alt_title(self, alt_title: MultiLanguageString):
        """
        :param alt_title: The alternative title of the citation
        """
        self._metadata['alt_title'] = EntityRef.format_multilingual_text(alt_title)

    def set_publication_date(self, pub_date: datetime.date):
        """
        :param pub_date: The date the citation was published
        """
        self._metadata['publication_date'] = EntityRef.format_date(pub_date)

    def set_revision_date(self, revision_date: datetime.date):
        """
        :param revision_date: The date the citation was last revised
        """
        self._metadata['revision_date'] = EntityRef.format_date(revision_date)

    def set_creation_date(self, creation_date: datetime.date):
        """
        :param creation_date: The date the citation was published
        """
        self._metadata['creation_date'] = EntityRef.format_date(creation_date)

    def set_edition_info(self, ed_name: t.Optional[MultiLanguageString] = None, ed_date: t.Optional[datetime.date] = None):
        """
        :param ed_name: The name of the edition
        :param ed_date: The date the citation was published
        """
        self._metadata['edition'] = EntityRef.format_multilingual_text(ed_name)
        self._metadata['publication_date'] = EntityRef.format_date(ed_date)

    def set_details(self, details: MultiLanguageString):
        """
        :param details: More details about this citation
        """
        self._metadata['details'] = EntityRef.format_multilingual_text(details)

    def set_isbn(self, isbn: str):
        self._metadata['isbn'] = isbn

    def set_issn(self, issn: str):
        self._metadata['issn'] = issn

    def set_resource(self, resource: Resource):
        """
        :param resource: A resource which the citation is in reference to (e.g. a web link to it)
        """
        self._children['resource'] = resource

    def set_identifier(self, code: str, id_system: IDSystem, description: t.Optional[MultiLanguageString] = None):
        """
        :param code: A unique identifier for this citation, e.g. a DOI
        :param id_system: The system used for the code (e.g. the DOI system)
        :param description: Optionally a description of what the ID code represents
        """
        self._metadata['id_code'] = code
        self._metadata['id_system'] = id_system
        self._metadata['description'] = EntityRef.format_multilingual_text(description)








class DatasetMetadata:

    def __init__(self):
        self._metadata: dict[str, t.Any] = {}
        self._act_workflow: t.Optional[str] = None
        self._pub_workflow: t.Optional[str] = None
        self._security_level: t.Optional[str] = None
        self._org_name: t.Optional[str] = None
        self._display_name: dict[str, str] = {}
        self._users: set[str] = set()
        self._profiles: set[str] = set('cnodc')
        self._children: dict[str, t.Union[EntityRef, list[EntityRef]]] = {
            'iso_maintenance': [],
            'variables': [],
            'canon_urls': [],
            'additional_docs': [],
            'metadata_profiles': [],
            'metadata_standards': [],
            'alt_metadata': [],
            'parent_metadata': None,
            'publisher': None,
            'metadata_owner': None,
            'responsibles': [],
        }

    def add_responsible_party(self, role: ContactRole, contact: Contact):
        self._children['responsibles'].append(_ResponsibleParty(role, contact))

    def set_metadata_owner(self, contact: Contact):
        self._children['metadata_owner'] = contact

    def set_publisher(self, publisher: Contact):
        self._children['publisher'] = publisher

    def set_parent_metadata(self, citation: Citation):
        self._children['parent_metadata'] = citation

    def add_alt_metadata(self, citation: Citation):
        self._children['alt_metadata'].append(citation)

    def add_metadata_standard(self, citation: Citation):
        self._children['metadata_standards'].append(citation)

    def add_metadata_profile(self, citation: Citation):
        self._children['metadata_profiles'].append(citation)

    def add_additional_documentation(self, citation: Citation):
        self._children['additional_docs'].append(citation)

    def add_record(self, record: MaintenanceRecord):
        """
        :param record: A maintenance record to add
        """
        self._children['iso_maintenance'].append(record)

    def add_variable(self, var: Variable):
        """
        :param var: A variable to add to the dataset
        """
        self._children['variables'].append(var)

    def add_canon_url(self, canon_url: Resource):
        self._children['canon_urls'].append(canon_url)

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

    def set_display_name(self, display_name: MultiLanguageString):
        """
        :param display_name: A string or multi-language dict containing the internal display name of the dataset.
        """
        self._display_name = EntityRef.format_multilingual_text(display_name)

    def set_english_display_name(self,display_name: str):
        """
        :param display_name: The English display name
        """
        self._display_name['en'] = display_name

    def set_french_display_name(self, display_name: str):
        """
        :param display_name: The French display name
        """
        self._display_name['fr'] = display_name

    def set_title(self, title: MultiLanguageString):
        """
        :param title: A string or multi-language dict containing the title of the dataset as displayed to the public/
        """
        self._metadata['title'] = EntityRef.format_multilingual_text(title)

    def set_english_title(self, title: str):
        """
        :param title: The English title
        """
        if 'title' not in self._metadata:
            self._metadata['title'] = {}
        self._metadata['title']['en'] = title

    def set_french_title(self, title: str):
        """
        :param title: The French title
        """
        if 'title' not in self._metadata:
            self._metadata['title'] = {}
        self._metadata['title']['fr'] = title

    # ACDD only stuff

    def set_institution(self, institution: str):
        """
        :param institution: The name of the institution that owns the data (NetCDF only)
        """
        self._metadata['institution'] = institution

    def set_program(self, program: str):
        """
        :param program: The name of the program that collected the data (NetCDF only)
        """
        self._metadata['program' ] = program

    def set_conventions(self, conventions: list[str]):
        """
        :param conventions: A list of convention names (NetCDF only)
        """
        self._metadata['conventions'] = ','.join(conventions)

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

    def set_feature_type(self, feature_type: CommonDataModelType):
        """
        :param feature_type: The type of data that is contained in this dataset. Only set it if using a NetCDF format
                             recognized by the CF Conventions that is compatible with the feature type.
        """
        self._metadata['feature_type'] = feature_type.value

    # Combined stuff

    def set_credit(self, credit: MultiLanguageString):
        """
        :param credit: A string or multi-language dict providing acknowledgement of contributions, extra work done, etc.
        """
        self._metadata['acknowledgement'] = EntityRef.format_multilingual_text(credit)

    def set_comment(self, comment: MultiLanguageString):
        """
        :param comment: A string or multi-language dict with additional less-important information on the dataset.
        """
        self._metadata['comment'] = EntityRef.format_multilingual_text(comment)

    def set_doi(self, doi: str):
        """
        :param doi: The DOI, ideally without a prefix like "https://doi.org" or "doi:"
        """
        if doi.startswith("https://doi.org/"):
            doi = doi[16:]
        elif doi.startswith("http://doi.org/"):
            doi = doi[15:]
        elif doi.startswith("doi:"):
            doi = doi[4:]
        self.set_identifier(doi, IDSystem.DOI)

    def set_identifier(self, code: str, id_system: IDSystem, desc: t.Optional[MultiLanguageString] = None):
        self._metadata['dataset_id_code'] = code
        self._metadata['dataset_id_system'] = id_system
        self._metadata['dataset_id_description'] = EntityRef.format_multilingual_text(desc)

    def set_processing_info(self, processing_level: t.Optional[str] = None, processing_desc: t.Optional[MultiLanguageString] = None, processing_environment: t.Optional[MultiLanguageString] = None, processing_system: t.Optional[IDSystem] = None):
        """
        :param processing_level: A short code indicating the processing level of the  data.
        :param processing_desc: A string or multi-language dict describing the processing done on this dataset.
        :param processing_environment: A string or multi-language dict describing the environment in which the processing was done.
        """
        self._metadata['processing_level'] = processing_level
        self._metadata['processing_description'] = EntityRef.format_multilingual_text(processing_desc)
        self._metadata['processing_environment'] = EntityRef.format_multilingual_text(processing_environment)
        self._metadata['processing_system'] = processing_system if processing_system is not None else None

    def set_purpose(self, purpose: MultiLanguageString):
        """
        :param purpose: A string or multi-language dict describing the purpose of the dataset
        """
        self._metadata['purpose'] = EntityRef.format_multilingual_text(purpose)

    def set_references(self, references: MultiLanguageString):
        """
        :param references: A string or multi-language dict describing any relevant references for the dataset.
        """
        self._metadata['references'] = EntityRef.format_multilingual_text(references)

    def set_source(self, source: MultiLanguageString):
        """
        :param source: A string or multi-language dict describing the source of the data.
        """
        self._metadata['source'] = EntityRef.format_multilingual_text(source)

    def set_abstract(self, abstract: MultiLanguageString):
        """
        :param abstract: A string or multi-language dict with a short summary or abstract of the dataset.
        """
        self._metadata['summary'] = EntityRef.format_multilingual_text(abstract)

    def set_horizontal_bounds(self, lat_min: decimal.Decimal, lat_max: decimal.Decimal, lon_min: decimal.Decimal, lon_max: decimal.Decimal, boundary_wkt: t.Optional[str] = None, ref_system: CoordinateReferenceSystem = CoordinateReferenceSystem.WGS84):
        """
        :param lat_min: The minimum latitude of the dataset
        :param lat_max: The maximum latitude of the dataset
        :param lon_min: The minimum longitude of the dataset
        :param lon_max: The maximum longitude of the dataset
        :param boundary_wkt: The WKT polygon shape of the dataset, if available
        :param ref_system: The coordinate reference system used, defaults to WGS84
        """
        self._metadata["geospatial_lat_min"] = str(lat_min)
        self._metadata["geospatial_lat_max"] = str(lat_max)
        self._metadata["geospatial_lon_min"] = str(lon_min)
        self._metadata["geospatial_lon_max"] = str(lon_max)
        self._metadata["geospatial_bounds"] = boundary_wkt
        self._metadata["geospatial_bounds_crs"] = ref_system.value

    def set_vertical_bounds(self, vertical_min: decimal.Decimal, vertical_max: decimal.Decimal, ref_system: CoordinateReferenceSystem = CoordinateReferenceSystem.MSL_Depth):
        """
        Note that minimum and maximum refer to the value of the number, not to the deepest or shallowest value.
        :param vertical_min: The minimum depth of the dataset
        :param vertical_max: The maximum depth of the dataset
        :param ref_system: The vertical coordinate reference system used, defaults to MSL_Depth (positive values from an unspecified MSL reference datum)
        """
        self._metadata['geospatial_vertical_min'] = str(vertical_min)
        self._metadata['geospatial_vertical_max'] = str(vertical_max)
        self._metadata['geospatial_bounds_vertical_crs'] = ref_system.value

    def set_temporal_bounds(self, start_time: datetime.datetime, end_time: t.Optional[datetime.datetime] = None, calendar: CoordinateReferenceSystem = CoordinateReferenceSystem.Gregorian):
        """
        :param start_time: The start time for the dataset. Include timezone.
        :param end_time: The end time for the dataset. Include timezone. Optional; if not provided, dataset is flagged as "ongoing".
        :param calendar: The temporal coordinate reference system used, defaults to Gregorian calendar.
        """
        self._metadata['time_coverage_start'] = EntityRef.format_date(start_time)
        if end_time is not None:
            self._metadata['time_coverage_end'] = EntityRef.format_date(end_time)
            self._metadata['is_ongoing'] = False
        else:
            self._metadata['is_ongoing'] = True
        self._metadata['temporal_crs'] = calendar.value

    def set_date_issued(self, date: datetime.date):
        """
        :param date: The date the dataset was first made available to the public.
        """
        self._metadata['date_issued'] = EntityRef.format_date(date)

    def set_date_created(self, date: datetime.date):
        """
        :param date: The date the dataset was first created.
        """
        self._metadata['date_created'] = EntityRef.format_date(date)

    def set_date_modified(self, date: datetime.date):
        """
        :param date: The date the dataset was last modified.
        """
        self._metadata['date_modified'] = EntityRef.format_date(date)


    # ISO stuff

    def set_metadata_maintenance_frequency(self, freq: MaintenanceFrequency):
        """
        :param freq: How often is the metadata updated?
        """
        self._metadata['metadata_maintenance_frequency'] = freq.value

    def set_dataset_maintenance_frequency(self, freq: MaintenanceFrequency):
        """
        :param freq: How often is the dataset updated?
        """
        self._metadata['resource_maintenance_frequency'] = freq.value

    def set_topic_category(self, cat: TopicCategory):
        """
        :param cat: The topic category (usually TopicCategory.Oceans)
        """
        self._metadata['topic_category'] = cat.value

    def set_status(self, status: StatusCode):
        """
        :param status: The status of the dataset (e.g. StatusCode.Final if no changes are expected)
        """
        self._metadata['status'] = status.value

    def set_spatial_representation(self, spatial_rep: SpatialRepresentation):
        """
        :param spatial_rep: For datasets with coordinates, what does the data look like (tabular, gridded, vector, etc).
        """
        self._metadata['spatial_representation_type'] = spatial_rep.value


    # CNODC stuff

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

    def set_file_storage_location(self, storage_path: MultiLanguageString):
        """
        :param storage_path: A string or multi-language dict describing the location where the files are stored.
        """
        self._metadata['file_storage_location'] = EntityRef.format_multilingual_text(storage_path)

    def set_internal_notes(self, notes: MultiLanguageString):
        """
        :param notes: A string or multi-language dict with internal notes (not published).
        """
        self._metadata['internal_notes'] = EntityRef.format_multilingual_text(notes)

    def set_is_available_via_request_form(self, val: bool):
        """
        :param val: True if you can request this data via our request form.
        """
        self._metadata['via_meds_request_form'] = not not val

    def set_government_metadata(self,
                                publication_places: t.Union[GCPlace, list[GCPlace], set[GCPlace]] = GCPlace.Ottawa,
                                subject: GCSubject = GCSubject.Oceanography,
                                collection: GCCollectionType = GCCollectionType.Geospatial,
                                audiences: t.Union[GCAudience, list[GCAudience], set[GCAudience]] = GCAudience.Scientists):
        """
        :param publication_places: The place(s) this dataset was published from (defaults to Ottawa)
        :param subject: The subject of this dataset (defaults to Oceanography)
        :param collection: The collection of this dataset (defaults to Geospatial data)
        :param audiences: The audience(s) this dataset is intended for (defaults to Scientists)
        """
        self._metadata['goc_audience'] = [audiences.value] if isinstance(audiences, GCAudience) else [x.value for x in audiences]
        self._metadata['goc_subject'] = subject.value
        self._metadata['goc_collection_type'] = collection.value
        self._metadata['goc_publication_place'] = [publication_places.value] if isinstance(publication_places, GCPlace) else [x.value for x in publication_places]

    # ERDDAP stuff

    def set_erddap_info(self, dataset_id: str, dataset_type: ERDDAPDatasetType, file_path: t.Optional[str] = None, file_pattern: t.Optional[str] = None):
        """
        :param dataset_id: The ID of the dataset as it should be used in ERDDAP (must be unique)
        :param file_path: The path of the files on the ERDDAP server
        :param file_pattern: If multiple files are stored in that path, the file pattern to match (otherwise all files are used)
        """
        self.add_profile('erddap')
        self._metadata['erddap_data_file_path'] = file_path
        self._metadata['erddap_data_file_pattern'] = file_pattern
        self._metadata['erddap_dataset_id'] = dataset_id
        self._metadata['erddap_dataset_type'] = dataset_type.value


    def build_request_body(self) -> dict:
        body = {
            'profiles': list(self._profiles),
            'org_name': self._org_name,
            'display_names': self._display_name,
            'users': list(self._users),
            'metadata': self._metadata,
            'activation_workflow': self._act_workflow,
            'publication_workflow': self._pub_workflow,
            'security_level': self._security_level,
        }
        for key in self._children.keys():
            if self._children[key] is None:
                continue
            elif isinstance(self._children[key], EntityRef):
                body['metadata'][key] = self._children[key].build_request_body()
            else:
                body['metadata'][key] = [
                    x.build_request_body()
                    for x in self._children[key]
                ]
        return body

