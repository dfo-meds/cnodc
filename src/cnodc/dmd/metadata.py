import decimal
import enum
import typing as t
import datetime


MultiLanguageString = t.Union[str, dict[str, str]]


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



class DatasetMetadata:

    def __init__(self):
        self._metadata = {}
        self._act_workflow = None
        self._pub_workflow = None
        self._security_level = None
        self._org_name = None
        self._display_name = {}
        self._users = set()
        self._profiles = set('cnodc')

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
        if isinstance(display_name, str):
            self._display_name = {
                'und': display_name,
            }
        else:
            self._display_name = display_name

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
        if isinstance(title, str):
            self._metadata['title'] = {
                'und': title,
            }
        else:
            self._metadata['title'] = title

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

    # standard name vocab

    # Combined stuff

    def set_credit(self, credit: MultiLanguageString):
        """
        :param credit: A string or multi-language dict providing acknowledgement of contributions, extra work done, etc.
        """
        self._metadata['acknowledgement'] = credit

    def set_comment(self, comment: MultiLanguageString):
        """
        :param comment: A string or multi-language dict with additional less-important information on the dataset.
        """
        self._metadata['comment'] = comment

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
        self._metadata['dataset_id_code'] = doi
        self._metadata['dataset_id_system'] = {
            '_guid': 'doi',
            'code_space': 'https://doi.org/',
            'version': '',
        }

    def set_processing_info(self, processing_level: t.Optional[str] = None, processing_desc: t.Optional[MultiLanguageString] = None, processing_environment: t.Optional[MultiLanguageString] = None):
        """
        :param processing_level: A short code indicating the processing level of the  data.
        :param processing_desc: A string or multi-language dict describing the processing done on this dataset.
        :param processing_environment: A string or multi-language dict describing the environment in which the processing was done.
        """
        self._metadata['processing_level'] = processing_level
        self._metadata['processing_description'] = processing_desc
        self._metadata['processing_environment'] = processing_environment

    def set_purpose(self, purpose: MultiLanguageString):
        """
        :param purpose: A string or multi-language dict describing the purpose of the dataset
        """
        self._metadata['purpose'] = purpose

    def set_references(self, references: MultiLanguageString):
        """
        :param references: A string or multi-language dict describing any relevant references for the dataset.
        """
        self._metadata['references'] = references

    def set_source(self, source: MultiLanguageString):
        """
        :param source: A string or multi-language dict describing the source of the data.
        """
        self._metadata['source'] = source

    def set_abstract(self, abstract: MultiLanguageString):
        """
        :param abstract: A string or multi-language dict with a short summary or abstract of the dataset.
        """
        self._metadata['summary'] = abstract

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
        self._metadata['time_coverage_start'] = start_time.isoformat("T")
        if end_time is not None:
            self._metadata['time_coverage_end'] = end_time.isoformat("T")
            self._metadata['is_ongoing'] = False
        else:
            self._metadata['is_ongoing'] = True
        self._metadata['temporal_crs'] = calendar.value

    def set_date_issued(self, date: datetime.date):
        """
        :param date: The date the dataset was first made available to the public.
        """
        self._metadata['date_issued'] = date.isoformat()

    def set_date_created(self, date: datetime.date):
        """
        :param date: The date the dataset was first created.
        """
        self._metadata['date_created'] = date.isoformat()

    def set_date_modified(self, date: datetime.date):
        """
        :param date: The date the dataset was last modified.
        """
        self._metadata['date_modified'] = date.isoformat()


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

    def set_file_storage_location(self, storage_path: MultiLanguageString):
        """
        :param storage_path: A string or multi-language dict describing the location where the files are stored.
        """
        self._metadata['file_storage_location'] = storage_path

    def set_internal_notes(self, notes: MultiLanguageString):
        """
        :param notes: A string or multi-language dict with internal notes (not published).
        """
        self._metadata['internal_notes'] = notes

    def set_is_available_via_request_form(self, val: bool):
        """
        :param val: True if you can request this data via our request form.
        """
        self._metadata['via_meds_request_form'] = not not val


    # ERDDAP stuff

    def set_erddap_info(self, dataset_id: str, file_path: t.Optional[str] = None, file_pattern: t.Optional[str] = None):
        """
        :param dataset_id: The ID of the dataset as it should be used in ERDDAP (must be unique)
        :param file_path: The path of the files on the ERDDAP server
        :param file_pattern: If multiple files are stored in that path, the file pattern to match (otherwise all files are used)
        """
        self.add_profile('erddap')
        self._metadata['erddap_data_file_path'] = file_path
        self._metadata['erddap_data_file_pattern'] = file_pattern
        self._metadata['erddap_dataset_id'] = dataset_id



    def build_request_body(self) -> dict:
        return {
            'profiles': list(self._profiles),
            'org_name': self._org_name,
            'display_names': self._display_name,
            'users': list(self._users),
            'metadata': self._metadata,
            'activation_workflow': self._act_workflow,
            'publication_workflow': self._pub_workflow,
            'security_level': self._security_level,
        }

