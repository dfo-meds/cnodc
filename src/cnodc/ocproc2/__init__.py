"""Provides a core implementation of OCPROC2.

    OCPROC2 is a flexible storage format for scientific data that focuses on
    storing individual observations or samples. Within the oceanography context, this
    means the format is typically used for an individual profile or set of
    surface observations, though other uses are possible.

    The format is based on the concept of a "record". A record stores information,
    organized into elements which are grouped into three categories: metadata,
    parameters, and coordinates.

    Coordinates represent what NetCDF refers to as a "dimension". They provide
    context to situate the record within a larger dataset, such as latitude,
    time, depth, longitude, bin number, etc.

    Parameters represent measurements or observations taken within the context
    described by the coordinates. For example, temperature, salinity, current
    speed, wind speed, and cloud cover percentage are all parameters.

    Metadata elements represent additional information about the record, such
    as the station's WMO number, the units of a parameter, the uncertainty
    associated with a value, etc.

    Metadata can be applied at multiple levels: records have metadata and elements
    themselves can have their own metadata. In addition, record sets (as described
    below) can have their own metadata.

    Records may also have child records, which represent a collection of elements
    with a further qualification of the coordinates. These are grouped into
    record sets, which are ordered collections of records. Each record set has
    a record set type and index (e.g. PROFILE #1, PROFILE #2, etc.). The record
    set type describes how the child record relates to its parent:

    - PROFILE records are located at the same coordinates as their parent but
      provide an additional Depth or Pressure coordinate to indicate the depth
      within the ocean.
    - SPEC_WAVE records are located at the same coordinates as their parent and
      are used to provide the spectral wave measurements for each bin. They use
      a CentralFrequency pseudo-coordinate to uniquely identify the bins.
    - WAVE_SENSORS records are used to distinguish measurements from a heave
      sensor and a slope sensor when both are present on the same instrument.
    - TSERIES records are located at the same coordinates as their parent but
      provide an additional Time or TimeOffset (i.e. relative to the parent)
      coordinate.

    Child records may have their own child records (e.g. a sensor array that takes
    temperatures at various depths and at regular intervals might generate a set of
    TSERIES child records each of which has PROFILE child records at each depth).

    OCPROC2 records are designed to be coordinated with CF-compliant NetCDF files for
    long-term storage and distribution. To this end, the following data structures
    are strongly recommended to align with the discrete sampling geometries used in
    CF NetCDF files:

    - Profiles should be stored as a parent record with Latitude, Longitude, and Time
      coordinates and all surface observations. Information at depth should be recorded
      in child records of type PROFILE with either Depth or Pressure coordinates.
    - Points should be stored as a single record with Latitude, Longitude, Time and
      optionally Depth or Pressure (assumed to be 0/surface-level if omitted).
    - Trajectories (and Trajectory Profiles) should be stored as individual OCPROC2
      records and assembled into trajectories.
    - Time series (and Time Series Profiles) are ideally stored as individual OCPROC2
      records and assembled into time series. Where this is not feasible, the TSERIES
      can be used instead. In the case of Time Series Profiles, the PROFILE child
      records should be children of the TSERIES records (i.e. Parent > TSERIES > PROFILE).
    - Child records using pseudo-coordinates (e.g. SPEC_WAVE records) should be included
      at the most sensible location - for example, wave records are taken at the surface,
      so SPEC_WAVE records should be stored under a parent record representing a location
      on the surface.

    Metadata and parameters should be set at the most relevant place; for example, a
    WMO ID describes the platform that took all of the information, so it should be on
    the parent record. A temperature sensor serial number should be on the temperature
    parameter itself. A digitization indicator for a profile should be on the profile's
    metadata.

    Parent records also have two special types of metadata they store, which are more
    relevant to the processing of the record than the original record itself:

    - History entries record actions taken on the record after it was created. This
      includes the time of the action, a description of it, the user who performed it,
      and more.
    - QC test results record the outcome of quality control tests applied to the record.
      This includes the outcome itself, notes, the time of the test, information about
      the test, and more.
"""
from .operations import QCOperator, QCSetValue, QCAddHistory, QCSetWorkingQuality
from .structures import BaseRecord, ParentRecord, ChildRecord, RecordSet, RecordMap, ElementMap
from .elements import MultiElement, AbstractElement, SingleElement, ElementMap
from .history import HistoryEntry, QCResult, QCMessage, normalize_qc_path, MessageType, QCTestRunInfo
from .ontology import OCProc2Ontology, OCProc2ElementInfo, OCProc2ChildRecordTypeInfo
