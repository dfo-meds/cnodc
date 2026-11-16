Workflow
========

Metadata
--------
Metadata can be provided by the various file intake methods.
There are some key metadata fields that are used in processing files:

* ``last-modified-date``: An ISO-formatted date to use as the last modified date of the record (assumed UTC if no timezone present)
* ``filename``: the original filename
* ``source-name``: the source, used to track file origins
* ``program-name``: the program, used to separate incoming data by program at MEDS
* ``source-file-identifier``: A unique identifier for this file which is used to match different versions of the same file

Filename Patterns
-----------------
* ``safe_filename``: the approved filename
* ``safe_stem``: the stem of the safe file name (i.e. filename without extension)
* ``safe_ext``: the extension of the safe file name
* ``now``: the current time (UTC) in ``YYYYmmddHHMMSS`` format
* ``today``: the current time (UTC) in ``YYYYmmdd`` format (guaranteed to match that of ``now``)
* ``safe_mtime``: the file's modified time (UTC) in ``YYYYmmddHHMMSS`` format
