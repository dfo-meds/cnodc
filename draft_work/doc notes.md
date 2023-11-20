


# Web API

Authenticated calls should retrieve a token via /login.
The token should then be included as an Authorization header, i.e.
  Authorization: Bearer TOKEN

The token must be renewed before the expiry runs out via /renew


## Authentication

/login  JSON

request
username: str
password: str
shared: str [optional]

response
token: str
expiry: str (iso format)
shared: str [optional]


/renew  JSON

request
shared: str [optional]

response
token: str
expiry: str (iso format)
shared: str [optional]


/change-password  JSON

request
new_password


/users JSON
TBD

/users/<username> JSON
TBD

/users/<username>/create  JSON

request
password: str


/users/<username>/update JSON

request
password: str [optional]
is_active: bool [optional]


/users/<username>/assign-role  JSON

request
role_name: str


/users/<username>/unassign-role  JSON

request
role_name: str





## File Upload

Submit a file for upload to /submit/<workflow_name>. The body of the request
is the data to upload.

workflow_name is in the table that defines a configuration of what to do with the file

useful headers:
X-CNODC-Upload-MD5: The MD5 checksum of the content sent along with the request. If provided, an error is raised if the checksums don't match
X-CNODC-More-Data: Set to '1' to indicate a partial upload.
X-CNODC-Token: Set to a token for continuing or cancelling a partial upload 
X-CNODC-Filename: Set a filename to use for the uploaded file. 
X-CNODC-AllowOverwrite: Allows overwriting of the file (if the configuration allows it)

If a partial upload is indicated, the response will be a JSON payload containing

more_data_endpoint: URL to submit more data to
cancel_endpoint: URL to cancel the request (and remove the previous uploads)
x-cnodc-token: Value to pass to more_data_endpoint or cancel_endpoint as a header (i.e. X-CNODC-Token: TOKEN_VALUE)

The last request for a partial upload should set X-CNODC-More-Data to '0' to complete the upload.

File names are restricted to the characters `A-Za-z0-9._-` and at most 255 characters. Other characters are removed.
If the file name is omitted, longer than 255 characters, all periods, or matches a Windows
reserved path name, the request ID (which should be unique) is used instead.

AllowOverwrite is only respected if the config value is not 'always' or 'never'.

workflow configuration example (as a JSON object):

validation: The fully qualified path of a Python callable object to load and call. It must take a pathlib.Path object
  to the local path and a dict object of headers. It may raise an error (ideally sub-classing CNODCError) if validation
  fails or simply return False/None to raise a generic error. Must return True if validation passes
allow_overwrite: Set to 'always' to always allow overwrite, 'never' to never allow overwrite or NULL (or omit) to allow the user to decide
metadata: Set to a dictionary of strings to strings that will be uploaded along with the file if available
  Replacement from header values can be done by including the token %{header_name}, omitting the X-CNODC prefix and putting the header name in lower case.
  Only X-CNODC prefixed headers can be used
  Metadata names must consist of letters, digits and underscores and must start with not a digit 
  Metadata values must consist of only valid characters for HTTP header values. To ensure compliance with this, non-compliant characters are url encoded automatically
  The special value %{now} can be used to put the current date/time in ISO format (in UTC timezone)
upload: The directory (as understood by the FileController) to upload the file to
upload_tier: The StorageTier text value to save the file as (if supported):
  - frequent: For very frequently used files (the default); this corresponds to Azure's HOT 
  - infrequent: For infrequently used files; corresponds to Azure's COOL
  - archival: For rarely used files; corresponds to Azure's ARCHIVE
archive: The directory (as understood by the FileController) to upload the file to
archive_tier: The StorageTier text value to save the file as (if supported)
    - as above but defaults to ARCHIVE
queue: The queue name to send a queue item to when upload is complete
permission: A permission name (that can be assigned to a role) that is required to upload files to this workflow






