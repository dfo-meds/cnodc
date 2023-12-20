# Workflows

Workflows define how a file is processed when it is received by the CNODC. 
The processing follows the general structure below, with all components being
optional:

1. Validate the file, if necessary
2. Save the file to a primary working location in the cloud
3. Save the file to one or more alternate locations in the cloud
4. Run the file through a sequence of queues to process it further

## Configuration

Configuration is done via a mapping (typically specified in YAML) that follows
the following scheme:

```yaml 
filename_pattern: ''  # Pattern to construct file names, see below
accept_user_filename: false  # Set to true to allow users to specify a filename
validation: ''  # Fully qualified path to a Python callable to verify the submitted file
allow_overwrite: 'user'  # Set to 'never' to prevent users from overwriting files or 'always' to always allow overwrites.  
working_target:     # Configuration for where the working copy of the file is to be stored
  directory: ''     # Path to the directory to store the file 
  gzip: false       # Set to true to gzip this copy of the file
  metadata:         # Metadata to store with this copy (if possible)
    name1: value1   # Names and values must be strings; values may be patterns, see below
    name2: value2
    ...
  tier:  ''         # One of 'frequent' (default), 'infrequent' or 'archival' 
additional_targets:  # Configuration for where additional copies should be stored
  -
    directory: ''  
    ...
  -
    directory: ''
    ...
processing_steps:  # A list of queues to be executed in sequence (see below)
  - 
    queue_name: ''  # The name of the queue
    priority: 0     # The priority to enqueue items as 
  ...
    
```

### Common tokens
```yaml
filename: ''          # User-specified filename (where accepted)
default-filename: ''  # Fallback filename, provided by the upload manager
request-id: ''        # A unique identifier for each file submission
workflow-name: ''     # The name of the workflow to execute
last-modified-time: ''  # The date/time (in ISO format) of the best guess of the last modified date of the file

```

### Pattern replacements
The following strings can be used in filename or metadata patterns:

- %{TOKEN_NAME}: Replaced with the contents of the given header. Use the lower case name of the token above.
- %{now}: Replaced with the ISO formatted current date/time

Note that filenames and metadata values are sanitized to prevent injection attempts.

## Workflow Execution

### Step 1 - Validation
If `validation` is specified, it must be a fully-qualified path to a Python callable
object, typically a function. This function must raise an error (ideally `CNODCError` or
a sub-class) if it finds any issues with the submitted files. If one is raised, processing
halts and the exception logged for later review. If the submission is via an interactive 
process, the user is notified about the exception.

### Step 2 - File Uploads
The submitted file is then saved to any directories specified. Appropriate metadata is added
and the file may be compressed using `gzip` using the appropriate options.

If any upload fails, any previously successful upload is removed if possible. However, removal
cannot be guaranteed (e.g. in the event that the cloud service has gone down).

Of note, the storage tier of files (if applicable to the storage system) is always initially set
to the equivalent of `StorageTier.FREQUENT` (i.e. HOT for Azure Blob). This is because storage 
systems often have an early delete penalty and, if the workflow later fails, the penalty will 
be incurred. Instead, storage tiers are set after the entire process is completed. Failure to 
set a storage tier is logged but not treated as a fatal error.

### Step 3 - Processing
For processing queues to be used, a `working_target` must be specified. The path to the file used
is the one specified by the `working_target`. It is an error to specify `processing_queues` without
a valid `working_target` entry.

The entries in `processing_steps` are called in sequence and calling of the next entry is managed
by the previous step. There are two kinds of step payloads that can be created:

1. A file payload, referring to a source file stored in the cloud
2. An NODB item payload, referring to a specific entry in the NODB. A batch payload is similar but 
   refers to a set of NODB items. 
   
Note that a step may consist of more than one queue. For the purposes of designing a workflow, only 
two things are important: the input payload of the first queue and the output payload of the last queue. 
These must be of the types given above. In designing a workflow, it is important to match the output of 
the previous item to the input of the following.

For example, an NODB loader step typically consumes file payloads and produces NODB item payloads. The
step immediately upstream must produce a file payload and the step immediately downstream must consume
item payloads. 

After file uploads are complete, a file payload is created from the working target and sent to the first step.


