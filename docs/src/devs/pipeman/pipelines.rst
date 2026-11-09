Data Pipelines
==============

A data pipeline, also called a "workflow", is an ordered set of steps to take on a new file that has been
delivered to MEDS.

When a file is received, it is validated using the validation function, if one is provided. If it is invalid,
processing stops. Otherwise, it is converted into a :py:class:`pipeman.processing.payloads.FilePayload`. This
item is then queued to the first step in the workflow by sending it to the given queue name. Whenever the step
returns the item for the next step, the next step is identified and the payload object sent to the next step.

The pipeline is defined in YAML using a configuration file like the following ::

    # This will be used when users select a pipeline to send data to
    label:
      en: English Name
      fr: Nom en francais

    # A fully-qualified path to a function that can validate a new file
    # The validation function must match the following signature:
    #   validation(local_path: pathlib.Path, metadata: dict[str, str], filename: str)
    # and it should raise an exception if the function is invalid
    validation: pipeman.programs.whatever.validate_my_file

    # Change to true to treat the original filename as safe
    accept_user_filename: false

    # A pattern to use to name the file
    filename_pattern: ""

    # A dictionary of metadata to use for all new files
    default_metadata:
      key1: value1

    # A set of permission names that the user must have to submit files
    permissions:
      - permission1

    # Any file larger than this will be rejected.
    max_file_size: 91234

    # This is the primary working copy of the file that will be used
    working_target:

      # The path, in a format that the storage system will understand
      directory: "/path/to/directory"

      # Set to one of:
      # - always: Always allow overwrite, even if the user says don't
      # - never: Never allow overwrite, even if the user says its ok
      # - user: Rely on the user to tell us if file overwriting is ok or not
      allow_overwrite: "always"

      # Set to true to gzip the file
      gzip: true

      # Set to one of:
      # - frequent: used a lot
      # - infrequent: used less often
      # - archival: no access expected in near future (backups only)
      # Note this only has an effect if the storage engine supports it.
      storage_tier: "frequent"

      # Set to true to keep all the versions of the file
      # this tracks by modified date.
      keep_versions: false

    # Additional copies are stored here (for archival for example)
    additional_targets:
      - directory: "/path/to/archive"
      - allow_overwrite: "always"
      - gzip: true

    # Each step is identified by its key (used to track which step an item is on)
    # along with information on how to process the item
    processing_steps:

      step1:

        # The queue name to send the item to
        name: queue_name1

        # The order that the queue item should be sent in
        order: 1

        # The priority the item should be queued as (default is 0)
        priority: 0

        # Additional configuration for workers
        worker_config:
          worker_name:
            worker_config_key: worker_config_value


      step2:
        name: queue_name2
        order: 2


See :py:class:`nodb.workflow.WorkflowConfiguration` for more details.
