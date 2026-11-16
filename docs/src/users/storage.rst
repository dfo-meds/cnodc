Storage System
==============
The storage system allows us to handle files from many different types of storage systems
in a common way.


Supported Paths
---------------
The system recognizes the following types of paths:

* The local filesystem (e.g. ``C:\my\path``, ``/home/user/foobar``, or ``//network/share/folder``)
* FTP servers (e.g. ``ftp://server.domain/my/path``, may also start with ``ftps://`` or ``ftpse://``)
* Azure Blobs (e.g. ``https://account.blob.core.windows.net/container/blob/path``)
* Azure Files (e.g. ``https://account.file.core.windows.net/share/file/path``)


Base Paths
----------
The storage system offers a method to define common base paths that can be reused.
To define a base path, define it in the configuration file like so:

.. code:: toml

   [storage.base_names]
   user_dir = "/home/user/
   our_ftp = "ftp://our-ftp.com"

You can then use ``base:BASE_PATH://file/path``, replacing ``BASE_PATH`` with one of the keys that you defined.
For example, ``base:user_dir://foobar/`` is the same as ``/home/user/foobar/`` with the above configuration.


Azure Configuration
-------------------
You can configure the connection strings for Azure connections like so:

.. code:: toml

   [storage.azure.ACCOUNT_NAME]
   connection_string = "blahblahblah"

This connection string is then used for any connection to a URL like ``https://ACCOUNT_NAME.SERVICE.core.windows.net``
where ``SERVICE`` is one of the Microsoft storage services.

.. note::
   At the moment, only connection strings are supported.


FTP Configuration
-----------------
You can configure the connection information for an FTP server using its domain name like so:

.. code:: toml

   [storage.ftp.DOMAIN_NAME]
   # set to "explicit" to use TLS
   # tls = "none"

   # set the port used to access
   # port = 21

   # set the username and password to login
   # username = ""
   # password = ""

   # set the timeout for connections
   # timeout = 60

   # set the maximum number of bytes to be sent at one time
   # buffer_size = 262144

   # set to true or false if you know the server supports RFC 3659 commands
   # otherwise, its auto-detected the first time its needed.
   # rfc3659_support = false

   # set the timezone of the date/times returned
   # default assumption is UTC
   # server_timezone = "Etc/UTC"
