F-Spot FUSE filesystem
Copyright (C) 2009-2010 Matias Aguirre <matiasaguirre@gmail.com>


1. Description
==============
F-Spot FS implements a FUSE filesystem (http://fuse.sourceforge.net/) which
access F-Spot database to build a directory hierarchy of tag, sub-tags and
tagged photos. This way it's easy to traverse the photo collection while
keeping it organized in it's original format (by date, etc.).

Images are soft links to original files.

Write access is available, when copying/moving a file into the virtual
filesystem, implies copying to collection directory (defined in gconf)
and tagging properly.


2. Dependencies
===============
Some dependencies:

* F-Spot.
  F-Spot FS uses F-Spot collection database [F-Spot](http://f-spot.org).
  Currently schema version 17 and upper is supported. Also it's gconf
  configuration is needed to retrieve collection directory.
* [Python FUSE](http://pypi.python.org/pypi/fuse-python/)
* [Python SQLite](http://docs.python.org/library/sqlite3.html)
* SQLAlchemy 0.5.8 or higher [SQLAlchemy](http://www.sqlalchemy.org/)


3. Installation
===============
Install using [distutils](http://docs.python.org/install/#the-new-standard-distutils).

`$ sudo python setup.py install`


4. Run
======
To run just invoke the installed script, it uses some defaults values
like mount directory `$HOME/.photos/`.

`$ fsfs`

For more details:

`$ fsfs --help`
