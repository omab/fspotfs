#!/usr/bin/python
"""
Copyright (C) 2009  Matias Aguirre <matiasaguirre@gmail.com>

F-SpotFS is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import os, sys, stat, errno, fuse, time, tempfile, Image, ExifTags, shutil
from datetime import datetime
from urllib import unquote, quote
from optparse import OptionParser, OptionError
from os.path import basename, dirname, join, isfile, isabs, isdir, exists

from .fspotdb import *

# F-Spot gconf key that stores user collection path,
# this should be on database IMO
FSPOT_SP_GCONF_KEY = '/apps/f-spot/import/storage_path'

try:
    import gconf
    COLLECTION_ROOT = gconf.client_get_default().get_value(FSPOT_SP_GCONF_KEY)
    # DateTime Exif Tag id
    DATETIME_ID = dict((v, k) for k, v in ExifTags.TAGS.iteritems())['DateTime']
    DISABLE_IMPORT = False
except (ImportError, KeyError):
    DISABLE_IMPORT = True

DESCRIPTION        = 'F-Spot FUSE Filesystem'
FSPOT_DB_FILE      = 'f-spot/photos.db'
FSPOT_DB_VERSION   = '17' # database version supported
DEFAULT_MOUNTPOINT = join(os.environ['HOME'], '.photos')
ROOT_ID            = 0
ROOT_NAME          = ''
EXIF_DATEFORMAT    = '%Y:%m:%d %H:%M:%S'
LINK_TYPE          = stat.S_IFREG | stat.S_IFLNK

# Current user UID and GID
UID = os.getuid()
GID = os.getgid()

# Startup time
GLOBAL_TIME = long(time.time())

###
# Stats for FUSE implementation

class BaseStat(fuse.Stat):
    """Base Stat class. Sets atime, mtime and ctime to a dummy
    global value (time when application started running)."""
    def __init__(self, *args, **kwargs):
        """Init atime, mtime and ctime."""
        super(BaseStat, self).__init__(*args, **kwargs)
        self.st_atime = self.st_mtime = self.st_ctime = GLOBAL_TIME
        # set current user UID and GID to hierarchy nodes
        self.st_uid = UID
        self.st_gid = GID


class DirStat(BaseStat):
    """Directory stat"""
    def __init__(self, *args, **kwargs):
        super(DirStat, self).__init__(*args, **kwargs)
        self.st_mode = stat.S_IFDIR | 0755
        self.st_nlink = 2


class ImageLinkStat(BaseStat):
    """Link to Image stat"""
    def __init__(self, path, *args, **kwargs):
        super(ImageLinkStat, self).__init__(*args, **kwargs)
        self.st_mode = stat.S_IFREG | stat.S_IFLNK | 0644
        self.st_nlink = 0
        os_stat = os.stat(path)
        self.st_size = os_stat.st_size if os_stat else 0


class NewFileState(BaseStat):
    def __init__(self, *args, **kwargs):
        super(NewFileState, self).__init__(*args, **kwargs)
        self.st_mode = stat.S_IFREG | 0644
        self.st_nlink = 0
        self.st_size = 0


###
# FUSE F-Spot FS
class FSpotFS(fuse.Fuse):
    """F-Spot FUSE filesystem implementation. Just readonly support
    at the moment"""
    def __init__(self, db_path, repeated, *args, **kwargs):
        self.tags, self.reverse_tags, self.creation_pool  = {}, {}, {}
        self.db_path = db_path
        self.repeated = repeated
        self.load_tags()
        super(FSpotFS, self).__init__(*args, **kwargs)

    def load_tags(self):
        """Loads registered tags and internally cache them"""
        tags = self.query(TAGS_SQL)
        self.tags[ROOT_ID] = {'children': {}, 'name': ROOT_NAME,
                              'parent': None}
        self.reverse_tags[ROOT_NAME] = ROOT_ID

        # load tags
        for id, name, category_id in tags:
            self.tags[id] = {'children': {},
                             'name': name,
                             'parent': category_id}
            self.reverse_tags[name] = id

        # setup parent-child relations
        for id, name, category_id in tags:
            if category_id in self.tags:
                self.tags[category_id]['children'][id] = self.tags[id]

    def query(self, sql, *params):
        """Executes SQL query."""
        return query_multiple(self.db_path, sql, *params)

    def query_one(self, sql, *params):
        """Executes SQL query."""
        return query_one(self.db_path, sql, *params)

    def query_exec(self, sql, *params):
        """Executes SQL query."""
        return query_exec(self.db_path, sql, *params)

    def tag_children(self, parent):
        """Return ids of sub-tags of parent tag. Goes deep in the
        tag hierarchy returning second-level, and deeper subtags."""
        # first-level subtags
        result = self.tags[parent]['children'].keys()
        # second-level subtags and deeper
        next_level = set(reduce(lambda l1, l2: l1 + l2,
                                [self.tag_children(tid) for tid in result],
                                []))
        return result + list(next_level)

    def tag_names(self, parent=None, sorted=False):
        """Return tag names for parent or all tag names."""
        tags = self.tags
        try:
            if parent is not None:
                tags = tags[parent]['children']
        except KeyError:
            return []
        values = [tag['name'] for tag in tags.itervalues()]
        if sorted:
            values.sort()
        return values

    def tag_to_id(self, name):
        """Return tag if for tag name or None."""
        try:
            return self.reverse_tags[name]
        except KeyError:
            pass

    def file_names(self, tag=None, sorted=False):
        """Return photo names tagged as 'tag' or all photos if not tag,
        sub-tags are excluded if self.repeated is false."""
        if tag is not None:
            # treat root directory as it doesn't have sub-categories
            if tag != ROOT_ID and not self.repeated:
                children = self.tag_children(tag)
                if children: # get photos for tag avoiding sub-tags
                    files = self.query(prepare_in(TAG_PHOTOS, children),
                                       tag, *children)
                else: # get tag photos for current no-parent tag
                    files = self.query(LEAF_PHOTOS, tag)
            else: # get tag photos not ignoring repeated
                files = self.query(LEAF_PHOTOS, tag)
        else: # get all photos
            files = self.query(ALL_PHOTOS)
        result = [file[0] for file in files]
        if sorted:
            result.sort()
        return result

    def real_path(self, tagid, name):
        """Return real file path in collection."""
        name = self.quote_name(name)
        row = self.query_one(FILE_SQL, tagid, name, name)
        if row is not None:
            base_uri, filename = row
            return unquote(join(base_uri.replace('file://', ''), filename)).\
                          encode('utf-8')

    def is_dir(self, path):
        """Check if path is a directory in f-spot."""
        return path in ('.', '..', '/') or \
               basename(path) in [i.encode('utf-8') for i in self.tag_names()]

    def quote_name(self, name):
        return quote(name, safe='()')

    def is_file(self, path):
        """Check if path is a file in f-spot."""
        # TODO: Improve with querying to database
        return self.quote_name(basename(path)) in \
                    [i.encode('utf-8') for i in self.file_names()]

    def getattr(self, path):
        """Getattr handler."""
        return self._getattr(path) or -errno.ENOENT

    def _getattr(self, path):
        """Hierarchy stats builder, will return None if path is invalid."""
        if self.is_dir(path):
            return DirStat()
        elif self.is_file(path):
            fname = basename(path)
            tag = basename(dirname(path))
            tagid = self.tag_to_id(tag)
            if self.quote_name(fname) in self.file_names(tagid):
                return ImageLinkStat(self.real_path(tagid, fname))
        elif not DISABLE_IMPORT and path in self.creation_pool:
            return NewFileState()
        return None

    def readlink(self, path):
        """Readlink handler."""
        return self.real_path(self.tag_to_id(basename(dirname(path))),
                              basename(path))

    def access(self, path, offset):
        """Check file access."""
        # Access granted by default at the moment, unless the file does
        # not exists
        return -errno.EINVAL if self._getattr(path) is None else 0

    def readdir(self, path, offset):
        """Readdier handler."""
        parent = self.tag_to_id(basename(path))

        yield fuse.Direntry('.')
        yield fuse.Direntry('..')

        for name in self.tag_names(parent, sorted=True):
            yield fuse.Direntry(unquote(name.encode('utf-8')))

        for name in self.file_names(parent, sorted=True):
            yield fuse.Direntry(unquote(name.encode('utf-8')), type=LINK_TYPE)

    def mkdir(self, path, mode):
        """Register new tag or sub-tag and display it as a new directory."""
        name = basename(path)
        parent_id = self.tag_to_id(basename(dirname(path)))
        # register in database
        self.query_exec(NEW_TAG_SQL, name.encode('utf-8'), parent_id)
        self.load_tags() # reload cache
        return 0

    def rmdir(self, path):
        """Removes a directory, unregister the tags and the photos tagged
        by it. Only subdirectories without sub-directories (tags without
        sub-tags).
        """
        tagid = self.tag_to_id(basename(path))
        children = self.tag_children(tagid)

        if children: # tag with sub-tags removal not allowed
            return -errno.EINVAL
        self.query_exec(UNTAG_PHOTOS_SQL, tagid) # untag photos
        self.query_exec(TAG_REMOVE_SQL, tagid) # remove tag
        self.load_tags() # reload cache

    def rename( self, old_path, new_path):
        """Rename tags. No images rename allowed."""
        old_tag, new_tag = basename(old_path), basename(new_path)

        tagid = self.tag_to_id(old_tag)
        if not tagid: # original tag does not exist
            return -errno.ENOENT
        if self.tag_to_id(new_tag): # new name already exists
            return -errno.EINVAL
        self.query_exec(TAG_RENAME_SQL, new_tag, tagid) # rename tag
        self.load_tags() # reload cache

    def create(self, path, flags, mode):
        """Create file handler."""
        if DISABLE_IMPORT: # no support if import is disabled
            return -errno.ENOSYS

        if path not in self.creation_pool:
            # Write to a temporary file that will be moved later
            # this is because we don't know the file metadata until
            # it's written
            fd, tmp_path = tempfile.mkstemp()
            # Register path in our creation pool, this way avoid
            # failures to OS on getattr
            self.creation_pool[path] = (fd, tmp_path)
        return self.creation_pool[path]

    def write(self, path, buff, offs, data=None):
        """Write file handler."""
        if DISABLE_IMPORT: # no support if import is disabled
            return -errno.ENOSYS
        if path not in self.creation_pool: # file was not created
            return -errno.ENOENT
        fd, tmp_path = (data or self.creation_pool[path])
        return os.write(fd, buff) # write to temp file

    def release(self, path, flags, data=None):
        """Release file handler.

        Will move temporary written file to collection structure and tag
        properly.
        """
        if DISABLE_IMPORT: # no support if import is disabled
            return -errno.ENOSYS
        if path not in self.creation_pool: # file was not created
            return -errno.ENOENT

        fd, tmp_path = (data or self.creation_pool[path])
        os.close(fd) # close temporary file

        def cleanup(result=-errno.EINVAL):
            """Clanups files."""
            self.creation_pool.pop(path)
            if isfile(tmp_path):
                os.remove(tmp_path)
            return result

        tagid = self.tag_to_id(basename(dirname(path)))
        if tagid is None: # destination tag does not exists
            return cleanup()

        try: # open image on temporary location
            img = Image.open(tmp_path)
        except IOError:
            return cleanup()

        try: # try to get date from exif
            exif_date = img._getexif()[DATETIME_ID]
            date = datetime.strptime(exif_date, EXIF_DATEFORMAT)
        except (KeyError, TypeError, ValueError): # use today date in error
            date = datetime.now()

        # build base path /collection-root/<year>/<month>/<day>/
        base = join(COLLECTION_ROOT, str(date.year),
                    '%02d' % date.month, '%02d' % date.day)
        if not isdir(base): # build collection directory
            try:
                os.makedirs(base)
            except OSError:
                return cleanup()

        name = basename(path)
        base_uri = self.base_uri(base)

        # ovewrite is not supported, lets assume they are the same
        # files and retag it
        dest = join(base, name)
        if not isfile(dest):
            try:
                shutil.move(tmp_path, dest)
            except OSError:
                return cleanup()

            # register on database
            self.query_exec(ADD_IMAGE_SQL, int(time.time()), base_uri, name)
            image_id = self.query_one(IMAGE_ID_SQL, base_uri, name)[0]
            self.query_exec(ADD_VERSION_SQL, image_id, base_uri, name)
        else:
            image_id = self.query_one(IMAGE_ID_SQL, base_uri, name)[0]

        if self.real_path(tagid, name) is None: # tag it, if not already tagged
            self.query_exec(TAG_IMAGE, image_id, tagid)

        return cleanup(result=0)

    def chmod(self, path, *args, **kwargs):
        """Chmod support (called when moving images)"""
        return 0

    def chmown(self, path, *args, **kwargs):
        """Chown support (called when moving images)"""
        return 0

    def symlink(self, source, target):
        """Linking or symbolic link copying handler.

        If linking is inside collection
            source: is absolute path to image in collection directory
            target: is relative path in virtual filesystem.

        If linking is from outside is not supported.
        """
        # TODO: Support linking from outside using create
        if source.startswith(COLLECTION_ROOT):
            name = basename(source)
            tag_id = self.tag_to_id(basename(dirname(target)))
            image_id = self.query_one(IMAGE_ID_SQL,
                                      self.base_uri(dirname(source)),
                                      name)[0]
            self.query_exec(TAG_IMAGE, image_id, tag_id)
            return 0
        else:
            return -errno.ENOSYS

    def base_uri(self, path):
        """Builds baseuri for path.

        Path needs to be absolute or will be converted.
        """
        if not path.startswith('/'):
            path = '/' + path
        if not path.endswith('/'):
            path = path + '/'
        return 'file://' + path


def run():
    """Parse commandline options and run server"""
    def param_error(msg, parser):
        """Print message followed by options usage and exit."""
        print >>sys.stderr, msg, '\n'
        parser.print_help()
        sys.exit(1)

    parser = OptionParser(usage='%prog [options]', description=DESCRIPTION)
    parser.add_option('-d', '--fsdb', action='store', type='string',
                      dest='fsdb', default='',
                      help='Path to F-Spot sqlite database.')
    parser.add_option('-m', '--mount', action='store', type='string',
                      dest='mountpoint', default=DEFAULT_MOUNTPOINT,
                      help='Mountpoint path (default %s)' % DEFAULT_MOUNTPOINT)
    parser.add_option('-r', '--repeated', action='store_true', dest='repeated',
                      help='Show re-tagged images in the same family tree' \
                           ' (default False)')
    parser.add_option('-v', '--dbversion', action='store', type='string',
                      dest='dbversion', default=FSPOT_DB_VERSION,
                      help='F-Spot database schema version to use' \
                           ' (default v%s)' % FSPOT_DB_VERSION)
    parser.add_option('-l', '--log', action='store_true', dest='log',
                      help='Shows FUSE log (default False)')
    try:
        opts, args = parser.parse_args()
    except OptionError, e: # Invalid option
        param_error(str(e), parser)

    # override F-Spot database path
    if opts.fsdb:
        fspot_db = opts.fsdb
        if not isabs(fspot_db):
            fspot_db = join(os.environ['HOME'], fspot_db)
    elif 'XDG_CONFIG_HOME' in os.environ:
        # build F-Spot database path with XDG enviroment values
        fspot_db = join(os.environ['XDG_CONFIG_HOME'], FSPOT_DB_FILE)
    else:
        # build F-Spot database HOME enviroment value
        fspot_db = join(os.environ['HOME'], '.config', FSPOT_DB_FILE)

    if not isfile(fspot_db):
        param_error('File "%s" not found' % fspot_db, parser)

    # check database schema compatibility
    try:
        float(opts.dbversion) # check if dbversion is float
        version = opts.dbversion.split('.')
        fspot_version = query_one(fspot_db, DB_VERSION_SQL)[0].split('.')
        assert len(fspot_version) >= len(version) and \
               all(x == y for x, y in zip(fspot_version, version))
    except ValueError:
        param_error('Incorrect version format "%s"' % opts.dbversion, parser)
    except AssertionError:
        param_error('Versions mismatch, current database version is "%s",' \
                    ' passed value was "%s"' % ('.'.join(fspot_version),
                                                opts.dbversion),
                    parser)

    mountpoint = opts.mountpoint
    if not exists(mountpoint) or not isdir(mountpoint):
        param_error('Invalid mountpoint "%s"' % mountpoint, parser)

    fuse.fuse_python_api = (0, 2)
    args = fuse.FuseArgs()
    args.mountpoint = mountpoint
    if opts.log:
        args.add('debug')
    FSpotFS(fspot_db, opts.repeated, fuse_args=args).main() # run server


if __name__ == '__main__':
    run()
