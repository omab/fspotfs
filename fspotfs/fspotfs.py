#!/usr/bin/python
# -*- coding: utf-8 -*-
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
except (ImportError, KeyError, ValueError):
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
    """New file stat"""
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
        self.tags[ROOT_ID] = {'children': {}, 'name': ROOT_NAME,
                              'parent': None}
        self.reverse_tags[ROOT_NAME] = ROOT_ID

        tags = list(Tag.all())
        # load tags
        for tag in tags:
            self.tags[tag.id] = {'children': {},
                                 'name': tag.name,
                                 'parent': tag.category_id}
            self.reverse_tags[tag.name] = tag.id

        # setup parent-child relations
        for tag in tags:
            if tag.category_id in self.tags:
                self.tags[tag.category_id]['children'][tag.id] = self.tags[tag.id]

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

    def file_names(self, tag_id=None):
        """Return photo names tagged as @tag_id or all photos if not tag,
        sub-tags are excluded if self.repeated is false."""
        photos = []

        if tag_id is not None:
            if tag_id == ROOT_ID:
                photos = Tag.untagged_photos()
            elif not self.repeated:
                tag = Tag.get(tag_id)
                if tag:
                    photos = tag.own_photos()
        else: # get all photos
            photos = Photo.all_photos()

        return [photo.filename for photo in photos]

    def real_path(self, tag_id, name):
        """Return real file path in collection."""
        photo = None

        if tag_id == ROOT_ID:
            result = Photo.with_version().filter(Photo.filename == name).first()
            if result:
                photo, version = result
                photo.update_from_version(version)
        else:
            tag = Tag.get(tag_id)
            if tag:
                photo = tag.get_file(name)
        return photo.path.encode('utf-8') if photo else None

    def is_dir(self, path):
        """Check if path is a directory in f-spot."""
        return path in ('.', '..', '/') or \
               basename(path) in [i.encode('utf-8') for i in self.tag_names()]

    def quote_name(self, name):
        return quote(name, safe='()')

    def getattr(self, path):
        """Getattr handler."""
        return self._getattr(path) or -errno.ENOENT

    def _getattr(self, path):
        """Hierarchy stats builder, will return None if path is invalid."""
        if self.is_dir(path):
            return DirStat()
        elif Photo.filter(filename=self.quote_name(basename(path))).first():
            fname = basename(path)
            tag = basename(dirname(path))
            tag_id = self.tag_to_id(tag)
            if self.quote_name(fname) in self.file_names(tag_id):
                return ImageLinkStat(self.real_path(tag_id, fname))
        elif not DISABLE_IMPORT and path in self.creation_pool:
            return NewFileState()
        else:
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

        for name in self.file_names(parent):
            yield fuse.Direntry(unquote(name.encode('utf-8')), type=LINK_TYPE)

    def mkdir(self, path, mode):
        """Register new tag or sub-tag and display it as a new directory."""
        name = basename(path)
        parent_id = self.tag_to_id(basename(dirname(path)))
        if parent_id is not None:
            # register in database
            tag = Tag(id=None, name=name.encode('utf-8'), category_id=parent_id)
            tag.add()
            # register in cache
            self.tags[tag.id] = {'children': {}, 'name': tag.name,
                                 'parent': tag.category_id}
            self.reverse_tags[tag.name] = tag.id
            self.tags[parent_id]['children'][tag.id] = self.tags[tag.id]
            return 0
        else:
            return -errno.EINVAL

    def unlink(self, path):
        """Unlink files. It's interpreted as unttagging, not remove."""
        tag_id = self.tag_to_id(basename(dirname(path)))
        if tag_id is None:
            return -errno.EINVAL
        photo = Photo.filter(filename=basename(path)).first()
        if photo is None:
            return -errno.ENOENT
        PhotoTag.filter(tag_id=tag_id, photo_id=photo.id).first().delete()
        return 0

    def rmdir(self, path):
        """Removes a directory, unregister the tags and the photos tagged
        by it. Only subdirectories without sub-directories (tags without
        sub-tags).
        """
        tag = Tag.get(self.tag_to_id(basename(path)))
        if tag:
            # update cache
            self.tags[self.tags[tag.id]['parent']]['children'].pop(tag.id, None)
            self.tags.pop(tag.id)
            self.reverse_tags.pop(tag.name)
            # delete from db
            tag.delete()
            return 0
        else:
            return -errno.ENOENT

    def rename(self, old_path, new_path):
        """Renaming handler.

        Not allowed to:
            * Rename photos
            * Rename into other directory (move)
        """
        old_tag, new_tag = basename(old_path), basename(new_path)
        if new_tag in self.reverse_tags: # new name already exists
            return -errno.EINVAL

        tag = Tag.get(self.tag_to_id(old_tag))
        if tag:
            tag.name = new_tag
            tag.update()
            # update cache
            self.reverse_tags.pop(old_tag)
            self.tags[tag.id]['name'] = new_tag
            self.reverse_tags[new_tag] = tag.id
        else: # original tag does not exist
            return -errno.ENOENT


    def create(self, path, flags, mode):
        """Create file handler."""
        if DISABLE_IMPORT: # no support if import is disabled
            return -errno.ENOSYS

        if path not in self.creation_pool:
            # Register path in our creation pool, this way avoid
            # failures to OS on getattr
            self.creation_pool[path] = PhotoFile(self, path, flags, mode)
        return self.creation_pool[path]

    def write(self, path, buff, offs, data=None):
        """Write file handler."""
        if DISABLE_IMPORT: # no support if import is disabled
            return -errno.ENOSYS
        if path not in self.creation_pool: # file was not created
            return -errno.ENOENT
        photo = (data or self.creation_pool[path])
        return photo.write(buff, offs)

    def flush(self, path, data):
        """Flush buffers contents."""
        return data.flush()

    def release(self, path, flags, data=None):
        """Release file handler.

        Will move temporary written file to collection structure and tag
        properly.
        """
        if DISABLE_IMPORT: # no support if import is disabled
            return -errno.ENOSYS
        if path not in self.creation_pool: # file was not created
            return -errno.ENOENT

        file = self.creation_pool.pop(path, None) or data
        if not file:
            return -errno.EINVAL

        tag_id = self.tag_to_id(basename(dirname(file.path)))
        if tag_id is None: # destination tag does not exists
            file.clean()
            return -errno.EINVAL

        try: # open image on temporary location
            img = Image.open(file.tmp_path)
        except IOError:
            file.clean()
            return -errno.EINVAL

        try: # try to get date from exif
            exif_date = img._getexif()[DATETIME_ID]
            date = datetime.strptime(exif_date, EXIF_DATEFORMAT)
        except (AttributeError, KeyError, TypeError, ValueError): # use today date in error
            date = datetime.now()

        # build base path /collection-root/<year>/<month>/<day>/
        base = join(COLLECTION_ROOT, str(date.year),
                    '%02d' % date.month, '%02d' % date.day)
        if not isdir(base): # build collection directory
            try:
                os.makedirs(base)
            except OSError:
                file.clean()
                return -error.EINVAL

        name = basename(file.path)
        base_uri = self.base_uri(base)

        # ovewrite is not supported, lets assume they are the same
        # files and retag it
        dest = join(base, name)
        if not isfile(dest):
            try:
                shutil.move(file.tmp_path, dest)
            except OSError:
                file.clean()
                return -error.EINVAL

            # register on database
            photo = Photo(id=None, time=int(time.time()), base_uri=base_uri,
                          default_version_id=1, filename=name)
            photo.add()
            pv = PhotoVersion(photo_id=photo.id, version_id=1, name='Original',
                              filename=photo.filename, base_uri=photo.base_uri)
            pv.add()
        else:
            photo = Photo.filter(base_uri=base_uri, filename=name).first()

        if photo and tag_id != ROOT_ID and \
           not PhotoTag.filter(tag_id=tag_id, photo_id=photo.id).first():
            PhotoTag(tag_id=tag_id, photo_id=photo.id).add()

        file.clean()
        return 0

    def chmod(self, path, *args, **kwargs):
        """Chmod support (called when moving images)"""
        return 0

    def chmown(self, path, *args, **kwargs):
        """Chown support (called when moving images)"""
        return 0

    def symlink(self, source, target):
        """Linking or symbolic link copying handler.

        @source: is path to image
        @target: is path in virtual filesystem

        Linking from outside is not supported.
        """
        name = basename(source)
        photo = Photo.filter(filename=name).first()
        if photo is not None:
            tag_id = self.tag_to_id(basename(dirname(target)))
            pt = PhotoTag.filter(tag_id=tag_id, photo_id=photo.id).first()
            if pt is None:
                PhotoTag(tag_id=tag_id, photo_id=photo.id).add()
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

class PhotoFile(object):
    """New Photo file object"""
    def __init__(self, fspotfs, path, flags, mode):
        """Init method

            @fspotfs: fspotfs instance
            @path: destination path
            @flags: file flags
            @mode: opening mode
        """
        self.fspotfs = fspotfs
        self.path = path
        self.tmp_path = tempfile.mktemp()
        self.file = open(self.tmp_path, 'w+')

    def write(self, buff, offset):
        """Write method"""
        self.file.seek(offset)
        self.file.write(buff) # write to temp file
        return len(buff)

    def clean(self):
        """Clean, will close and remove temporary files"""
        self.file.close()
        return os.remove(self.tmp_path)

    def flush(self):
        """Flush file buffer"""
        return self.file.flush()


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

    # initializes database session
    init_session('sqlite:///' + fspot_db, True)

    # check database schema compatibility
    try:
        version = opts.dbversion.split('.')
        fspot_version = get_db_version().split('.')
        assert len(fspot_version) >= len(version) and \
               all(x == y for x, y in zip(fspot_version, version))
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
