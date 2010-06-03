#!/usr/bin/python
"""
Copyright (C) 2009  Matias Aguirre <matiasaguirre@gmail.com>

This program is free software: you can redistribute it and/or modify
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

import os, sys, stat, errno, fuse, time, sqlite3
from urllib import unquote
from optparse import OptionParser, OptionError
from os.path import basename, dirname, join, isfile, isabs


fuse.fuse_python_api = (0, 2)

DESCRIPTION        = 'F-Spot FUSE Filesystem'
FSPOT_DB_FILE      = 'f-spot/photos.db'
FSPOT_DB_VERSION   = '17' # database version supported
DEFAULT_MOUNTPOINT = join(os.environ['HOME'], '.photos')
ROOT_ID            = 0
ROOT_NAME          = ''

# Current user UID and GID
UID = os.getuid()
GID = os.getgid()

###
# SQL sentences

# get F-Spot database version
DB_VERSION_SQL = """SELECT data FROM meta
                    WHERE name = "F-Spot Database Version"
                    LIMIT 1"""

# get real path parts for photo in tag
FILE_SQL = """SELECT replace(p.base_uri, 'file://', ''),
                     p.filename
              FROM photo_tags pt
              LEFT JOIN photos p
                ON p.id = pt.photo_id
              WHERE pt.tag_id = ? AND p.filename = ?
              LIMIT 1"""

# get photos for tag excluding photos in sub-tags
TAG_PHOTOS = """SELECT p.filename
                FROM photo_tags pt
                LEFT JOIN photos p
                    ON p.id = pt.photo_id
                WHERE pt.tag_id = ? AND
                      pt.photo_id NOT IN (SELECT DISTINCT pt2.photo_id
                                          FROM photo_tags pt2
                                          WHERE pt2.tag_id IN %(in_items)s)"""

# photos for leaf tag
LEAF_PHOTOS = """SELECT p.filename
                 FROM photo_tags pt
                 LEFT JOIN photos p
                    ON p.id = pt.photo_id
                 WHERE pt.tag_id = ?"""

# All photos
ALL_PHOTOS = 'SELECT filename FROM photos'

# return id for tag name
TAG_ID = 'SELECT id FROM tags WHERE name = ? LIMIT 1'

# tag names
TAG_NAMES = 'SELECT name FROM tags'

# sub-tag names
SUBTAG_NAMES = 'SELECT name FROM tags WHERE category_id = ?'
        
# sub-tag ids
SUBTAG_IDS = 'SELECT id FROM tags WHERE category_id = ?'

# tags
TAGS_SQL = 'SELECT id, name, category_id FROM tags'

# new tag sql
NEW_TAG_SQL = """INSERT INTO tags
                    (name, category_id, is_category, sort_priority, icon)
                 VALUES
                    (?, ?, 1, 0, NULL)"""

# untag photos sql
UNTAG_PHOTOS_SQL = 'DELETE FROM photo_tags WHERE tag_id = ?'

# remove tag sql
TAG_REMOVE_SQL = 'DELETE FROM tags WHERE id = ?'

# rename tag sql
TAG_RENAME_SQL = 'UPDATE tags SET name = ? WHERE id = ?'

# Startup time
GLOBAL_TIME = int(time.time())

def with_cursor(fn):
    """Wraps a function that needs a cursor."""
    def wrapper(db_path, sql, *params):
        conn = sqlite3.connect(db_path)
        if conn:
            cur = conn.cursor()
            cur.execute('PRAGMA temp_store = MEMORY');
            cur.execute('PRAGMA synchronous = OFF');
            result = fn(conn, cur, db_path, sql, *params)
            cur.close()
            return result
    return wrapper


@with_cursor
def query_multiple(conn, cur, db_path, sql, *params):
    """Executes SQL query."""
    cur.execute(sql, params)
    return cur.fetchall()

@with_cursor
def query_one(conn, cur, db_path, sql, *params):
    """Execute sql and return just one row."""
    cur.execute(sql, params)
    return cur.fetchone()

@with_cursor
def query_exec(conn, cur, db_path, sql, *params):
    """Execute sql."""
    cur.execute(sql, params)
    conn.commit()

def prepare_in(sql, items):
    """Prepares an sql statement with an IN clause.
    The sql *must* have an 'in_items' placeholder."""
    return sql % {'in_items': '(' + ', '.join('?' for item in items) + ')'}

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


###
# FUSE F-Spot FS
class FSpotFS(fuse.Fuse):
    """F-Spot FUSE filesystem implementation. Just readonly support
    at the moment"""
    def __init__(self, db_path, repeated, *args, **kwargs):
        self.tags, self.reverse_tags = {}, {}
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

    def tag_names(self, parent=None):
        """Return tag names for parent or all tag names."""
        tags = self.tags
        try:
            if parent is not None:
                tags = tags[parent]['children']
        except KeyError:
            return []
        return [tag['name'] for tag in tags.itervalues()]

    def tag_to_id(self, name):
        """Return tag if for tag name or None."""
        try:
            return self.reverse_tags[name]
        except KeyError:
            pass

    def file_names(self, tag=None):
        """Return photo names tagged as 'tag' or all photos if not tag,
        sub-tags are excluded if self.repeated is false."""
        if tag is not None:
            if not self.repeated:
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
        return [file[0] for file in files]

    def link_path(self, tag, name):
        """Return path to filename."""
        tagid = self.tag_to_id(tag)
        if tagid is not None:
            row = self.query_one(FILE_SQL, tagid, name)
            try:
                uri, filename = row
                return unquote(join(uri, filename)).encode('utf-8')
            except (TypeError, IndexError):
                pass
        return ''

    def is_dir(self, path):
        """Check if path is a directory in f-spot."""
        return path in ('.', '..', '/') or \
               basename(path) in [i.encode('utf-8') for i in self.tag_names()]

    def is_file(self, path):
        """Check if path is a file in f-spot."""
        # TODO: Improve with querying to database
        return basename(path) in [i.encode('utf-8') for i in self.file_names()]

    def getattr(self, path):
        """Getattr handler."""
        if self.is_dir(path):
            return DirStat()
        elif self.is_file(path):
            fname = basename(path)
            tag = basename(dirname(path))
            if fname in self.file_names(self.tag_to_id(tag)):
                return ImageLinkStat(self.link_path(tag, fname))
        return -errno.ENOENT

    def readlink(self, path):
        """Readlink handler."""
        return self.link_path(basename(dirname(path)), basename(path))

    def access(self, path, offset):
        """Check file access."""
        # Access granted by default at the moment, unless the file does
        # not exists
        if self.is_dir(path) or \
           self.is_file(path) and \
           basename(path) in \
           self.file_names(self.tag_to_id(basename(dirname(path)))):
            return 0
        return -errno.EINVAL

    def readdir(self, path, offset):
        """Readdier handler."""
        # Yields items returned by _readdir method
        for item in self._readdir(path, offset):
            yield item

    def _readdir(self, path, offset):
        parent = self.tag_to_id(basename(path))

        dirs = [fuse.Direntry(r.encode('utf-8'))
                    for r in self.tag_names(parent)]
        dirs.sort(key=lambda x: x.name)

        type = stat.S_IFREG | stat.S_IFLNK
        files = [fuse.Direntry(i.encode('utf-8'), type=type)
                    for i in self.file_names(parent)]
        files.sort(key=lambda x: x.name)

        return [fuse.Direntry('.'), fuse.Direntry('..')] + dirs + files

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
        old_tag = basename(old_path)
        new_tag = basename(new_path)
        tagid = self.tag_to_id(old_tag)
        if not tagid: # original tag does not exist
            return -errno.ENOENT
        if self.tag_to_id(new_tag): # new name already exists
            return -errno.EINVAL
        self.query_exec(TAG_RENAME_SQL, new_tag, tagid) # rename tag
        self.load_tags() # reload cache


def run():
    """Parse commandline options and run server"""
    def param_error(msg, parser):
        """Print message followed by options usage and exit."""
        print >>sys.stderr, msg
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
    except ValueError, e:
        param_error('Incorrect version format "%s"' % opts.dbversion, parser)
    except AssertionError, e:
        param_error('Versions mismatch, current database version is "%s",' \
                    ' passed value was "%s"' % ('.'.join(fspot_version),
                                                opts.dbversion),
                    parser)

    args = fuse.FuseArgs()
    args.mountpoint = opts.mountpoint
    if opts.log:
        args.add('debug')
    FSpotFS(fspot_db, opts.repeated, fuse_args=args).main() # run server


if __name__ == '__main__':
    run()
