"""
Copyright (C) 2010  Matias Aguirre <matiasaguirre@gmail.com>

This file is part of F-SpotFS.

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
import sqlite3

# F-Spot Database access

###
# SQL sentences

# get F-Spot database version
DB_VERSION_SQL = """SELECT data FROM meta
                    WHERE name = "F-Spot Database Version"
                    LIMIT 1"""

# get real path parts for photo in tag
FILE_SQL = """SELECT ifnull(v.base_uri, p.base_uri), ifnull(v.filename, p.filename)
              FROM photo_tags pt
              LEFT JOIN photos p
                ON p.id = pt.photo_id
              LEFT JOIN photo_versions v
                ON v.photo_id = p.id AND
                   v.version_id = p.default_version_id
              WHERE pt.tag_id = ? AND (v.filename = ? or p.filename = ?)
              LIMIT 1"""

# get photos for tag excluding photos in sub-tags
TAG_PHOTOS = """SELECT ifnull(v.filename, p.filename)
                FROM photo_tags pt
                LEFT JOIN photos p
                    ON p.id = pt.photo_id
                LEFT JOIN photo_versions v
                    ON v.photo_id = p.id AND
                       v.version_id = p.default_version_id
                WHERE pt.tag_id = ? AND
                      pt.photo_id NOT IN (SELECT DISTINCT pt2.photo_id
                                          FROM photo_tags pt2
                                          WHERE pt2.tag_id IN %(in_items)s)"""

# photos for leaf tag
LEAF_PHOTOS = """SELECT ifnull(v.filename, p.filename)
                 FROM photo_tags pt
                 LEFT JOIN photos p
                    ON p.id = pt.photo_id
                 LEFT JOIN photo_versions v
                    ON v.photo_id = p.id AND
                       v.version_id = p.default_version_id
                 WHERE pt.tag_id = ?"""

# All photos
ALL_PHOTOS = """SELECT ifnull(v.filename, p.filename)
                FROM photos p
                LEFT JOIN photo_versions v
                    ON v.photo_id = p.id AND
                       v.version_id = p.default_version_id"""

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

# register image sql
ADD_IMAGE_SQL = """INSERT INTO photos (time, base_uri, filename, description,
                                       roll_id, default_version_id, rating,
                                       md5_sum)
                   VALUES (?, ?, ?, '', 1, 1, 0, NULL)"""

# select image sql
IMAGE_ID_SQL = 'SELECT id FROM photos WHERE base_uri = ? AND filename = ?'

# photo version insert
ADD_VERSION_SQL = """INSERT INTO photo_versions (photo_id, version_id, name,
                                                 base_uri, filename, md5_sum,
                                                 protected)
                     VALUES (?, 1, 'Original', ?, ?, NULL, 1)"""

# tag image sql
TAG_IMAGE = 'INSERT INTO photo_tags (photo_id, tag_id) VALUES (?, ?)'


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


