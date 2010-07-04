# -*- coding: utf-8 -*-
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
from os import path
from urllib import quote, unquote
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relation, backref, sessionmaker


# Declarative approach
Base = declarative_base()


class _Manager(object):
    """Simpler management methods."""
    def _operation(self, op_name):
        """Calls session operation named @op_name if it exists and is
        callable. Detaches from current object session if it's not
        current session. Operations must accept instance as first argument.
        """
        session = get_session()
        op = getattr(session, op_name, None)
        if not op or not hasattr(op, '__call__'):
            raise AttributeError, \
                    'Not callable object returned for operation %s.' % op_name
        if self._sa_instance_state.session_id != session.hash_key:
            # need to detach before some operations, why?
            self._sa_instance_state.detach()
        op(self)
        session.commit()
        session.flush()
        return self

    def add(self):
        """Registers item in session, transaction is commited inmediattly."""
        self._operation('add')

    def delete(self):
        """Delete item in session, transaction is commit inmediattly."""
        self._operation('delete')

    def update(self):
        """Delete item in session, transaction is commit inmediattly."""
        self._operation('update')

    @classmethod
    def all(klass):
        """Query all entries for current object mapper."""
        return get_session().query(klass).all()

    @classmethod
    def filter(klass, **kwargs):
        """Query all entries for current object mapper."""
        return get_session().query(klass).filter_by(**kwargs)

    @classmethod
    def get(klass, ident):
        return get_session().query(klass).get(ident)

    @classmethod
    def join(klass, *args):
        return get_session().query(klass).join(*args)

    @classmethod
    def order_by(klass, *args):
        return get_session().query(klass).order_by(*args)


def photo_path(obj):
    """Return path striping file:// prefix."""
    return unquote(path.join(obj.base_uri.replace('file://', ''),
                             obj.filename))


class Photo(Base, _Manager):
    """photos table mapper."""
    __tablename__ = 'photos'

    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(Integer)
    base_uri = Column(String)
    filename = Column(String)
    description = Column(String)
    roll_id = Column(Integer)
    default_version_id = Column(Integer)
    rating = Column(Integer)
    md5_sum = Column(String)

    def __init__(self, *args, **kwargs):
        super(Photo, self).__init__(*args, **kwargs)
        self.description = ''
        self.roll_id = 1
        self.rating = 0
        self.md5_sum = ''

    @property
    def path(self):
        """Return file absolute path in collection."""
        return photo_path(self)

    @classmethod
    def all_photos(klass):
        return update_with_version(Photo.with_version().order_by(Photo.filename).all())

    @classmethod
    def with_version(klass):
        """Return photo an photo_version data."""
        return get_session().query(Photo, PhotoVersion)\
                            .join((PhotoVersion,
                                  ((PhotoVersion.version_id == Photo.default_version_id) &
                                   (PhotoVersion.photo_id == Photo.id))))

    @classmethod
    def by_tag(klass, tagid):
        """Return photos tagged by @tagname. Joins with photo_version table."""
        return Photo.with_version().join((PhotoTag, (PhotoTag.tag_id == tagid) &
                                         (PhotoTag.photo_id == Photo.id)))

    def update_from_version(self, version):
        """Update current photo base_uri and filename from @version."""
        self._base_uri = self.base_uri
        self._filename = self.filename
        if version:
            self.base_uri = version.base_uri or self.base_uri
            self.filename = version.filename or self.filename

    def __repr__(self):
        """repr string"""
        return '<Photo %s>' % self.filename

class PhotoVersion(Base, _Manager):
    """photo_versions table mapper."""
    __tablename__ = 'photo_versions'

    photo_id = Column(Integer, ForeignKey('photos.id'), primary_key=True)
    version_id = Column(Integer, primary_key=True)
    name = Column(String)
    base_uri = Column(String)
    filename = Column(String)

    photo = relation(Photo, backref=backref('versions'))

    @property
    def path(self):
        """Return file absolute path in collection."""
        return photo_path(self)

    def __repr__(self):
        """repr string"""
        return '<PhotoVersion %s (%s)>' % (self.name, self.version_id)


class Tag(Base, _Manager):
    """tags table mapper."""
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    category_id = Column(Integer, ForeignKey('tags.id'))

    subtags = relation('Tag', backref=backref('parent', remote_side='Tag.id'))

    @classmethod
    def untagged_photos(klass):
        photos = Photo.with_version()\
                      .outerjoin((PhotoTag, PhotoTag.photo_id == Photo.id))\
                      .filter(PhotoTag.tag_id == None)
        return update_with_version(photos)

    def own_photos(self):
        subtag_ids = [subtag.id for subtag in self.subtags]
        pt_alias = PhotoTag.__table__.alias()

        photos = Photo.with_version()\
                      .join((PhotoTag, PhotoTag.photo_id == Photo.id))\
                      .filter(PhotoTag.tag_id == self.id)\
                      .outerjoin((pt_alias,
                                 (pt_alias.c.photo_id == PhotoTag.photo_id) &
                                 (pt_alias.c.tag_id != PhotoTag.tag_id)))\
                      .filter((pt_alias.c.tag_id == None) |
                              (~pt_alias.c.tag_id.in_(subtag_ids)))
        return update_with_version(photos)

    def get_file(self, name):
        """Returns photo and photo_version for file @name tagged by @tag."""
        result = Photo.by_tag(self.id)\
                      .filter(Photo.filename == quote(name, safe='()'))\
                      .first()
        if result:
            photo, pversion = result
            photo.update_from_version(pversion)
            return photo

    def __repr__(self):
        """repr string"""
        return '<Tag %s>' % self.name


class PhotoTag(Base, _Manager):
    """photo_tags table mapper."""
    __tablename__ = 'photo_tags'

    photo_id = Column(Integer, ForeignKey('photos.id'), primary_key=True)
    tag_id = Column(Integer, ForeignKey('tags.id'), primary_key=True)

    photo = relation(Photo, backref=backref('tags'))
    tag = relation(Tag, backref=backref('photos'))

    def __repr__(self):
        """repr string"""
        return '<PhotoTag %s - %s>' % (self.tag_id, self.photo_id)


class Meta(Base, _Manager):
    """meta table mapper."""
    __tablename__ = 'meta'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    data = Column(String)

    def __repr__(self):
        """repr string"""
        return '<Meta %s - %s>' % (self.name, self.data[:15])


# global engine and session
_engine, _session = None, None


def init_session(db_path, echo=False):
    """Initializes engine and sessionmaker."""
    global _engine, _session
    _engine = create_engine(db_path, echo=echo)
    _session = sessionmaker(bind=_engine)


def get_session():
    """Returns a new session."""
    global _session
    assert _session != None
    return _session()


def get_db_version():
    """Return F-Spot database schema version."""
    return Meta.filter(name='F-Spot Database Version').first().data
        

def update_with_version(photos):
    result = []
    for photo, pversion in photos:
        photo.update_from_version(pversion)
        result.append(photo)
    return result
