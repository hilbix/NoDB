#!/usr/bin/env python3
#
# This is not a database.
#
# It is just a straight forward JSON object storage
# with an ORM like binding.
#
# no = hashfinder.nodb.NoDB()
# db = no.open('file')
# t  = b.main
# o1 = t['object1']
# o2 = t.object2
# o1['item']=1
# o2.item=1
# o1['w'].t['f']=2
# o2.w.t.f=2
# o1 == o2  # True
# o1 is o2  # False
#
# Flags all default to False:
#
# create=True   # Database is created on the fly if not exist
# unsafe=True   # Database is not flushed on write
# manual=True   # Database flushing is not implicite
# force=True	# Unconditionally do things
#
# nodb = NoDB(flags)
# nodb.open(file, flags)	  # open another database
# nodb.flush(db, flags)	   # manually flush, unconditionally re-write database if force==True
# nodb.flush(None, flags)	 # flush all known databases
# nodb.close(db, flags)	   # close the handle
# nodb.close(None, flags)	 # close all known databases
# nodb.destroy(db, flags)	 # destroy database if empty
# nodb.destroy(None, flags)   # destroy all known databases
# nodb.destroy(file, flags)   # drop database if empty.  force=True to destroy even if nonempty
#
# This Works is placed under the terms of the Copyright Less License,
# see file COPYRIGHT.CLL.  USE AT OWN RISK, ABSOLUTELY NO WARRANTY.

import os
import sys
import copy
import json
import fcntl
import weakref

DEBUG=False

def LOG(*args):
	if DEBUG:
		print(*args, file=sys.stderr)
		sys.stderr.flush()

def debugstr(*args):
	return '['+' '.join([str(a) for a in args])+']'

def resolve(path, name):
	return os.path.realpath(os.path.join(path, name))

class LockedFile:
	def __init__(self, name):
		self.locks	= 0
		self.name	= name
		self.fd		= None

	def file(self):
		return self.fd

	def lock(self):
		if not self.locks:
			self.fd	= os.open(self.name, os.O_RDWR)
			fcntl.lockf(self.fd, fcntl.LOCK_EX)
		self.locks  += 1

	def unlock(self, force=False):
		if not self.locks:  return self
		self.locks	-= 1
		if force:   self.locks	= 0
		if not self.locks:
			os.close(self.fd)
			self.fd	= None

	def __enter__(self):
		return self.lock()

	def __exit__(self, typ, val, tb):
		self.unlock()

	def __del__(self):
		self.unlock(True)

class Flags:
	create	= False
	unsafe	= False
	manual	= False
	force	= False

	def __init__(self, flags):
		self.__set(flags)

	def __set(self, flags):
		if not flags:
			return
		if isinstance(flags, Flags):
			for a in dir(flags):
				if a[0]=='_': continue
				assert hasattr(self, a), 'unknown flag: '+a
				setattr(self, a, getattr(flags, a))
			return self

		for a in flags:
			assert hasattr(self, a), 'unknown flag: '+a
			setattr(self, a, flags[a])
		return self

	def __call__(self, flags):
		return flags and Flags(self).__set(flags) or self

	def __str__(self):
		return debugstr('Flags', *[k+'='+str(getattr(self,k)) for k in dir(self) if k[0]!='_'])

class Storage:
	def __init__(self, name, unregister):
		if not name.endswith('.json'):
			name += '.json'
		self.name	= name
		self.unregister	= unregister
		self.open	= True

	def write(self, ob, flags=None):
		LOG('Swrite', self.name, ob, flags)
		if not self.open:
			raise RuntimeError('write to already closed database '+self.name)
		with LockedFile(self.name) as lock:
			with open(self.name+'.tmp', 'w') as f:
				json.dump(ob, f)
				if not flags or not flags.unsafe:
					f.flush()
			os.rename(self.name+'.tmp', self.name)

	def read(self, flags=None):
		if not self.open:
			raise RuntimeError('read from already closed database '+self.name)
		if flags and flags.create:
			try:
				# this is not entirely correct
				# as there still is a race
				with open(self.name, 'x') as f:
					json.dump({}, f)
				if not flags.unsafe:
					f.flush()
			except:
				pass
		with open(self.name, 'r') as f:
			return json.load(f)

	def close(self, flags=None):
		self.open	= False
		if self.unregister:
			self.unregister(self)
		self.unregister	= None

	def destroy(self, flags=None):
		o	= self.read()
		self.close(flags)
		if not o or (flags and flags.force):
			unlink(self.name)

	def __str__(self):
		return debugstr('open' if self.open else 'closed', 'storage', self.name)

class Entrydata:
	def __init__(self, e, parent, o, key):
		self.e		= e		# do we need this?
		self.parent	= parent
		self.o		= o[key]
		self.key	= key
		self.map	= weakref.WeakValueDictionary()

	def get(self, key, default):
		LOG('Eget', key, default, self.o)
		if key not in self.o:
			if default is None:
				# Raising is wrong
				# Creating an empty object is wrong
				# We should store None here.
				# On strore later down the road,
				# we should create the intermediage None-Objects as dicts on the fly.
				# However accessing it down the road should raise then (in ob())
				# but only if the parent is None as well.  (Else return None gracefully)
				# TODO XXX TODO BUG leave that to the future
#				raise KeyError(self.path(key))
				default = {}
			self.o[key] = default
			self.invalidate(key)
		if key in self.map:
			return self.map[key]
		e		= Entry(self, self.o, key)
		self.map[key]	= e
		return e

	def set(self, key, val):
		LOG('Eset', key, val, self.o)
		if key in self.o and val is self.o[key]:
			return
		# TODO XXX TODO BUG storing None should be equivalent to delete
		# However today this is wrong, as the None then comes back as {}, see bug in .get()
		self.o[key]	= val
		self.invalidate(key);

	def delete(self, key):
		if key not in self.o:
			return
		del self.o[key]
		self.invalidate(key)

	def path(self, key):
		return self.parent and self.parent.path(self.key)+'.'+key or '..'

	# invalidation blows up
	def invalidate(self, key=None):
		if key:
			self.dirt(key)
		self.parent	= None
		for key in self.map:
			self.map[key].invalidate()
		self.e		= None

	# dirt falls down, but points to itself
	def dirt(self, key):
		if self.parent:
			self.parent.dirt(self.key+'.'+key)

	def ob(self):
		return self.o

class DBdata:
	def __init__(self, db, parent, store, flags):
		LOG('Dinit', db, parent, store, flags)
		self.db		= db
		self.parent	= parent
		self.store	= store
		self.flags	= flags
		self.name	= store.name
		self.o		= None
		self.dirty	= False

	def read(self):
		self.o		= self.store.read(self.flags)

	def get(self, key):
		LOG('Dget', key, self.o, self)
		if key not in self.o:
			self.o[key]	= {}
		return Entry(self, self.o, key)

	def path(self, sub):
		LOG('Ddirt', sub, self)
		return sub

	def dirt(self, key):
		LOG('Ddirt', key, self)
		self.dirty	= True

	def flush(self, flags=None):
		LOG('Dflush', flags, self)
		flags	 = self.flags(flags)
		if self.dirty or flags.force:
			self.store.write(self.o, flags)
			self.dirty	= False

	def close(self, flags=None):
		LOG('Dclose', flags, self)
		if not self.store:
			return
		self.flush(flags)
		self.store.close(self.flags(flags))
		self.unregister()

	def discard(self, flags=None):
		LOG('Ddiscard', flags, self)
		if self.store and self.dirty:
			raise RuntimeError('discarding unsaved data: '+self.name)

	def destroy(self, flags=None):
		LOG('Ddestroy', flags, self)
		flags	= self.flags(flags)
		if not flags.unsafe:
			self.flush(flags)
		self.store.destroy(flags)
		self.unregister()

	def unregister(self):
		LOG('Dunreg', self.store, self)
		if not self.store:
			return
		self.store.close()
		self.parent.unregister(self.db)
		self.store	= None

	def __str__(self):
		return debugstr('DB', 'dirty' if self.dirty else 'clean', self.store if self.store else self.name)

# Helpers for the proxy to access the underlying class instance
# and not the proxied object
def _Define(c, attr, val):
	return object.__setattr__(c, attr, val)

def _Direct(c, attr):
	return object.__getattribute__(c, attr)

# This is just a proxy.  The real functionality is defined in Entrydata
class Entry:
	def __init__(self, *args, **kw):
		_Define(self, 'd', Entrydata(self, *args, **kw))

	def __getattribute__(self, key, default=None):
		return _Direct(self, 'd').get(key, default)
	# XXX TODO XXX BUG: support key.sub, as __getattr__ does not handle this correctly
	__getitem__	= __getattribute__

	def __setattr__(self, key, val):
		_Direct(self, 'd').set(key, val)
	# XXX TODO XXX BUG: support key.sub, as __setattr__ does not handle this correctly
	__setitem__	= __setattr__
	
	def __delattr__(self, key):
		_Direct(self, 'd').delete(key)
	# XXX TODO XXX BUG: support key.sub, as __delattr__ does not handle this correctly
	__delitem__	= __delattr__

	def __nonzero__(self):	return bool(_Direct(self, 'd').ob())
	def __str__(self):	return str (_Direct(self, 'd').ob())
	def __repr__(self):	return repr(_Direct(self, 'd').ob())
	def __hash__(self):	return hash(_Direct(self, 'd').ob())


# This is just a proxy.  The real functionality is defined in DBdata
class DB:
	def __init__(self, *args, **kw):
		d	= DBdata(self, *args, **kw)
		_Define(self, 'd', d)
		# Evil can happen now
		d.read()

	def __setattr__(self, *args):
		raise RuntimeError('database objects cannot be altered')
	__setitem__	= __setattr__

	def __getattribute__(self, key):
		return _Direct(self, 'd').get(key)
	__getitem__	= __getattribute__

	def __enter__(self):
		return self

	def __exit__(self, typ, val, tb):
		_Direct(self, 'd').close();

	def __del__(self):
		_Direct(self, 'd').discard();


class NoDB:
	storage	= Storage

	def __init__(self, path=None, **flags):
		if path is None: path='.'
		self._dbs	= []
		self._store	= {}
		self._flags	= Flags(flags)
		self._path	= resolve(os.getcwd(), path)

	def open(self, name, **flags):
		name	= resolve(self._path, name)
		store	= self._storage(name)
		d	= DB(self, store, self._flag(flags))
		self._dbs.append(d)
		return d

	def unregister(self, db):
		self._dbs.remove(db)

	def flush(self, db=None, flags=None):
		flags	= self._flag(flags)
		if db:
			_Direct(db, 'd').flush(flags)
			return self
		for d in self._dbs:
			_Direct(d, 'd').flush(flags)
		return self

	def close(self, db=None, flags=None):
		flags	= self._flag(flags)
		if db:
			_Direct(db, 'd').close(flags)
			return self
		for d in self._dbs:
			_Direct(d, 'd').close(flags)
		return self

	def destroy(self, db=None, flags=None):
		flags	= self._flag(flags)
		if db:
			if isinstance(db, str):
				db	= self.open(db, flags)
			_Direct(db, 'd').destroy(flags)
			return self
		for d in self._dbs:
			_Direct(d, 'd').destroy(flags)
		return self

	def _storage(self, name):
		if name in self._store:
			return self._store[name]

		def unregister(st):
			assert self._store[name] is st
			del self._store[name]

		st			= self.storage(name, unregister)
		self._store[name]	= st
		return st

	def _flag(self, flags):
		return self._flags(flags)

def main(prog, db, key, val=None):
	db	= os.path.expanduser(db)
	d	= NoDB(create=True)
	if val:
		with d.open(db) as h:
			h.main[key]	= val
	print('key',key,'=',d.open(db).main[key])

if __name__=='__main__':
	sys.exit(main(*sys.argv))

