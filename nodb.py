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

#import itertools
#from deco import *

#def LOG(*args): print(*args, file=sys.stderr); sys.stderr.flush()

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
		self(flags)

	def __call__(self, flags):
		if flags:
			for a in flags:
				assert hasattr(self, a), 'unknown flag: '+a
				setattr(self, a, flags[a])
		return self

class Storage:
	def __init__(self, name, unregister):
		if not name.endswith('.json'):
			name += '.json'
		self.name	= name
		self.unregister	= unregister

	def write(self, ob, flags=None):
		with LockedFile(self.name) as lock:
			with open(self.name+'.tmp', 'w') as f:
				json.dump(ob, f)
				if not flags or not flags.unsafe:
					f.flush()
			os.rename(self.name+'.tmp', self.name)

	def read(self, flags=None):
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
		self.name	= None
		self.unregister(self)

def _Define(c, attr, val):
	return object.__setattr__(c, attr, val)

def _Direct(c, attr):
	return object.__getattribute__(c, attr)

class Entry:
	def __init__(self, parent, o, key):
		_Define(self,	'parent',	parent);
		_Define(self,	'o',		o[key]);
		_Define(self,	'key',		key);

	def __getattribute__(self, key, default=None):
		o	= _Direct(self, 'o')
		if key not in o:
			if default is None:
				raise KeyError(_Direct(self, 'path')(key))
			o[key] = default
			_Direct(self, 'dirt')(_Direct(self, 'key'));
		return Entry(self, o, key)
	__getitem__	= __getattribute__

	def __setattr__(self, key, val):
		o	= _Direct(self, 'o')
		if key in o and val is o[key]:
			return
		o[key]	= val
		_Direct(self, 'dirt')(_Direct(self, 'key'));
	__setitem__	= __setattr__
	
	def __delattr__(self, key):
		o	= _Direct(self, 'o')
		if key not in o:
			return
		del o[key]
		_Direct(self, 'dirt')(_Direct(self, 'key'));
	__delitem__	= __delattr__

	def __nonzero__(self):	return bool(_Direct(self, 'o'))
	def __str__(self):	return str (_Direct(self, 'o'))
	def __repr__(self):	return repr(_Direct(self, 'o'))
	def __hash__(self):	return hash(_Direct(self, 'o'))

	def path(self, key):
		return _Direct(_Direct(self, 'parent'), 'path')(_Direct(self, 'key'))+'.'+key

	def dirt(self, key):
		_Direct(_Direct(self, 'parent'), 'dirt')(_Direct(self, 'key')+'.'+key)

class DB:
	def __init__(self, store, flags):
		_Define(self, 'store',	store)
		_Define(self, 'flags',	flags)
		_Define(self, 'o',	store.read(flags))
		_Define(self, 'dirty',	False)

	def __setattr__(self, *args):
		raise RuntimeError('database objects cannot be altered')

	def __getattribute__(self, key):
		o	= _Direct(self, 'o')
		if key not in o:
			o[key]	= {}
		return Entry(self, o, key)

	def path(self, sub):
		return sub;

	def __enter__(self):
		return self

	def __exit__(self, typ, val, tb):
		_Direct(self, 'close')();

#	def __del__(self):
#		_Direct(self, 'close')();

	def dirt(self, key):
		_Define(self, 'dirty', True)

	def close(self, flags=None):
		flags	 = _Direct(self, 'flags')(flags)
		if _Direct(self, 'dirty'):
			_Direct(self, 'store').write(_Direct(self, 'o'), flags)
		_Direct(self, 'store').close(flags)
		_Define(self, 'store', None)

class NoDB:
	dbs	= []

	def __init__(self, path=None, **flags):
		if path is None: path='.'
		self._dbs	= []
		self._store	= {}
		self._flags	= Flags(flags)
		self._path	= resolve(os.getcwd(), path)

	def open(self, name, **flags):
		name	= resolve(self._path, name)
		store	= self._storage(name)
		d	= DB(store, self._flag(flags))
		return d

	def close(self, db=None, *args):
		if db:
			_Direct(db, 'close')(db)
			return self
		for d in self.dbs:
			_Direct(d, 'close')(d)
		return self

	def _storage(self, name):
		if name in self._store:
			return self._store[name]

		def unregister(st):
			assert self._store[name] is st
			del self._store[name]

		st			= Storage(name, unregister)
		self._store[name]	= st
		return st

	def _flag(self, flags):
		return copy.copy(self._flags)(flags)

def main(prog, db, key, val=None):
	db	= os.path.expanduser(db)
	d	= NoDB(create=True)
	if val:
		with d.open(db) as h:
			h.main[key]	= val
	print('key',key,'=',d.open(db).main[key])

if __name__=='__main__':
	sys.exit(main(*sys.argv))

