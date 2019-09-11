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
import operator

DEBUG=False

def LOG(*args):
	if DEBUG:
		print('(',end='', file=sys.stderr)
		print(' '.join([str(a) for a in args]), end='', file=sys.stderr)
		print(')', file=sys.stderr)
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
		LOG('Lfile', self)
		return self.fd

	def lock(self):
		if not self.locks:
			self.fd	= os.open(self.name, os.O_RDWR)
			LOG('Locking', self)
			fcntl.lockf(self.fd, fcntl.LOCK_EX)
		self.locks  += 1
		LOG('Locked', self)

	def unlock(self, force=False):
		LOG('Lunlock', self)
		if not self.locks:  return self
		self.locks	-= 1
		if force:   self.locks	= 0
		if not self.locks:
			os.close(self.fd)
			self.fd	= None
		LOG('Lunlocked', self)

	def __enter__(self):
		return self.lock()

	def __exit__(self, typ, val, tb):
		self.unlock()

	def __del__(self):
		self.unlock(True)

	def __str__(self):
		return debugstr('Lock', self.locks, self.fd, self.name)


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
		LOG('Sinit', self)

	def write(self, ob, flags=None):
		LOG('Swrite', ob, flags, self)
		if not self.open:
			raise RuntimeError('write to already closed database '+self.name)
		with LockedFile(self.name) as lock:
			with open(self.name+'.tmp', 'w') as f:
				json.dump(ob, f)
				if not flags or not flags.unsafe:
					f.flush()
			os.rename(self.name+'.tmp', self.name)

	def read(self, flags=None):
		LOG('Sread', flags, self)
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
		LOG('Sclose', flags)
		self.open	= False
		if self.unregister:
			self.unregister(self)
		self.unregister	= None

	def destroy(self, flags=None):
		LOG('Sdestroy', flags)
		o	= self.read()
		self.close(flags)
		if not o or (flags and flags.force):
			os.unlink(self.name)
		else:
			raise RuntimeError('refuse to destroy nonempty database '+self.name)

	def __str__(self):
		return debugstr('Storage', 'open' if self.open else 'closed', self.name)

class Entrydata:
	def __init__(self, e, parent, o, key):
		self.e		= e		# do we need this?
		self.parent	= parent
		self.orig	= o
		self.o		= o[key]
		self.key	= key
		self.map	= weakref.WeakValueDictionary()
		LOG('Einit', self)

	def get(self, key, default):
		LOG('Eget', key, default, self)
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
			self.notify(key)
		if key in self.map:
			return self.map[key]
		e		= Entry(self, self.o, key)
		self.map[key]	= e
		return e

	def set(self, key, val):
		LOG('Eset', key, val, self)
		if key in self.o and val is self.o[key]:
			return
		# TODO XXX TODO BUG storing None should be equivalent to delete
		# However today this is wrong, as the None then comes back as {}, see bug in .get()
		self.o[key]	= val
		self.notify(key);

	def delete(self, key):
		LOG('Edel', key, self)
		if key not in self.o:
			return
		del self.o[key]
		self.notify(key)

	def path(self, key):
		return self.parent and self.parent.path(self.key)+'.'+key or '..'

	def notify(self, key=None):
		LOG('Enotify', key, self)
		self.dirt(key)
		for a in self.map:
			self.map[a].invalidate()

	# invalidation blows up
	def invalidate(self):
		LOG('Einvalid', self)
		self.parent	= None
		for a in self.map:
			self.map[a].invalidate()
		self.e		= None
		self.orig	= None

	# dirt falls down, but points to itself
	def dirt(self, key):
		LOG('Edirt', key, self)
		if self.parent:
			self.parent.dirt(self.key if key is None else self.key+'.'+key)

	def ob(self):
		LOG('Eob', self)
		return self.o

	# function directly applies
	def apply0(self, fn, *args):
		LOG('Eapply0', fn, self)
		fn(self.o, *args)
		self.orig[self.key]	= self.o
		self.notify()
		LOG('Eapply0 ret', self)
		return self.o

	# function returns a value
	def apply1(self, fn, *args):
		LOG('Eapply0', fn, self)
		self.o			= fn(self.o, *args)
		self.orig[self.key]	= self.o
		self.notify()
		LOG('Eapply0 ret', self)
		return self.o

	def __str__(self):
		# we cannot use self.e in the following debugstr,
		# as this needs self.e.d being present,
		# however this is not set until self.__init__() returned,
		# but self.__init__() needs self.__str__() for LOG.
		# also debugst(self.e) needs str(self.e) needs self.ob() which needs self.__str__() which needs debugst(self.e)
		return debugstr('Entrydata', 'ok' if self.parent else 'ko', self.key, self.o)

class DBdata:
	def __init__(self, db, parent, store, flags):
		self.db		= db
		self.parent	= parent
		self.store	= store
		self.flags	= flags
		self.name	= store.name
		self.o		= None
		self.dirty	= False
		LOG('Dinit', db, parent, flags, self)

	def read(self):
		LOG('Dread', self.flags, self)
		self.o		= self.store.read(self.flags)

	def get(self, key):
		LOG('Dget', key, self.o, self)
		if key not in self.o:
			self.o[key]	= {}
		return Entry(self, self.o, key)

	def set(self, key, val):
		raise RuntimeError('database root objects cannot be altered: '+key)

	def delete(self, key):
		LOG('Ddel', key, self.o, self)
		del self.o[key]
		self.dirt(key)

	def path(self, sub):
		LOG('Dpath', sub, self)
		return sub

	def dirt(self, key):
		LOG('Ddirt', key, self)
		self.dirty	= True
		if not self.flags.manual:
			self.flush()

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
		d	= Entrydata(self, *args, **kw)
		_Define(self, 'd', d)

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

	# I think this still is terribly incomplete

	def __nonzero__(self):	return bool(_Direct(self, 'd').ob())
	def __str__(self):	return str (_Direct(self, 'd').ob())
	def __dir__(self):	return dir (_Direct(self, 'd').ob())
	def __repr__(self):	return repr(_Direct(self, 'd').ob())
	def __hash__(self):	return hash(_Direct(self, 'd').ob())

	def __reversed__(self):	return reversed(_Direct(self, 'd').ob())

	def __abs__(self,o):	return abs(_Direct(self, 'd').ob())
	def __int__(self,o):	return int(_Direct(self, 'd').ob())
	def __oct__(self,o):	return oct(_Direct(self, 'd').ob())
	def __hex__(self,o):	return hex(_Direct(self, 'd').ob())
	def __len__(self,o):	return len(_Direct(self, 'd').ob())
	def __float__(self,o):	return float(_Direct(self, 'd').ob())

	def __contains__(self,o):return o in _Direct(self, 'd').ob()
	def __index__(self,o):	return operator.index(_Direct(self, 'd').ob())
	def __getslice__(self,a,b):return _Direct(self, 'd').ob()[a:b]

	def __neg__(self,o):	return -_Direct(self, 'd').ob()
	def __pos__(self,o):	return +_Direct(self, 'd').ob()
	def __invert__(self,o):	return ~_Direct(self, 'd').ob()

	def __lt__(self,o):	return _Direct(self, 'd').ob() <  o
	def __le__(self,o):	return _Direct(self, 'd').ob() <= o
	def __eq__(self,o):	return _Direct(self, 'd').ob() == o
	def __ne__(self,o):	return _Direct(self, 'd').ob() != o
	def __gt__(self,o):	return _Direct(self, 'd').ob() >  o
	def __ge__(self,o):	return _Direct(self, 'd').ob() >= o
	def __add__(self,o):	return _Direct(self, 'd').ob() +  o
	def __sub__(self,o):	return _Direct(self, 'd').ob() -  o
	def __mul__(self,o):	return _Direct(self, 'd').ob() *  o
	def __mod__(self,o):	return _Direct(self, 'd').ob() %  o
	def __lshift__(self,o):	return _Direct(self, 'd').ob() << o
	def __rshift__(self,o):	return _Direct(self, 'd').ob() >> o
	def __and__(self,o):	return _Direct(self, 'd').ob() &  o
	def __or__(self,o):	return _Direct(self, 'd').ob() |  o
	def __xor__(self,o):	return _Direct(self, 'd').ob() ^  o
	def __floordiv__(self,o):return _Direct(self, 'd').ob() // o

	def __radd__(self,o):	return o +  _Direct(self, 'd').ob()
	def __rsub__(self,o):	return o -  _Direct(self, 'd').ob()
	def __rmul__(self,o):	return o *  _Direct(self, 'd').ob()
	def __rmod__(self,o):	return o %  _Direct(self, 'd').ob()
	def __rlshift__(self,o):return o << _Direct(self, 'd').ob()
	def __rrshift__(self,o):return o >> _Direct(self, 'd').ob()
	def __rand__(self,o):	return o &  _Direct(self, 'd').ob()
	def __ror__(self,o):	return o |  _Direct(self, 'd').ob()
	def __rxor__(self,o):	return o ^  _Direct(self, 'd').ob()
	def __rfloordiv__(self,o):return o // _Direct(self, 'd').ob()

	def __pow__(self,o):	return operator.pow(_Direct(self, 'd').ob(), o)
	def __rpow__(self,o):	return operator.pow(o, _Direct(self, 'd').ob())
	def __divmod__(self,o):	return operator.divmod(_Direct(self, 'd').ob(), o)
	def __rdivmod__(self,o):return operator.divmod(o, _Direct(self, 'd').ob())
	def __div__(self,o):	return operator.div(_Direct(self, 'd').ob(), o)
	def __rdiv__(self,o):	return operator.div(o, _Direct(self, 'd').ob())
	def __truediv__(self,o):return operator.truediv(_Direct(self, 'd').ob(), o)
	def __rtruediv__(self	,o):return operator.truediv(o, _Direct(self, 'd').ob())

# I hope this is right:
	def __enter__(self):	return _Direct(self, 'd').ob().__enter__()
	def __exit__(self,*a,**k):return _Direct(self, 'd').ob().__exit__(*a, **k)
	def __iter__(self):	return iter(_Direct(self, 'd').ob())
#	def __call__(self,*a,**k):return _Direct(self, 'd').ob()(*a, **k)
#	def __reduce__(self):	  return lambda x:x, (_Direct(self, 'd').ob(), )
#	def __reduce_ex__(self,p):return lambda x:x, (_Direct(self, 'd').ob(), )

# apply functions, which alter the value
	def __setslice__(self,	*a):	return _Direct(self, 'd').apply0(operator.setslice	, *a)
	def __delslice__(self,	*a):	return _Direct(self, 'd').apply0(operator.delslice	, *a)
	def __iadd__(self,	*a):	return _Direct(self, 'd').apply1(operator.iadd		, *a)
	def __isub__(self,	*a):	return _Direct(self, 'd').apply1(operator.isub		, *a)
	def __imul__(self,	*a):	return _Direct(self, 'd').apply1(operator.imul		, *a)
	def __imod__(self,	*a):	return _Direct(self, 'd').apply1(operator.imod		, *a)
	def __ipow__(self,	*a):	return _Direct(self, 'd').apply1(operator.ipow		, *a)
	def __ifloordiv__(self,	*a):	return _Direct(self, 'd').apply1(operator.ifloordiv	, *a)
	def __ilshift__(self,	*a):	return _Direct(self, 'd').apply1(operator.ilshift	, *a)
	def __irshift__(self,	*a):	return _Direct(self, 'd').apply1(operator.irshift	, *a)
	def __iand__(self,	*a):	return _Direct(self, 'd').apply1(operator.iand		, *a)
	def __ior__(self,	*a):	return _Direct(self, 'd').apply1(operator.ior		, *a)
	def __ixor__(self,	*a):	return _Direct(self, 'd').apply1(operator.ixor		, *a)
	def __idiv__(self,	*a):	return _Direct(self, 'd').apply1(operator.idiv		, *a)
	def __itruediv__(self,	*a):	return _Direct(self, 'd').apply1(operator.itruediv	, *a)

# see also https://github.com/ionelmc/python-lazy-object-proxy/blob/master/src/lazy_object_proxy/slots.py

# This is just a proxy.  The real functionality is defined in DBdata
class DB:
	def __init__(self, *args, **kw):
		d	= DBdata(self, *args, **kw)
		_Define(self, 'd', d)
		# Evil can happen now that self.d is defined:
		d.read()

	def __getattribute__(self, key):
		return _Direct(self, 'd').get(key)
	# XXX TODO XXX BUG: support key.sub, as __getattr__ does not handle this correctly
	__getitem__	= __getattribute__

	def __setattr__(self, key, val):
		_Direct(self, 'd').set(key, val)
	# XXX TODO XXX BUG: support key.sub, as __setattr__ does not handle this correctly
	__setitem__	= __setattr__

	def __delattr__(self, key):
		return _Direct(self, 'd').delete(key)
	# XXX TODO XXX BUG: support key.sub, as __delattr__ does not handle this correctly
	__delitem__	= __delattr__

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
		return self._open(name, flags)

	def _open(self, name, flags):
		LOG('Nopen', name, flags)
		name	= resolve(self._path, name)
		store	= self._storage(name)
		d	= DB(self, store, self._flag(flags))
		self._dbs.append(d)
		return d

	def unregister(self, db):
		LOG('Nunreg', db)
		self._dbs.remove(db)

	def flush(self, db=None, **flags):
		LOG('Nflush', db, flags)
		flags	= self._flag(flags)
		if db:
			_Direct(db, 'd').flush(flags)
			return self
		for d in self._dbs:
			_Direct(d, 'd').flush(flags)
		return self

	def close(self, db=None, **flags):
		LOG('Nclose', db, flags)
		flags	= self._flag(flags)
		if db:
			_Direct(db, 'd').close(flags)
			return self
		for d in self._dbs:
			_Direct(d, 'd').close(flags)
		return self

	def destroy(self, db=None, **flags):
		LOG('Ndestroy', db, flags)
		flags	= self._flag(flags)
		if db:
			if isinstance(db, str):
				db	= self._open(db, flags)
			_Direct(db, 'd').destroy(flags)
			return self
		for d in self._dbs:
			_Direct(d, 'd').destroy(flags)
		return self

	def _storage(self, name):
		if name in self._store:
			LOG('Nstore', 'known', name)
			return self._store[name]

		def unregister(st):
			LOG('Nstore-unreg', name, st)
			assert self._store[name] is st
			del self._store[name]

		st			= self.storage(name, unregister)
		self._store[name]	= st
		LOG('Nstore', 'new', name, st)
		return st

	def _flag(self, flags):
		return self._flags(flags)

def main(prog, db, key, val=None):
	LOG('Nmain', 'start', prog, db, key, val)
	db	= os.path.expanduser(db)
	d	= NoDB(create=True)
	if val:
		with d.open(db) as h:
			h.main[key]	= val
	print('key',key,'=',d.open(db).main[key])
	LOG('Nmain', 'end')
	return 0

if __name__=='__main__':
	sys.exit(main(*sys.argv))

