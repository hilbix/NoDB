> This is nearly not tested!  And there is no test suite yet.  Help welcome!

[![CI status](https://api.cirrus-ci.com/github/hilbix/NoDB.svg?branch=master)](https://cirrus-ci.com/github/hilbix/NoDB)


# NoDB

This is a pure Python 3 JSON backed object store with an ORM like access interface:

- It is very small and basic, but hopefully very complete
- It is not optimized in any way (yet)
- The complete database is rewritten on each .flush() (which is similar to may others)
- JSON is used to serialize all data

It is not fully completed yet (see notes in the source), so some details might be missing or not working as you expect.
However most of what you need should be there to implement a quick and dirty way to maintain a complex `.json` file
with data up to several 10 MB (with 50 MB and SSD you should be able to create 10 transactions).

This is more like an ISAM storage than a real database, as there is no query language, no indeces or anything.
All you can do is to have a bunch of top level objects (like tables) in an object which then can be filled with any data,
as long as this data is JSON serializable.  Also you can work with several JSON files in parallel.

- Perhaps you can even process foreign JSON files with this.  However this expects the JSON to be a dict of dicts.
- There is no direct list support.  However you can store lists anywhere, except at the two lowest levels.
- Missing entries are auto-created as a dict.
- There are not really transactions.  But a file should be updated atomically.


## Usage

	git submodule add https://github.com/hilbix/NoDB.git
	ln -s NoDB/nodb.py lib/		# or wherever your Python includes


## Bugs

- It was not tested very much.
- Perhaps things break if you try to access something not as a string.
- This is still a very early release


## Example

	import nodb

	no	= nodb.NoDB()

	db	= no.open('test.json', create=True)
	table	= db.table1
	table.one	= 3
	table.two.three	= 4
	del table1.one

	no.close()

Like in JavaScript you can access the same as property or key.  So `table.one` is the same as `table['one']`.


## Features

- Bare minimum (less than 700 lines of code).  I doubt you can make it much smaller, even that it was not designed to minimize size.
- It is able to detect when you forgot a flush.  This raises an exception on program termination.
- If something breaks on filesystem level while writing the data (disk full, power outage), the existing data should not be harmed.
  - This is done by atomically replacing the existing data with fresh data, only if the fresh data was written successfully.
  - You can switch off `fdatasync()` with flag `unsafe=True`.  This should tremendously speed up writes by sacrificing data integrity.
  - Note on filesystems like ZFS you probably do not need `fdatasync()`, because those ensure data integrity at a higher level.
- It has a way to only drop a database when it is empty.
- Concurrent writes are serialized using locks.  Note that this does not ensure ACID way of doing things.
  - POSIX locks only work on your local machine and most time not across networks.
  - This might be improved in future using filsystem metadata to report unintended changes in the source file.
- You can replace the storage class.  This is not yet standardized, so expect that this breaks in future.


## FAQ

WTF why?

- Because handling smaller but complex data assets is a PITA when it comes to serializing and deserializing them.
- This here conveniently bundles this at a single place such that you can update your objects and then `.flush()`.
- You do not even need to remember if something has changed, as this is handled all in the background.
- Just use the object and be happy.
- Also this makes it more easy to access objects, as you do not need to handle the 'create missing properties' problem,
  as they are created on-the-fly for you.

License?

- This Works is placed under the terms of the Copyright Less License,  
  see file COPYRIGHT.CLL.  USE AT OWN RISK, ABSOLUTELY NO WARRANTY.
- Read: This is free as free beer, free speech, free baby.

Is this production ready?

- Probably not.  But if you find issues, please report them (via GitHub).
- Read:  Perhaps it is good enough to use for your needs to be extended to do reliably what you want.
- And if you like you can give back.  But you do not need to.  The License does not require you give back.

Drawbacks?

- Things can break into pieces if you irregularily replace objects which are in use somewhere else.  This is not really detected (yet).
- However this is true for all data models in a language which allows references to objects.
  If you disconnect an object from it's roots and still change it, usually nothing prevents you from doing so.
- However in a future variant (see the `WeakValueDictionary`) this might be detected and reported at runtime with an exception.

Contact?  Question?  Bug?  Problem?

- Issue or PR on GitHub.  Eventually I listen.

PyPi?  PIP?

- Isn't `git` enough already?
- If you really want to package it, try to create a PR which does not break anything.  Thanks!
- Debian/Ubuntu etc. maintaner welcome, too.  (I know how to maintain Debian packages, but I lack the time to prepare.)

`FileNotFoundError` on `nodb.NoDB().open('file')`

- The file must exist, end on `.json` (this is added automatically) and be a valid JSON file.
- To create a file use: `nodb.NoDB(create=True).open('file')` or `nodb.NoDB().open('file', create=True)`

Why `no.close(db)` and not `db.close()`?

- Because this is object oriented NoSQL ORM.  `db.close` is table `close` on `db`.  It is crucial that there are no reserved names!
- Benefit: `no.close()` closes all known databases.  Similar: `no.flush()`

Can `no = nodb.NoDB(unsafe=True, manual=True)` be used?

- `manual=True` is recommended to extremely speed up things, but then you must make sure to `.flush(db)` yourself.
  - `.close(db)` does a flush, too.
  - On Python's object destruction (`__del__` call) we cannot flush data, as blocking disk IO is probibited there.
- `unsafe=True` should only be used on filesystems, which flush metadata after or with the same transaction as the data.
  - `ZFS` should be safe, but I haven't tested it
  - `ext3` with `data=ordered` should be safe, but I haven't tested it
  - this ensures the `rename()` does not happen before all data is secured on disk.

Multiple `nodb.NoDB()` classes?

- Even that this is supported, please only use one single global `nodb.NoDB()` instance.
- You can give Flags with the `.open(name, flag=value)` call.

Multithreading?

- Not even thought about it.  (Note that Python has the GIL which makes it impossible to have real concurrently running threads.)
- Running the `nodb` in a single thread should be safe.  But please do not mix the objects between threads, this isn't supported.  Yet.
- If you have a good idea, create an issue to discuss.

Debugging?

- `import nodb; nodb.DEBUG=1`

