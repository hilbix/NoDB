#!/usr/bin/env python3

import nodb
import atexit

nodb.DEBUG=1

#nodb.NoDB().open('teste')

no      = nodb.NoDB()
no.close(no.open('test', create=True))

print("--0--")

db		= no.open('test')
t		= db.table1
t.one		= 3
t.two.three	= 4
no.close(db)

print("--1--")
v	= no.open('test').table1.two
print("--1a--")
print(v.three)
print("--1b--")
assert v.three==4
print("--2--")
v.three	+= 1
print("--2a--")
assert v.three==5

print("--A--")
del no.open('test').table1.one
print("--B--")
del no.open('test').table1.two
print("--C--")
del no.open('test').table1
print("--D--")

no.destroy('test')

