#!/usr/bin/env python3

import nodb

#nodb.DEBUG=1

no = nodb.NoDB()
db = no.open('file', create=True)

t  = db.main
o1 = t['object1']

o2 = t.object2
o1['item']=1
o2.item=1
o1['w'].t['f']=2
o2.w.t.f=2

print(o1 == o2)
print(o1 is o2)

