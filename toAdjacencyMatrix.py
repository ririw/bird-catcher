import sqlite3
import sys  
import itertools
import numpy as np
conn = sqlite3.connect('./graph.db')

c = conn.cursor()
i = 0
userIMap = dict()
for user in c.execute('''select id from users where include=1'''):
   userIMap[user[0]] = i
   i += 1

m = [[0 for j in range(i)] for j in range(i)]
for edge in c.execute('''select source,target from following where include=1'''):
   i = userIMap[edge[0]]
   j = userIMap[edge[1]]
   m[i][j] = 1 

k = 0
for i in m:
   for j in m:
      print j," ",
   sys.stderr.write(str(k))
   k += 1
   print ""
