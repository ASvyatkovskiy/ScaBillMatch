from __future__ import print_function

#states = sorted([29,31,39,5,12,8,4,13,21,42])
#seen = [(12,13),(12,21),(12,4),(12,8),(13,21),(13,42),(21,42),(29,12),(29,13),(29,21),(29,31),(29,4),(29,42),(29,5),(29,8),(31,12),(31,13),(31,39),(31,4),(31,42),(31,5),(31,8),(39,12),(39,13),(39,21),(39,4),(39,42),(39,5),(41,3),(4,42),(5,12),(5,13),(5,21),(5,4),(5,42),(5,8),(8,21),(8,4),(8,42),(4,13),(4,21),(29,39),(8,39),(12,42),(21,31),(8,13)]

states = range(51)
input = open("processed_so_far.txt")

a,b="",""
seen = list()
to_remove = list()
for line in input.readlines():
    a,b=line.split("_")
    seen.append((int(a),int(b)))


#print (type(seen),type(seen[0]),type(seen[0][0]))
output = ""
total = 0
for a in states:
    for b in states:
        if a >= b: 
            continue
        total += 1
        if (a,b) not in seen and (b,a) not in seen: 
            #print (a,b) 
            output += "List("+str(a)+"L,"+str(b)+"L), "

#print (total)
print (output)
