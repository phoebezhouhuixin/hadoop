# Call this with one argument: the character ID you are starting from.
# For example, Spider Man is 5306, The Hulk is 2548. Refer to marvelnames.txt for others.

import sys

print('Creating BFS starting input for character ' + sys.argv[1])

with open("BFS-iteration-0.txt", 'w') as out:
    with open("marvelgraph.txt") as f:

        for line in f:
            fields = line.split()
            heroID = fields[0]
            numConnections = len(fields) - 1
            connections = fields[-numConnections:] # list of all friends

            color = 'WHITE' # unseen
            distance = 9999

            if (heroID == sys.argv[1]) :  # if the hero is the character ID that we are starting from
                color = 'GRAY' # seen, but have not explored its neighbouring nodes
                distance = 0

            if (heroID != ''):
                edges = ','.join(connections) # ("abc").join("def") --> 'dabceabcf'
                # i.e. we are merging all list elements into one string, such that 
                # IDs of all friends are separated by commas
                outStr = '|'.join((heroID, edges, str(distance), color))
                # --> "heroID|edgesstring|9999|WHITE"
                # 9999 edges away from the character we want to start from
                out.write(outStr)
                out.write("\n")


    f.close()

out.close()
