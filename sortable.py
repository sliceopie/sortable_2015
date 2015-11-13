# Coding Challenge - sortable
# Assumptions:
#   - Product lists and their matches are limited such that storing them entirely in memory is reasonable.
#   - Listings are huge, and should not be loaded into memory. The listings are assumed to be received in streams or should be processed in chunks. 
# Notes: 
#    A tree of dictionaries will be used to store products into a hierarchy. This will allow quick association due to the relatively good performance of python dictionaries
#    If there isn't a match to all known data, then a match is not made. Family names are required to match if present in product.
#    If there are multiple complete matches, the product is assumed to be an accessory or combination, and not the actual product desired.
#    String comparison is used for matching. There are proprietary matching algorithms that might be able to do better. As well, string matching can make very good use of GPUs for better speed and efficiency. But, before spending a huge amount of time on these optimizations, need should be identified. 

import sys
import argparse
import json
from itertools import islice

# Parse the arguments
parser = argparse.ArgumentParser(description='Map products to list')
parser.add_argument('--list', dest='list', type=str, nargs='?', default='listings.txt', help='listings path')
parser.add_argument('--products', dest='prod', type=str, nargs='?', default='products.txt', help='list of products path')
parser.add_argument('--outfile', dest='outfile', type=str, nargs='?', default='out.txt', help='mapped output')
args = parser.parse_args()


# Process product data
productsDataRaw = '['
with open(args.prod) as productFile:
    productsDataRaw += productFile.read().strip().replace("\n", ",")
productFile.close()
productsDataRaw += ']'

productsJSON = json.loads(productsDataRaw)

#Generate a tree of products to index listings against
#sets at each level for quick indexing
productTree = {}

# Helper function to create nodes in tree
def getNode(parent, product, field):
    node = '?'
    
    if field in product:
        node = product[field].lower()
        
    if node not in parent:
        parent[node] = {}
    return parent[node]

# Now, do actual tree creation
for product in productsJSON:
    prodmantree = getNode(productTree, product, 'manufacturer')
        
    prodmanfamtree = getNode(prodmantree, product, 'family')
        
    prodmanfammodtree = getNode(prodmanfamtree, product, 'model')
        
    prodmanfammodtree['product_name'] = product['product_name']
    prodmanfammodtree['listings'] = []
    
    
    
#process incoming stream - For the purpose of this example, the file is broken into chunks. 

with open(args.list) as listFile:
    while True:
        # Get chunk from file
        listDataLinesChunk = list(islice(listFile, 1000))
        if not listDataLinesChunk:
            break
        
        listDataRawChunk = '['
        listDataRawChunk += ",".join(listDataLinesChunk).strip()
        listDataRawChunk += ']'
        
        #convert to objects
        try:
            listingsJSON = json.loads(listDataRawChunk)
        except ValueError:
            listingsJSON = {}
        
        for listObject in listingsJSON:
            #create a search string to 
            searchString = "" + listObject['manufacturer'].lower() + " " + listObject['title'].lower()
            #Try to match. 
            matchcount = 0
            theman = ""
            thefam = ""
            themod = ""
            for man in productTree.iterkeys():
                foundmatch = False
                #search for manufacturer
                if man is not '?' and man in searchString:
                    refined1 = searchString.replace(man, "", 1) #reduce noise
                    
                    #search for family
                    for fam in productTree[man].iterkeys():
                        if fam is not '?' and fam in refined1:
                            refined2 = refined1.replace(fam, "", 1)
                            
                            #search for model
                            for  model in productTree[man][fam].iterkeys():
                                if model is not '?' and model  in refined2:
                                    foundmatch = True
                                    matchcount += 1
                                    theman = man
                                    thefam = fam
                                    themod = model
                    #Check for no-family models if none is found
                    if not foundmatch and '?' in productTree[man]:
                        for  model in productTree[man]['?'].iterkeys():
                            if model is not '?' and model  in refined1:
                                foundmatch = True
                                matchcount += 1
                                theman = man
                                thefam = '?'
                                themod = model
            # We got it                        
            if matchcount == 1:
                productTree[theman][thefam][themod]['listings'].append(listObject)
            
listFile.close()


#Output all the data
with open(args.outfile, 'w') as outFile:
    outstring = "\n"
    outlist = []
    for man in productTree.itervalues():
        if isinstance(man, dict):
            for fam in man.itervalues():
                if isinstance(fam, dict):
                    for prod in fam.itervalues():
                        if isinstance(prod, dict):
                            outlist.append(json.dumps(prod))

    outFile.write(outstring.join(outlist))
outFile.close()

