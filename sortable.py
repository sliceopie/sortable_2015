# Coding Challenge - sortable
# Assumptions:
#   - Product lists and their matches are limited such that storing them entirely in memory is reasonable.
#   - Listings are huge, and should not be loaded into memory. The listings are assumed to be received in streams or should be processed in chunks. 
# Notes: 
#    A tree of dictionaries will be used to store products into a hierarchy. This will allow quick association due to the relatively good performance of python dictionaries
#    If there isn't a match to all known data, then a match is not made. Family names are required to match if present in product.
#    If there are multiple complete matches, the product is assumed to be an accessory or combination, and not the actual product desired. Some intelligence has been added to try and resolve multiple matches. 
#    String comparison is used for matching. There are proprietary matching algorithms that might be able to do better. As well, string matching can make very good use of GPUs for better speed and efficiency. But, before spending a huge amount of time on these optimizations, need should be identified. 
#    There is a weakness in the algorithm such that false matches may be seen with a large amount of noise. This could be reduced with a tradeoff for time.; 

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
productTree = {} #For searching

# Helper function to create nodes in tree
def getNode(parent, product, field):
    node = '?'
    
    if field in product:
        node = product[field].lower().replace("-", "").replace("_", "").replace(" ", "")
        
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
    
    
#stats
matched = 0
unmatched = 0
    
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
            
            #########################################################
            ### Stage 1 : Initial matching
            #########################################################
            
            #create a search string to 
            searchString = u"" + listObject['manufacturer'].lower() + " " + listObject['title'].lower()
            searchStringcompressed = searchString.replace("-", "").replace("_", "").replace(" ", "")
            #Try to match. 
            matchcount = 0
            mans = []
            fams = []
            mods = []
            for man in productTree.iterkeys():
                foundmatch = False
                
                #search for manufacturer
                if man is not '?' and man in searchStringcompressed:
                    refined1 = searchStringcompressed.replace(man, "", 1) #reduce noise
                    
                    #search for family
                    for fam in productTree[man].iterkeys():
                        if fam is not '?' and fam in refined1:
                            refined2 = refined1.replace(fam, "", 1)
                            
                            #search for model
                            for  model in productTree[man][fam].iterkeys():
                                if model is not '?' and model  in refined2:
                                    foundmatch = True
                                    mans.append(man)
                                    fams.append(fam)
                                    mods.append(model)
                                        
                    #Check for no-family models if none is found
                    if not foundmatch and '?' in productTree[man]:
                        for  model in productTree[man]['?'].iterkeys():
                            if model is not '?' and model  in refined1:
                                foundmatch = True
                                mans.append(man)
                                fams.append("?")
                                mods.append(model)
            
            
            #########################################################
            ### Stage 2 : Recover from multiple hits
            #########################################################
            
            #check if there are multiples... try to resolve
            #reject any contradictions, accept most detailed in case of varying detail
            if len(mans) > 1:
                #simple case: same manufacturer and family
                if mans.count(mans[0]) == len(mans) and fams.count(fams[0]) == len(fams):
                    #next, filter out any model that can't be made of discrete in-order tokens (i.e. this 53 henry should not match to a model of s53h) This is expensive, so we don't do it by default.
                    searchList = (u"".join([ c if c.isalnum() else " " for c in searchString ])).split()

                    for mod in list(mods):
                        foundExactMatch = False
                        for idx, token in enumerate(searchList):
                            if mod[0] == token[0]:
                                matchString = ""
                                for remtokens in searchList[idx:]:
                                    matchString += remtokens
                                    if mod == matchString:
                                        foundExactMatch = True
                                        break
                                    if len(matchString) > len(mod):
                                        break
                            if foundExactMatch:
                                break
                        if not foundExactMatch:
                            mods.remove(mod)
                            mans.pop()
                            fams.pop()
                    
                    #Do we need more trimming yet?
                    if len(fams) > 1:
                        #take the longes item in the list
                        modIsValid = True
                        maybeValid = max(mods, key=len)
                        for mod in mods:
                            if mod not in maybeValid:
                                modIsValid = False
                                
                        #If all the models are embedded in eachother, the most specific is the best
                        #We have already removed any partial tokens
                        #If the models are all in a distinct position within the string, then this is lekely an accessory
                        #An improvement would be to re-evaluate the models baseed on a more intensive search at this point
                        if modIsValid:
                            mans = [mans[0]]
                            fams = [fams[0]]
                            mods = [maybeValid]
                    else:
                        pass
            
            # We got it
            if len(mans) == 1:
                matched += 1;
                productTree[mans[0]][fams[0]][mods[0]]['listings'].append(listObject)
            else:
                unmatched += 1;
#                print ("TheLen: " + str(len(mans)))
#                print(json.dumps(listObject, ensure_ascii=False).encode('utf8'))
#                print(searchString.encode('utf8'))
#                print(mans)
#                print(fams)
#                print(mods)
#                print("\n")
            
listFile.close()

print("Successfuly Matched: " + str(matched))
print("Successfuly Ignored: " + str(unmatched))

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

