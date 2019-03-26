import concurrent.futures
import urllib
import json 
import requests

#URLS =  #[some list of urls]
with open("1small.txt", 'r') as infile1, open("2small.txt", 'r') as infile2, open("verticesList.txt", "w+"):
    list1 = [line.strip() for line in infile1]
    list2 = [line.strip() for line in infile2]
    URLS = []

    for l1 in list1:
        #verticesList.write(l1 + "\n")
        for l2 in list2:
            #print ("\n Finding paths between %s and %s \n" %(l1, l2))
            #verticesList.write(l2 + "\n")
            data = {'gremlin':'g.withSideEffect("Neptune#repeatMode","CHUNKED_DFS").withSack(0).V().hasId("{}").repeat(outE().sack(sum).by("weight").inV().simplePath()).times(3).emit(hasId("{}")).hasId("{}").limit(300).order().by(sack(),incr).local(union(path().by(id).by("weight"),sack()).fold()).toList()'.format(l1, l2, l2)}
            #print data
            u = ('http://neptunedbcluster-70fbv2otqb11.cluster-c814mxxksbjw.us-east-1.neptune.amazonaws.com:8182/gremlin', {'gremlin':'g.withSideEffect("Neptune#repeatMode","CHUNKED_DFS").withSack(0).V().hasId("{}").repeat(outE().sack(sum).by("weight").inV().simplePath()).times(3).emit(hasId("{}")).hasId("{}").limit(300).order().by(sack(),incr).local(union(path().by(id).by("weight"),sack()).fold()).toList()'.format(l1, l2, l2)})
            #print response.content
            URLS.append(u)


# Retrieve a single page and report the url and contents
def load_url(url, timeout):
    conn = urllib.request.urlopen(url, timeout=timeout)
    return conn.readall()

# We can use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Start the load operations and mark each future with its URL
    future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]
        try:
            data = future.result()
            # do json processing here
        except Exception as exc:
            print('%r generated an exception: %s' % (url, exc))
        else:
            print('%r page is %d bytes' % (url, len(data)))