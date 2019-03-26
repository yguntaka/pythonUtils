with open("/Users/yguntaka/Documents/Clients/RelSci/g-hops-part1.csv") as infile, open("/Users/yguntaka/Documents/Clients/RelSci/out-y1-hops.csv", "a") as outfile:
  header_processed = False
  for line in infile:
    data = line.split(',')
    if header_processed:
      data.insert(2, 'noLabel')
    else:
      data.insert(2, 'label')
      header_processed = True
    outfile.write(",".join(data))