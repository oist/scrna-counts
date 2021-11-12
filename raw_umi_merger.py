#!/usr/bin/env python3
import pandas as pd
import os
import sys
import pickle
import gzip

# read inputs from args
try:
    script_name , ca, ruf, pf = sys.argv
except ValueError:
    sys.exit('ERROR: did not get correct input.\nusage: python raw_umi_merger.py <cluster_assignment_file> <raw_umi_folder> <pickle_output_file>')

#hardcode variables
cluster_assignments_path = ca
raw_umi_folder = ruf
pickle_file = pf

#create cluster dictionary
ca_file = open(cluster_assignments_path)
ca = {}
next(ca_file)

#filling cluster dictionary
for line in ca_file:
    line = line.strip().split()
    ca[line[0]] = line[1]
ca_file.close()

#combine raw umi files
raw_umi_files = next(os.walk(raw_umi_folder))[2]

umi = pd.DataFrame()

N = len(raw_umi_files)
i = 0
for f in raw_umi_files:
    i += 1
    umi_ind = pd.read_csv(gzip.open(raw_umi_folder+f), sep="\t")  #open each gzip file
    umi_ind = umi_ind[(umi_ind != 0).sum(axis=1) >= 1]  #remove genes with all zero
    umi = umi.join(umi_ind, how='outer')  #combine dataframes

# put zeros instead of nan
umi = umi.fillna(0.0)

#add clusters
umi = umi.append(pd.DataFrame([[ca[x] if x in ca else '-1' for x in umi.columns]], columns=umi.columns, index=["cluster"]))

# save pickle rick
umi.to_pickle(pickle_file)

