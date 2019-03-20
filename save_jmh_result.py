#!/usr/bin/env python
# -*- coding: utf-8 -*-
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

####################################################
# Sample script that shows how to save result data #
####################################################
import datetime
import os
import urllib
import urllib2
import argparse
import csv
import json
import random

# You need to enter the real URL and have the server running
DEFAULT_CODESPEED_URL = 'http://localhost:8000/'

current_date = datetime.datetime.today()


parser = argparse.ArgumentParser(description='Upload jmh benchmark csv results')
parser.add_argument('--commit', dest='commit', required=True,
                    help='md5')
parser.add_argument('--branch', dest='branch', required=True)
parser.add_argument('--input', dest='input', required=False,
                    help='input csv file')
parser.add_argument('--environment', dest='environment', required=True)
parser.add_argument('--dry', dest='dry', action='store_true')
parser.add_argument('--codespeed', dest='codespeed', default=DEFAULT_CODESPEED_URL,
                    help='codespeed url, default: %s' % DEFAULT_CODESPEED_URL)
parser.add_argument('--project', dest='project', default="Flink")
parser.add_argument('--exec', dest='executable', default="Flink")

def readData(args):
    results = []
    if args.input:
        path = args.input
    else:
        path = "jmh-result.csv"
    modificationDate = datetime.datetime.fromtimestamp(os.path.getmtime(path))
    #modificationDate = datetime.date(2016, 8, int(args.commit))

    with open(path) as csvFile:
        reader = csv.reader(csvFile, delimiter=",")
        lines = [line for line in reader]
        header = lines[0]
        params = sorted(filter(lambda s : s.startswith("Param"), header))
        paramIndexes = map(lambda param : header.index(param), params)
        benchmarkIndex = header.index("Benchmark")
        scoreIndex = header.index("Score")
        errorIndex = scoreIndex + 1

        for line in lines[1:]:
            name = line[benchmarkIndex].split(".")[-1]
            if len(paramIndexes) > 0:
                for paramIndex in paramIndexes:
                    if len(line[paramIndex]) > 0:
                        name += "." + line[paramIndex]

            results.append({
                'commitid': args.commit,
                'branch': args.branch,
                'project': args.project,
                'executable': args.executable,
                'benchmark': name,
                'environment': args.environment,
                'lessisbetter': False,
                'units': 'records/ms',
                'result_value': float(line[scoreIndex]),

                'revision_date': str(modificationDate),
                'result_date': str(modificationDate),
                'std_dev': line[errorIndex],  # Optional. Default is blank
            })
    return results

def add(data, codespeedUrl):
    #params = urllib.urlencode(data)
    response = "None"
    try:
        f = urllib2.urlopen(
            codespeedUrl + 'result/add/json/', urllib.urlencode(data))
    except urllib2.HTTPError as e:
        print str(e)
        print e.read()
        return
    response = f.read()
    f.close()
    print "Server (%s) response: %s\n" % (codespeedUrl, response)

if __name__ == "__main__":
    args = parser.parse_args()

    data = json.dumps(readData(args), indent=4, sort_keys=True)
    if args.dry:
        print data
    else:
        add({'json': data}, args.codespeed)
