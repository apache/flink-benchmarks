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

import datetime
import urllib
import urllib2
import argparse
import json
import re

DEFAULT_CODESPEED_URL = 'http://codespeed.dak8s.net:8000/'
ENVIRONMENT = 2

current_date = datetime.datetime.today()

parser = argparse.ArgumentParser(description='Upload jmh benchmark csv results')
parser.add_argument('--base-line-size', dest='baseLine', required=False, default=100, type=int,
                    help='Number of samples taken as the base line.')
parser.add_argument('--download-samples-size', dest='downloadSamples', required=False, default=200,
                    type=int,
                    help='Number of samples download from the codespeed. Not all values are '
                         'working.')
parser.add_argument('--recent-trend-size', dest='recentTrend', required=False, default=20, type=int,
                    help='Recent trend size, that is used to compare against the base line.')
parser.add_argument('--median-trend-threshold', dest='medianTrendThreshold', required=False,
                    default=-4, type=float,
                    help="Tolerated threshold for change between baseline and recent trend median "
                         "in percentages")
parser.add_argument('--dev-ratio-threshold', dest='devRatioThreshold', required=False, default=5,
                    type=float)
parser.add_argument('--codespeed', dest='codespeed', default=DEFAULT_CODESPEED_URL,
                    help='codespeed url, default: %s' % DEFAULT_CODESPEED_URL)

"""
Returns a dict executable id -> revision
"""
def loadExecutableAndRevisions(codespeedUrl):
    revisions = {}
    url = codespeedUrl + 'reports'
    f = urllib2.urlopen(url)
    response = f.read()
    f.close()
    for line in response.split('\n'):
        # Find urls like: /changes/?rev=b8e7fc387dd-ffcdbb4-1647231150&amp;exe=1&amp;env=Hetzner
        # and extract rev and exe params out of it
        reports = dict(re.findall(r'([a-z]+)=([a-z0-9\-]+)', line))
        if "exe" in reports and "rev" in reports:
            exe = reports["exe"]
            rev = reports["rev"]
            # remember only the first (latest) revision for the given executable
            if exe not in revisions:
                revisions[exe] = rev
    return revisions

"""
Returns a dict executable -> benchmark names 
"""
def loadBenchmarkNames(codespeedUrl):
    execToBenchmarks = {}
    revisions = loadExecutableAndRevisions(codespeedUrl)
    # Per each revision/executable ask for benchmark names
    # http://codespeed.dak8s.net:8000/changes/table/?tre=10&rev=b9593715f35-ffcdbb4-1647399268&exe=1&env=2
    for exe, rev in revisions.items():
        url = codespeedUrl + 'changes/table/?' + urllib.urlencode({'tre': '10', 'rev': rev, 'exe': exe, 'env': ENVIRONMENT})
        f = urllib2.urlopen(url)
        response = f.read()
        f.close()
        benchmarks = []
        for line in response.split("\n"):
            # look for lines like and extract benchmark name between characters > and <
            # <td title="">remoteSortPartition</td>
            if '<td title="">' in line:
                benchmark = re.findall(r'\>([^<]+)\<', line)[0]
                benchmarks.append(benchmark)
        execToBenchmarks[exe] = benchmarks
    return execToBenchmarks

"""
Returns a list of benchmark results
"""
def loadData(codespeedUrl, exe, benchmark, downloadSamples):
    url = codespeedUrl + 'timeline/json/?' + urllib.urlencode({'exe': exe, 'ben': benchmark, 'env': ENVIRONMENT, 'revs': downloadSamples})
    f = urllib2.urlopen(url)
    response = f.read()
    f.close()
    result = json.loads(response)['timelines'][0]['branches']['master'][exe]
    return [score for (date, score, deviation, commit, branch) in result]

def getMedian(lst):
    lst = sorted(lst)  # Sort the list first
    if len(lst) % 2 == 0:  # Checking if the length is even
        # Applying formula which is sum of middle two divided by 2
        return (lst[len(lst) // 2] + lst[(len(lst) - 1) // 2]) / 2
    else:
        # If length is odd then get middle value
        return lst[len(lst) // 2]

def checkBenchmark(args, exe, benchmark):
    results = loadData(args.codespeed, exe, benchmark, args.downloadSamples)

    urlToBenchmark = args.codespeed + 'timeline/#/?' + urllib.urlencode({
        'ben': benchmark,
        'exe': exe,
        'env': ENVIRONMENT,
        'revs': args.downloadSamples,
        'equid': 'off',
        'quarts': 'on',
        'extr': 'on'})

    if len(results) < args.baseLine:
        return
    median = getMedian(results[args.recentTrend:args.baseLine])
    recent_median = getMedian(results[:args.recentTrend])
    median_trend = 0
    if median != 0:
        median_trend = recent_median * 100 / median - 100
    if median_trend < args.medianTrendThreshold:
        # regression detected, print warning
        print "<%s|%s> median=%s recent_median=%s" % (urlToBenchmark, benchmark, median, recent_median)

if __name__ == "__main__":
    args = parser.parse_args()
    execToBenchmarks = loadBenchmarkNames(args.codespeed)
    for exe, benchmarks in execToBenchmarks.items():
        for benchmark in benchmarks:
            checkBenchmark(args, exe, benchmark)
