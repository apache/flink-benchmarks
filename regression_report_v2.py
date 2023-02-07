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
import argparse
import json
import urllib
import urllib2

from regression_report import loadBenchmarkNames

"""
This is a regression detection algorithm based on the historical maximum/minimum value, please refer
to https://docs.google.com/document/d/1Bvzvq79Ll5yxd1UtC0YzczgFbZPAgPcN3cI0MjVkIag/edit the detailed design.
"""

DEFAULT_CODESPEED_URL = 'http://codespeed.dak8s.net:8000/'
ENVIRONMENT = 2
DEFAULT_THRESHOLD = 0.04
DEFAULT_BASELINE = 30

"""
Returns a list of benchmark results
"""
def loadHistoryData(codespeedUrl, exe, benchmark, downloadSamples):
    url = codespeedUrl + 'timeline/json/?' + urllib.urlencode({'exe': exe, 'ben': benchmark, 'env': ENVIRONMENT, 'revs': downloadSamples})
    f = urllib2.urlopen(url)
    response = f.read()
    f.close()
    timelines = json.loads(response)['timelines'][0]
    result = timelines['branches']['master'][exe]
    lessIsbBetter = (timelines['lessisbetter'] == " (less is better)")
    return result, lessIsbBetter

def checkWithMax(urlToBenchmark, stds, scores, index, baselineSize):
    sustainable_x = [min(scores[i - 2 : i + 1]) for i in range(index - baselineSize, index)]
    baseline_throughput = max(sustainable_x)
    current_throughput = max(scores[index - 3 : index])
    current_unstable = stds[index] / current_throughput
    if 1 - current_throughput / baseline_throughput > max(DEFAULT_THRESHOLD, 2 * current_unstable):
        print "<%s|%s> baseline=%s current_value=%s" % (urlToBenchmark, benchmark, baseline_throughput, current_throughput)

def checkWithMin(urlToBenchmark, stds, scores, index, baselineSize):
    sustainable_x = [max(scores[i - 2 : i + 1]) for i in range(index - baselineSize, index)]
    baseline_throughput = min(sustainable_x)
    current_throughput = min(scores[index - 3 : index])
    current_unstable = stds[index] / current_throughput
    if 1 - current_throughput / baseline_throughput < -1.0 * max(DEFAULT_THRESHOLD, 2 * current_unstable):
        print "<%s|%s> baseline=%s current_value=%s" % (urlToBenchmark, benchmark, baseline_throughput, current_throughput)

def checkBenchmark(args, exe, benchmark):
    results, lessIsbBetter = loadHistoryData(args.codespeed, exe, benchmark, args.downloadSamples)
    results = list(reversed(results))
    scores = [score for (date, score, deviation, commit, branch) in results]
    stds = [deviation for (date, score, deviation, commit, branch) in results]

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

    if lessIsbBetter:
        checkWithMin(urlToBenchmark, stds, scores, len(scores) - 1,  args.baseLine)
    else:
        checkWithMax(urlToBenchmark, stds, scores, len(scores) - 1,  args.baseLine)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Regression report based on Max/Min value')
    parser.add_argument('--base-line-size', dest='baseLine', required=False, default=DEFAULT_BASELINE, type=int,
                        help='Number of samples taken as the base line.')
    parser.add_argument('--download-samples-size', dest='downloadSamples', required=False, default=200,
                        type=int,
                        help='Number of samples download from the codespeed. Not all values are '
                             'working.')
    parser.add_argument('--trend-threshold', dest='trendThreshold', required=False,
                        default=DEFAULT_THRESHOLD, type=float,
                        help="Tolerated threshold for change between baseline and recent trend value "
                             "in percentages")
    parser.add_argument('--codespeed', dest='codespeed', default=DEFAULT_CODESPEED_URL,
                        help='codespeed url, default: %s' % DEFAULT_CODESPEED_URL)
    args = parser.parse_args()
    execToBenchmarks = loadBenchmarkNames(args.codespeed)
    for exe, benchmarks in execToBenchmarks.items():
        for benchmark in benchmarks:
            checkBenchmark(args, exe, benchmark)
