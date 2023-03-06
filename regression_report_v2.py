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

ENVIRONMENT = 2

"""
Returns a list of benchmark results
"""
def loadHistoryData(codespeedUrl, exe, benchmark, baselineSize):
    url = codespeedUrl + 'timeline/json/?' + urllib.urlencode(
        {'exe': exe, 'ben': benchmark, 'env': ENVIRONMENT, 'revs': baselineSize})
    f = urllib2.urlopen(url)
    response = f.read()
    f.close()
    timelines = json.loads(response)['timelines'][0]
    result = timelines['branches']['master'][exe]
    lessIsbBetter = (timelines['lessisbetter'] == " (less is better)")
    return result, lessIsbBetter

def detectRegression(urlToBenchmark, stds, scores, baselineSize, minRegressionRatio, minInstabilityMultiplier,
                     direction):
    sustainable_x = [min(scores[i - 3: i]) for i in range(3, baselineSize)]
    baseline_throughput = max(sustainable_x)
    current_throughput = max(scores[-3:])
    current_instability = stds[-1] / current_throughput
    if direction * (1 - current_throughput / baseline_throughput) > max(minRegressionRatio, minInstabilityMultiplier * current_instability):
        print "<%s|%s> baseline=%s current_value=%s" % (urlToBenchmark, benchmark, baseline_throughput, current_throughput)

def checkBenchmark(args, exe, benchmark):
    results, lessIsbBetter = loadHistoryData(args.codespeedUrl, exe, benchmark, args.baselineSize + 3)
    results = list(reversed(results))
    scores = [score for (date, score, deviation, commit, branch) in results]
    stds = [deviation for (date, score, deviation, commit, branch) in results]

    urlToBenchmark = args.codespeedUrl + 'timeline/#/?' + urllib.urlencode({
        'ben': benchmark,
        'exe': exe,
        'env': ENVIRONMENT,
        'revs': args.displaySamples,
        'equid': 'off',
        'quarts': 'on',
        'extr': 'on'})

    if len(results) < args.baselineSize:
        return

    direction = 1
    if lessIsbBetter:
        scores = [-1 * score for score in scores]
        direction = -1
    detectRegression(urlToBenchmark, stds, scores, args.baselineSize, args.minRegressionRatio,
                     args.minInstabilityMultiplier, direction)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Regression report based on Max/Min value')
    parser.add_argument('--baseline-size', dest='baselineSize', required=False, default=30, type=int,
                        help='Number of samples taken as the base line.')
    parser.add_argument('--display-samples', dest='displaySamples', required=False, default=200,
                        type=int,
                        help='Number of samples to display in regression report for human inspection. Not all values '
                             'are working.')
    parser.add_argument('--min-regression-ratio', dest='minRegressionRatio', required=False,
                        default=0.04, type=float,
                        help='A regression should be alerted only if the ratio of change between the baseline '
                             'throughput and the current throughput exceeds the configured value.')
    parser.add_argument('--min-instability-multiplier', dest='minInstabilityMultiplier', required=False,
                        default=2, type=float,
                        help="Min instability multiplier to measure deviation.")
    parser.add_argument('--codespeed-url', dest='codespeedUrl', default="http://codespeed.dak8s.net:8000/",
                        help='The codespeed url.')

    args = parser.parse_args()
    execToBenchmarks = loadBenchmarkNames(args.codespeedUrl)
    for exe, benchmarks in execToBenchmarks.items():
        for benchmark in benchmarks:
            checkBenchmark(args, exe, benchmark)
