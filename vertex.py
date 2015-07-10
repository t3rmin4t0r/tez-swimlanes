#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys,math,os.path
import StringIO
from amlogparser import AMLog
from getopt import getopt
from swimlane import SVGHelper, ColourManager
from collections import Counter
from itertools import groupby,chain


def usage():
	sys.stderr.write("""
usage: swimlane.py [-t ms-per-pixel] [-o outputfile] [-x] <log-file>

Input files for this tool can be prepared by "yarn logs -applicationId <application_...> | grep HISTORY".
""")

def main(argv):
	(opts, args) = getopt(argv, "o:t:x")
	out = sys.stdout
	ticks = -1 # precision of 1/tick
	fraction = -1
	extended = False
	for k,v in opts:
		if(k == "-o"):
			out = open(v, "w")
		if(k == "-t"):
			ticks = int(v)
		if(k == "-x"):
			extended = True
	if len(args) == 0:
		return usage()
	log = AMLog(args[0]).structure()
	dags = log.dags
	vertexes = sorted(list(chain(*[d.vertexes for d in dags])),key = lambda v : v.t0)
	for v in vertexes:
		print v.start, v.t0, v.t0 <= v.start
	marginTop = 128
	marginRight = 128;
	laneSize = 64
	y = len(vertexes)*laneSize
	maxx = max([d.finish for d in dags]) 
	if ticks == -1:
		ticks = min(1000, (maxx - log.zero)/2048)
	xdomain = lambda t : (t - log.zero)/ticks 
	x = xdomain(maxx)
	svg = SVGHelper(x+2*marginRight+256, y+2*marginTop)
	a = marginTop
	svg.text(x/2, 32, log.name, style="font-size: 32px; text-anchor: middle")	
	svg.text(marginRight - 16, marginTop - 32, "Vertex ID", "text-anchor:end; font-size: 16px;")
	colourman = ColourManager()
	for x1 in set(range(0, x, 10*ticks)) | set([x]):
		svg.text(marginRight+x1, marginTop-laneSize/2, "%0.2f s" % ((x1 *  ticks)/1000), "text-anchor: middle; font-size: 12px")
		svg.line(marginRight+x1, marginTop-laneSize/2, marginRight+x1, marginTop+y, "stroke: #ddd")
	# draw a grid
	for (n,v) in enumerate(vertexes):
		a = marginTop + n*laneSize
		y1 = a
		y2 = y1 + (laneSize/2)
		y3 = y1 + laneSize
		c = colourman.next()
		x1 = marginRight + xdomain(v.init)
		x2 = marginRight + xdomain(v.start)
		x3 = marginRight + xdomain(min([t.start for t in v.tasks] or [v.start]))
		x4 = marginRight + xdomain(min([t.finish for t in v.tasks] or [v.finish]))
		x5 = marginRight + xdomain(v.finish)
		svg.line(x1, y1, x1, y3, style="stroke : black")
		if (x1 != x2):
			svg.line(x1, y2, x2, y2, style="stroke : black", stroke_dasharray="8,4")
		svg.rect(x3, y1, x5, y3, style="fill: %s; stroke: black;" % (c))
		if (x3 != x2):
			svg.rect(x2, y1, x3, y3, style="fill: %s; stroke: #ccc; opacity: 0.25" % (c))
		if extended and v.tasks:
			binheight = 1.0*len(v.tasks)
			binwidth = 24 # px
			bindown = lambda a : binwidth*(a/binwidth)
			binup = lambda a : bindown(a) + a % binwidth 
			histogram = Counter()
			for t in v.tasks:
				histogram.update(xrange(bindown(xdomain(t.start)), binup(xdomain(t.finish)), binwidth))
			binsizer = lambda h : (h == 0 and 0) or 4 + math.ceil(((laneSize-4)*h)/binheight)
			binliner = lambda k, h : (max(x3, marginRight+k), y3 - binsizer(h)) 
			pts = [binliner(k,h) for k, h in sorted(histogram.items())]
			pts = [(x3, y3)] + list(pts) + [(x5, y3)] 
			svg.polyline(pts, style="opacity: 0.6")
		elif (x4 != x5):
			svg.line(x4, y1, x4, y3, style="stroke : black", stroke_dasharray="2,2")
		svg.text((x3+x5)/2, y2-12, "%s x %d" % (v.name, len(v.tasks)), style="text-anchor: middle; font-size: 9px;")
		svg.line(marginRight, a, marginRight+x, a, "stroke: #ccc")
		svg.text(marginRight - 4, y2, v.vid, "text-anchor:end; font-size: 16px;")
	
	for dag in log.dags:
		x1 = marginRight+xdomain(dag.start)
		svg.line(x1, marginTop-24, x1, marginTop+y, "stroke: black;", stroke_dasharray="8,4")
		x2 = marginRight+xdomain(dag.finish)
		svg.line(x2, marginTop-24, x2, marginTop+y, "stroke: black;", stroke_dasharray="8,4")
		svg.line(x1, marginTop-24, x2, marginTop-24, "stroke: black")
		svg.text((x1+x2)/2, marginTop-32, "%s (%0.1f s)" % (dag.name, (dag.finish-dag.start)/1000.0) , "text-anchor: middle; font-size: 12px;")		
	svg.line(marginRight, marginTop, marginRight+x, marginTop)
	svg.line(marginRight, y+marginTop, marginRight+x, y+marginTop)
	svg.line(marginRight, marginTop, marginRight, y+marginTop)
	svg.line(marginRight+x, marginTop, marginRight+x, y+marginTop)
	out.write(svg.flush())
	out.close()

	

if __name__ == "__main__":
	main(sys.argv[1:])
