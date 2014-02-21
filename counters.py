import os,sys,re,math,os.path
import StringIO
from amlogparser import AMLog
import random
from getopt import getopt
from collections import defaultdict
import numpy as np
from scipy.stats import scoreatpercentile

def fivenum(data):
	""" quartiles and min/max """
	data = list(sorted(data))
	q1 = scoreatpercentile(data,25)
	q3 = scoreatpercentile(data,75)
	md = np.median(data)
	return (min(data), q1, md, q3, max(data))


def counters(e):
	args = e.raw.args
	part = args.split("counters=").pop()
	kvs = [kv.lstrip().split("=") for kv in part.split(",") if "=" in kv]
	counters = [(k,int(v)) for (k,v) in kvs]
	return dict(counters)

def vertexes(dag):
	for vertex in dag.vertexes:
		yield vertex

def tasks(v):
	for t in v.tasks:
		yield t

def dd(d):
	dx = defaultdict(lambda: None)
	dx.update(d)
	return dx

def extract(values, rows, col):
	x = values[col]
	y = [r[col] for r in rows if r[col] is not None] 
	if( x is None):
		return (None, 0, [None]*5)
	elif(not y):
		return (x, 0, [None]*5)
	else:
		return (x, len(y), fivenum(y))

def sizeof_fmt(num):
	if num < 100*1000:
		return "%s" % num
	for x in ['','K','M','G','T']:
		if num < 1024.0:
			return "%3.1f %s" % (num, x)
		num /= 1024.0

def minmax((x,n,y)):
	x1 = (x is None and " ") or str(x)
	median = y[2]
	_min = min(y)
	_q1 = y[1]
	_q3 = y[3]
	_max = max(y)
	if not x:
		return " "
	if _max == _min:
		return "%s = %d x %s" % (x1, n, sizeof_fmt(median))
	return "%s = %d x %s [%0.2fx, %0.2fx, 1.0x, %0.2fx, %0.2fx]" % (x1, n, sizeof_fmt(median), _min/median, _q1/median, _q3/median, _max/median)

def process(dag):
	ss = lambda x:  (x is None and " ") or str(x)
	vs = list(vertexes(dag))
	cntrs = reduce(lambda a,b: set(a) | set(b), [set(counters(v).keys()) for v in vs])
	print "<table>"
	print "<tr><th>Counter Name<th>","</th><th>".join([v.name for v in vs]),"</th></tr>"
	for col in cntrs:
		print "<tr><td>%s</td>" % col
		for v in vs:
			values = dd(counters(v))
			rows = [dd(counters(t)) for t in tasks(v)]
			formatted = minmax(extract(values, rows, col))
			print "<td>%s</td>" % formatted
		print "</tr>"
	print "</table>"

def main(argv):
	(opts, args) = getopt(argv, "o:t:f:")
	out = sys.stdout
	ticks = 20 # precision of 1/tick
	fraction = 0.95
	log = AMLog(args[0]).structure()
	print """<html>
	<head>
		<style>
			table,th,td {
				border: 1px solid #ccc;
			}
		</style>
	</head>
	<body>"""
	for dag in log.dags:
		print "<h2>",dag.name,"</h2>"
		process(dag)
	print "</body></html>"

if __name__ == "__main__":
	main(sys.argv[1:])
