#!/bin/env python

import cgi
import cgitb
import sys

cgitb.enable()
form = cgi.FieldStorage()

if "url" not in form:
	print "Content-Type: text/html"
	print ""
	print """
<html>
<body>
<form method="POST">
AM LOG URL: <input type="textbox" name="url" value="" /><br/><br/>
ticks: <input type="textbox" name="ticks" value="100" /><br/>
deadline: <input type="textbox" name="deadline" value="95%" /><br/>
<input type="submit"/><br/>
</form>
</body>
</html>
	"""
	sys.exit(0)

url = form["url"].value

if not (url.startswith("http://") and url.endswith("?start=0")):
	print "Content-Type: text/html"
	print ""
	print """
<html>
<body>
Error! provided URL needs to start with http:// and end with syslog/?start=0
</body>
</html>
	""" 
	sys.exit(0)
	
ticks = 25
deadline = 95
if "ticks" in form:
	ticks = int(form["ticks"].value)
if "deadline" in form:
	deadline = int(form["deadline"].value.replace("%",""))

argv = ["-t %d" % ticks, "-f %d" % deadline, url]

from swimlane import main

print "Content-Type: image/svg+xml"
print ""

main(argv)
