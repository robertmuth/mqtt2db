## MQTT2DB - A simple DB persister for mqtt messages


* listens to all mqtt messages from the specified broker and stores them
  into a DB (currently sqlite)
* indended for situations where mqtt contain time series data
* expects topics to be structured like so source/host/metric-part1/metric-part2/...
* works well with collectd mqtt plugin
* avoids storing most data that has not changed
* provides a simple web interface to for debugging and analysis
* license: GPL 3



