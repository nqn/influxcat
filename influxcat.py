import json
import urllib
import argparse
import time
import sys
import string
import datetime
from prettytable import PrettyTable

# Exceptional import of matplotlib. In case of server environment does not
# have GKT interface. This import throws then Runtime Exceptions.
matplotlib_exception = None
plt = None
md = None
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as md
except RuntimeError, e:
    matplotlib_exception = "Gtk is not allowed in this terminal:{}".format(e.message)

def json_from_url(url):
    while True:
        try:
            response = urllib.urlopen(url)
            data = response.read()

            try:
                out = json.loads(data)
                return out
            except ValueError:
                print "Error: %s" % data
                sys.exit(1)

        except IOError:
            print "Could not load %s: retrying in one second" % url
            time.sleep(1)
            continue


def query(influx_endpoint, query):
    data = urllib.quote_plus(query)
    url = '%s&q=%s' % (influx_endpoint, data)
    return json_from_url(url)


def format(result, sep):
    cols = result[0]["columns"]
    print '#' + string.join(cols, sep)
    for point in result[0]['points']:
        line = []
        for value in point:
          line.append(str(value))
        print string.join(line, sep)


def format_plot(result, plot_by):
    # TODO(nnielsen): Support comma seperated plot_by

    cols = result[0]["columns"]
    ts_name = result[0]["name"]
    
    # Find index of 'value'
    time_index = None
    value_index = None
    plot_by_index = None
    i = 0
    for col in cols:
        if col == 'time':
            time_index = i

        if col == 'value':
            value_index = i
          
        if col == plot_by:
            plot_by_index = i

        i += 1

    if value_index is None:
        print "'value' not found in dataset"
        sys.exit(1)

    if time_index is None:
        print "'time' not found in dataset"
        sys.exit(1)

    series_y = {}
    series_x = {}

    for point in result[0]['points']:
        value = point[value_index]
        timestamp = datetime.datetime.utcfromtimestamp(float(point[time_index]) / 1000)

        series_name = 'default'
        if plot_by_index is not None:
            series_name = point[plot_by_index]

        if series_name not in series_y:
            series_y[series_name] = []
            series_x[series_name] = []

        series_y[series_name].append(value)
        series_x[series_name].append(timestamp)
 
    for serie, y_data in series_y.iteritems():
        x_data = series_x[serie]
        plt.plot(x_data, y_data, label=serie)

    if plot_by_index is not None:
        plt.legend()

    plt.title(ts_name)
    plt.grid(True)
  
    x1,x2,y1,y2 = plt.axis()
    plt.axis((x1,x2, 0, y2))

    plt.xticks(rotation=25)

    ax=plt.gca()
    xfmt = md.DateFormatter('%H:%M:%S')
    ax.xaxis.set_major_formatter(xfmt)

    plt.gcf().subplots_adjust(bottom=0.20)

    timestamp = time.time()
    output_file = "influxcat-%s.pdf" % timestamp
    plt.savefig(output_file)

def main():
    parser = argparse.ArgumentParser(
        description='Nibbler collects statistics and metrics from a Mesos slave and push them to influxdb')

    parser.add_argument('--influxdb-host', default='localhost:8086', type=str, help='hostname and port for influxdb admin server')
    parser.add_argument('--influxdb-name', required=True, type=str, help='Database name to use')
    parser.add_argument('--influxdb-user', default='root', type=str, help='user for influxdb admin server')
    parser.add_argument('--influxdb-password', default='root', type=str, help='password for influxdb admin server')
    parser.add_argument('--output', default="pretty-print", type=str, help='Output format (pretty-print, tsv, csv or pyplot)')
    parser.add_argument('--plot-by', default=None, type=str, help='If pyplot is selected, this is the field the plotted series is seperated by')
    parser.add_argument("command", nargs=argparse.REMAINDER)

    args = parser.parse_args()
    if args.output == 'pyplot' and matplotlib_exception is not None:
        print matplotlib_exception
        print "Please provide another output option."
        sys.exit(1)

    influx_endpoint = 'http://%s/db/%s/series?u=%s&p=%s' % (
        args.influxdb_host, args.influxdb_name, args.influxdb_user, args.influxdb_password)

    if len(args.command) < 1:
        print "Empty query"
        sys.exit(1)

    result = query(influx_endpoint, string.join(args.command, ' '))

    if len(result) < 1:
        print "Empty result"
        sys.exit(1)

    if args.output == 'pretty-print':
        cols = result[0]["columns"]
        t = PrettyTable(cols)
        for point in result[0]['points']:
            t.add_row(point)
        print t

    elif args.output == 'tsv':
        format(result, '\t')

    elif args.output == 'csv':
        format(result, ',')

    elif args.output == 'pyplot':
        format_plot(result, args.plot_by)

if __name__ == "__main__":
  main()
