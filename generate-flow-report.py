## Attempts to generate a simple report of a flow stored in InfluxDB.
# Input: src_host (currently limited to br033 and br034), dest_ip
# Output: Text report with static flow metrics and statistics of dynamic flow metrics.
# Author: Bryan Learn

## Program workflow
# - Given: Monitored host we want data from and destination IP of flow we want report on.
# - Determine database to use (which host, i.e. br033)
# - Query InfluxDB to find flowID for flow of interest (determined by src_host and dest_ip); Store flowID
#   - Currently grabs the most recent flowID for the src,dest pair. The start time should be used to find the correct flowID.
# - Query for all(most?) data related to the flowID
# - Generate report using the data
#   - Header with static metrics: FlowID, IP 5-tuple, Start/End time & duration
#   - Body with statistics of dynamic metrics: Total ReTrans, Avg RTT, Avg bandwidth, Total data In/Out

import sys
import requests
import urllib
import json
import argparse
import socket
from dateutil.parser import parse

def main():
    ## Setup CLI Arguments
    parser = argparse.ArgumentParser(description='Attempts to generate a simple report of a flow stored in InfluxDB.')
    parser.add_argument('host', action='store', help='Monitored host to query data about')
    parser.add_argument('dest_ip', action='store', help='Destination IP of flow from host')
    parser.add_argument('--flow_id', action='store', help='ID for flow from host')
    
    opts = parser.parse_args()
    
    # Maps shorthand host name to full InfluxDB database name
    DB_NAME = {'br033': "ALL_PSC_br033.dmz.bridges.psc.edu", 'br034': "ALL_PSC_br034.dmz.bridges.psc.edu"}
    
    ## Process args
    fail = False
    host = None
    dest_ip = None
    flowID = None

    # - check for valid dest_ip
    try:
        socket.inet_aton(opts.dest_ip)
        dest_ip = opts.dest_ip
    except:
        fail=True
        print("Invalid IP address: {0}\n".format(opts.dest_ip))
    
    # - check if flowID is set
    if opts.flow_id is not None:
        flowID = opts.flow_id
        fail=False # if IP was invalid, we no longer care because we have the flowID already
    
    # - check for valid host
    if opts.host in DB_NAME.keys():
        host = opts.host
    else:
        fail=True
        print("Invalid host: {0}. Must be one of: {1}\n".format(opts.host, DB_NAME.keys()))


    if(fail):
        parser.print_help()
        sys.exit(0)

    ## Setup parameters for request to server
    # - InfluxDB host URL
    url = "http://hotel.psc.edu:8086/query?"
    # - database name
    db = DB_NAME['br033']
    # - Creds
    user = raw_input("Username: ")
    pwd = raw_input("Password: ")

    if flowID is None: # if we did not get FlowID, query for it
        # - Query
        query = "SELECT flow FROM dest_ip where value='{0}' limit 1".format(dest_ip)
        # - Put params into json; urlencode them; add them to host url
        ReqParams = {'u': user, 'p': pwd, 'q': query, 'db': db}
        ReqUrl = url+urllib.urlencode(ReqParams)

        #print(ReqUrl)

        ## Make request for flowID
        DBResponse = requests.get(ReqUrl, verify=True)
        #print("Request Status: {0}\n".format(DBResponse.status_code))
        
        ## Process Server response for flowID
        if(DBResponse.ok):
            # - Load response data into dict
            flowData = json.loads(DBResponse.content)

            # - Check values for status
            flowData['error'] = flowData.get('error', None)
            flowData['results'] = flowData.get('results', None)

            if(flowData['results'] is None and flowData['error'] is not None):
                # -- Empty result
                print("No data was found for host {0} and destination IP {1}\n").format(host, dest_ip)
                sys.exit(0)
            elif(flowData['error'] is not None):
                # -- InfluxDB query error
                print("InfluxDB returned an error on the query: {0}\n").format(flowData['error'])
                sys.exit(0)
            else:
                # -- No errors, data returned
                flowID = flowData['results'][0]['series'][0]['values'][0][1]
                print("FlowID is {0}".format(flowID))

            #print("Response contains {0} properties\n".format(len(flowData)))
            #for key in flowData:
            #    print("{0}: {1}".format(key, flowData[key]))
        else:
            print("Request for FlowID failed\n")
            DBResponse.raise_for_status()
    
    # Should have flowID by this point

    ## Make request for flowID's dataset
    # - Build new query
    staticMetrics = ["src_ip", "src_port", "dest_ip", "dest_port", "command", "StartTime"] # don't need analyzed
    dynamicMetrics = ["CurCwnd", "CountRTT", "CurMSS", "CurRTO", "DataOctetsIn", "DataOctetsOut", "DupAckEpisodes"]
    metrics = staticMetrics + dynamicMetrics
    query = ""
    for m in metrics: # build separate query string for each metric
        query += "SELECT value FROM {0} WHERE flow='{1}';".format(m, flowID)

    # - Update params and encode params into url
    #query = "SELECT value FROM /.*/ WHERE flow='{0}'".format(flowID)
    ReqParams = {'u': user, 'p': pwd, 'q': query, 'db': db}
    ReqUrl = url+urllib.urlencode(ReqParams)
    # - Send request
    DBResponse = requests.get(ReqUrl, verify=True)

    ## Process Server response for dataset
    flowData = None; # wipe previous response
    if(DBResponse.ok):
        # - Load response data into dict
        flowData = json.loads(DBResponse.content)

        # - Check values for status
        flowData['error'] = flowData.get('error', None)
        flowData['results'] = flowData.get('results', None)

        if(flowData['results'] is None and flowData['error'] is not None):
            # -- Empty result
            print("No data was found for flowID {0}\n").format(flowID)
            sys.exit(0)
        elif(flowData['error'] is not None):
            # -- InfluxDB query error
            print("InfluxDB returned an error on the query: {0}\n").format(flowData['error'])
            sys.exit(0)
        else:
            # -- No errors, data returned
            #print(flowData)

            passedMetrics = [None]*len(metrics) # Parallel list to metrics. Name string means passed, None means failed.
            print("\nStatic Metrics:")
            # check if all static metrics queries returned a result (check for empty results)
            for indx in range(len(staticMetrics)):
                s = flowData['results'][indx].get('series', None) # check value of series, set to None to indicate empty result
                if s is not None: # data was returned
                    passedMetrics[indx] = flowData['results'][indx]['series'][0]['name']
                    #print("{0}: {1}".format(flowData['results'][indx]['series'][0]['name'], flowData['results'][indx]['series'][0]['values'][0][1]))
                #else: # data was not returned
                    #print("{0}: Null".format(metrics[indx]))
            # Print Header
            print("Source: [IP: "),
            if "src_ip" in passedMetrics:
                print("{0}, ".format(flowData['results'][metrics.index("src_ip")]['series'][0]['values'][0][1])),
            else:
                print("Null, "),
            
            print("Port: "),
            if "src_port" in passedMetrics:
                print("{0}]".format(flowData['results'][metrics.index("src_port")]['series'][0]['values'][0][1]))
            else:
                print("Null]")
            
            print("Destination: [IP: "),
            if "dest_ip" in passedMetrics:
                print("{0}, ".format(flowData['results'][metrics.index("dest_ip")]['series'][0]['values'][0][1])),
            else:
                print("Null, "),
           
            print("Port: "),
            if "dest_port" in passedMetrics:
                print("{0}]".format(flowData['results'][metrics.index("dest_port")]['series'][0]['values'][0][1]))
            else:
                print("Null]")

            print("Command: "),
            if "command" in passedMetrics:
                print("{0}".format(flowData['results'][metrics.index("command")]['series'][0]['values'][0][1]))
            else:
                print("Null")

            print("Duration: "),
            if "StartTime" in passedMetrics:
                dur = "NaN"
                start = flowData['results'][metrics.index("StartTime")]['series'][0]['values'][0][0]
                end = flowData['results'][metrics.index("StartTime")]['series'][0]['values'][len(flowData['results'][metrics.index("StartTime")]['series'][0]['values'])-1][0]
                sDate = parse(start)
                eDate = parse(end)
                diff = eDate - sDate
                diff = divmod(diff.days * 86400 + diff.seconds, 60)
                dur = "{0} minutes and  {1} seconds".format(diff[0], diff[1])
                print("{0} |{1} <-> {2}|".format(dur, start, end))
            else:
                print("Null")

            # Print Stats
            print("\nDynamic Metrics:")
            print("[Metric: Min / Max / Avg]")
            # check if all dynamic metrics queries returned a result (check for empty results)
            for indx in range(len(staticMetrics), len(metrics)):
                s = flowData['results'][indx].get('series', None) # check value of series, set to None to indicate empty result
                if s is not None: # data was returned
                    #print("{0}: {1}".format(flowData['results'][indx]['series'][0]['name'], flowData['results'][indx]['series'][0]['values'][0][1]))
                    #print("{0} Avg: {1}".format(flowData['results'][indx]['series'][0]['name'], sum(flowData['results'][indx]['series'][0]['values'][0][1])/len(flowData['results'][indx]['series'][0]['values'][0][1])))
                    vals = []
                    for i in flowData['results'][indx]['series'][0]['values']: # for each value of this metric
                        vals.append(i[1])
                    avgV = sum(vals)/len(vals)
                    minV = min(vals)
                    maxV = max(vals)
                    print("{0}: {1} / {2} / {3}".format(flowData['results'][indx]['series'][0]['name'], minV, maxV, avgV))
                else: # data was not returned
                    print("{0}: Null".format(metrics[indx]))
            #flowID = flowData['results'][0]['series'][0]['values'][0][1]
    else:
        print("Request for FlowID failed\n")
        DBResponse.raise_for_status()

    ## Cleanup and exit
    sys.exit(0)

if __name__ == "__main__":
    main()
