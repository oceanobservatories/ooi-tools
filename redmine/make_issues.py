#!/usr/bin/env python

import csv
import os
import sys
import requests
import json

api_key_file = '.api_key'
#api_key_file = '.api_key_local'
redmine_file = '5_iad.csv'

def create_issue( api_key):

    redmine_ids = []

    if os.path.exists(redmine_file):

        f = open(redmine_file)

        for line in f:

            #change title as needed
            _subject = 'Implement %s Instrument Agent' % line.strip()
            print _subject

            data = {
            "issue": {
                    "project_id": 7,
                    "tracker_id": 2,
                    "subject": _subject,
                    "status_id": 1,
                    "priority_id": 2,
                    "done_ratio": 0,
                    "fixed_version_id": 14}}
            #this is hardcoded values for redmine
            #CHANGE THESE TO CUSTOMIZE REDMINE ISSUE CREATION
            #project_id 6 = Dataset Parser Development
            #           7 = Instrument Agent Development
            #           8 = Platform Agent Development
            #tracker_id 2 = Feature
            #status_id 3 = Resolved
            #          1 = New
            #priority_id 2 = Normal
            #fixed_version_id 8 = Group 1A (Target version)
            #                 9 = Group 1B
            #                 10 = Group 2A
            #                 11 = Group 2B
            #                 12 = Group 3
            #                 13 = Group 4
            #                 14 = Group 5


            r = requests.post('https://uframe-cm.ooi.rutgers.edu/issues.json?key=%s' % api_key,
                              verify=False, data=json.dumps(data), headers={'content-type': 'application/json'})
            print 'RESULT %r' % r

            #FOR TESTING PURPOSES ONLY
            # r = requests.post('http://localhost:8080/redmine/issues.json?key=%s' % api_key,
            #                   verify=False, data=json.dumps(data), headers={'content-type': 'application/json'})

            print 'Created Redmine Id: %r' % r.json()['issue']['id']
            redmine_ids.append(id)

        f.close()

    else:
        print 'Please create a file %s in the current directory containing redmine tickets to create' % redmine_file
        sys.exit(1)

    #generate output file for future use
    f = open("redmine_ids.csv", 'w')
    writer = csv.writer(f)
    writer.writerow(['id'])
    for id in redmine_ids:
        writer.writerow([id])

def create_subtask(api_key, parent_id, subject):



    data = {
            "issue": {
                    "project_id": 8,
                    "tracker_id": 7,
                    "subject": subject,
                    "status_id": 1,
                    "priority_id": 2,
                    "done_ratio": 0,
                    "fixed_version_id": 13,
                    "parent_issue_id": parent_id}}

    r = requests.post('https://uframe-cm.ooi.rutgers.edu/issues.json?key=%s' % api_key,
                      verify=False, data=json.dumps(data), headers={'content-type': 'application/json'})
    print 'RESULT %r' % r
    print 'Created Redmine Id: %r' % r.json()['issue']['id']


def main():

    if os.path.exists(api_key_file):
        api_key = open(api_key_file).read().strip()
    else:
        print 'Please create a file %s in the current directory containing your api key' % api_key_file
        sys.exit(1)

    #creating new issues
    #create_issue(api_key)

    #creating subtasks
    parent_ids = [1650,1651,1652,1653,1654,1655]

    for id in parent_ids:
        create_subtask(api_key, id, "Write IDD/IOS")
        create_subtask(api_key, id, "Code and Unit Test")
        create_subtask(api_key, id, "Development Integration")
        #create_subtask(api_key, id, "SW Qualification")
        create_subtask(api_key, id, "SW Code Review")



if __name__ == '__main__':
    main()
