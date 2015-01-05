#!/usr/bin/env python

import csv
import os
import traceback
import sys
import requests

api_key_file = '.api_key'

class Issue(object):
    def __init__(self, i):
        self.id = i.get('id')
        self.status = i.get('status', {}).get('name')
        self.priority = i.get('priority', {}).get('name')
        self.subject = i.get('subject')
        self.tracker = i.get('tracker', {}).get('name')
        self.assignee = i.get('assigned_to', {}).get('name')
        self.category = i.get('category', {}).get('name')
        self.status = i.get('status', {}).get('name')

    def __str__(self):
        return str(self.id)

def get_issue(issue, api_key):
    r = requests.get('https://uframe-cm.ooi.rutgers.edu/issues/%s.json?key=%s' % (issue, api_key), verify=False)
    issue = Issue(r.json()['issue'])
    return issue

def main():
    if os.path.exists(api_key_file):
        api_key = open(api_key_file).read().strip()
    else:
        print 'Please create a file %s in the current directory containing your api key' % api_key_file
        sys.exit(1)
    
    with open('issues.csv', 'w') as fh:
        writer = csv.writer(fh)
        writer.writerow(['id', 'status', 'priority', 'subject', 'tracker', 'assignee', 'category', 'status'])

        for issue in sys.argv[1:]:
            try:
                i = get_issue(issue, api_key)
                writer.writerow([i.id, i.status, i.priority, i.subject, i.tracker, i.assignee, i.category, i.status])
            except Exception as e:
                traceback.print_exc()

if __name__ == '__main__':
    main()