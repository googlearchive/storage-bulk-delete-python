#!/usr/bin/env python
import sys
import gflags
import httplib2
from multiprocessing import Process, Queue
from threading import Thread

from apiclient.discovery import build
from apiclient.http import BatchHttpRequest
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage
from oauth2client.tools import run

from config import *
FLAGS = gflags.FLAGS
FLAGS.auth_local_webserver = False

FLOW = OAuth2WebServerFlow(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    scope='https://www.googleapis.com/auth/devstorage.full_control',
    user_agent='Bulk remover 0.0.0.1')

# If the Credentials don't exist or are invalid, run through the native client
# flow. The Storage object will ensure that if successful the good
# Credentials will get written back to a file.
storage = Storage('.credentials.dat')
credentials = storage.get()
if credentials is None or credentials.invalid == True:
  credentials = run(FLOW, storage)

def segment(size, items):
  cuts = range(0, len(items), size)
  for start, end in zip(cuts, cuts[1:]):
    yield items[start:end]
  yield items[cuts[-1]:]

def batch_remove(objects, service, http):
  def cb(req_id, response, exception):
    if exception:
      print req_id, exception

  batch = BatchHttpRequest()
  for obj in objects:
    batch.add(service.objects().delete(bucket=BUCKET, object=obj), callback=cb)
  return batch.execute(http=http)

q = Queue()

def job_handler():
  # Create an httplib2.Http object to handle our HTTP requests and authorize it
  # with our good Credentials.
  http = httplib2.Http()
  http = credentials.authorize(http)

  # Build a service object for interacting with the API.
  service = build(serviceName='storage', version='v1beta1', http=http,
                  developerKey=DEVKEY)
  while not q.empty():
    objects = q.get()
    batch_remove(objects, service, http)


if __name__ == '__main__':
  if len(sys.argv) != 2:
    sys.exit('usage: ./bulk-remove.py <target-list>\n\t'
             'where target-list is a file full of filenames')

  # Populate the queue
  with open(sys.argv[1]) as input_file:
    object_names = [ x.split('/')[-1].strip() for x in input_file.readlines() ]

  # Calculate how many requests go inside each bulk request
  JOB_SIZE = min(MAX_JOB_SIZE, len(object_names)/PROCESS_COUNT) or 1
  for job in segment(JOB_SIZE, object_names):
    q.put(job)

  processes = []
  for i in range(PROCESS_COUNT):
    p = Process(target=job_handler)
    p.start()
    processes.append(p)

  for p in processes:
    p.join()
