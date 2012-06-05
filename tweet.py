#!/usr/bin/python
import tweepy
import sqlite3
import Queue
import time
import networkx as nx
import logging
logging.basicConfig(filename='fetcher.log',level=logging.DEBUG)
logger = logging.root
logger.addHandler(logging.StreamHandler())
from fetchers import *

def find_next():
   return find_next_hops_pre()

def find_next_hops_pre():
   logging.info("Searching")
   c = conn.cursor()
   c.execute("select t.id,t.kind,t.priority,t.other from todo as t order by t.priority asc")
   res = c.fetchone()
   logging.info( "Priority: %f" % res[2])
   if res[1] == "tweet":
      return TweetsFetcher(res[0], res[3])
   elif res[1] == "user":
      return UserFetcher(res[0])
   elif res[1] == "url":
      return URLFetcher(res[0])
   elif res[1] == "tag":
      return TagFetcher(res[0])
   else:
      raise Exception("Invalid todo kind") 
   conn.commit()
   c.close()
   logging.info("Done: " + str(res))
   return res

def remove_todo(id, kind):
   c = conn.cursor()
   c.execute("delete from todo where id=? and kind=?", (id, kind))
   conn.commit()
   c.close()

conn = sqlite3.connect('./graph.db')

try:
   c = conn.cursor()
   c.execute('''create table users (
      id integer,
      label text,
      followers integer,
      friends integer,
      distanceFromSource integer,
      added integer,
      removed integer,
      constraint un unique (id));''')
   c.execute('''create table tweets (
      id integer,
      author integer,
      text string,
      added integer,
      tweeted integer,
      inReplyToUser integer,
      inReplyToTweet integer,
      retweet bool,
      retweets integer,
      foreign key (author) references users(id),
      constraint un unique(id)
   );''')
   c.execute('''create table usermentions (
      tweet integer,
      user integer
   );''');
   c.execute('''create table hashmentions (
      tweet integer,
      tag text
   );''');
   c.execute('''create table urlmentions (
      tweet integer,
      url text
   );''');
   c.execute('''create table following (
      source integer,
      target integer,
      added integer,
      removed integer,
      constraint un unique (source, target),
      foreign key (source) references users(id),
      foreign key (target) references users(id));''')
   c.execute('''create table todo (
      id integer,
      kind string,
      other integer, 
      priority real,
      constraint un unique (id, kind));''');
   conn.commit()
   c.close()
except sqlite3.OperationalError as e:
   logging.exception(e)
   pass
jack = jack
try:
   c = conn.cursor()
   c.execute('''insert into users values 
         ( 20
         , 'jack'
         , 2003219
         , 1172
         , 0
         , ?
         , null);''', (int(time.time()),));
   c.execute('''insert into todo values (20, "user", NULL, 1);''');
   conn.commit()
except sqlite3.IntegrityError:
   pass
while True:
   try:
      toExplore = find_next();
      toExplore.explore(tweepy, conn)
   except tweepy.error.TweepError as exception:
      if exception.reason == u'Not authorized':
         logging.warn('Not authorized. User: %s' % str(toExplore))
         toExplore.remove(conn)
         continue
      elif exception.reason == u'Rate limit exceeded. Clients may not make more than 150 requests per hour.':
         logging.info("Waiting for counters to reset:")
         timeToWaitUntil = tweepy.api.rate_limit_status()['reset_time_in_seconds']
         while time.time() < timeToWaitUntil:
            logging.info( "Need to wait %02d minutes" % ((timeToWaitUntil-time.time())/60))
            time.sleep(60)
      else:
         logging.exception( exception.reason)
         raise exception
