import sqlite3
import json
import time
import secrets

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
      include bool default 1,
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
      include bool default 1,
      foreign key (author) references users(id),
      constraint un unique(id)
   );''')
   c.execute('''create table usermentions (
      tweet integer,
      user integer
   );''')
   c.execute('''create table hashmentions (
      tweet integer,
      tag text
   );''')
   c.execute('''create table urlmentions (
      tweet integer,
      url text
   );''')
   c.execute('''create table following (
      source integer,
      target integer,
      added integer,
      removed integer,
      include integer default 1,
      constraint un unique (source, target),
      foreign key (source) references users(id),
      foreign key (target) references users(id));''')
   c.execute('''create table todo (
      id integer,
      kind string,
      other integer, 
      priority real,
      constraint un unique (id, kind));''')
   conn.commit()
   c.close()
except sqlite3.OperationalError:
   pass
try:
   c = conn.cursor()
   c.execute('''insert into users values 
         ( ?
         , ?
         , ?
         , ?
         , 0
         , ?
         , null
         , 1);''', (secrets.firstUser['id'], secrets.firstUser['name'], secrets.firstUser['followers'], secrets.firstUser['friends'], int(time.time()),));
   c.execute('''insert into todo values (?, "user", NULL, 1);''', (secrets.firstUser['id'], ));
   conn.commit()
except sqlite3.IntegrityError:
   pass

while True:
   try:
      tweetJson = str(raw_input())
      tweet = json.loads(tweetJson)
      user = tweet['user']
      c = conn.cursor()
      c.execute('''insert into users values 
            ( ?
            , ?
            , ?
            , ?
            , 0
            , ?
            , null
            , 1);''', (user['id'], user['screen_name'], user['followers_count'], user['friends_count'], int(time.time())))
      c.execute('''insert into todo values (?, "user", NULL, 1);''', (user['id'], ));
      conn.commit()
   except sqlite3.IntegrityError:
      continue

