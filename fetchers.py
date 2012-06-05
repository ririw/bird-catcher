import time
import logging

def username(user_id, conn):
   c = conn.cursor()
   result = c.execute('''select label from users where id = ?'''
         , (user_id,))
   return result.fetchone()[0]

class UserFetcher(object):
   def __init__(self, user_id):
      self.user_id = user_id
   def remove(self, conn):
         c = conn.cursor()
         c.execute("delete from todo where id=? and kind='user'; ", 
               (self.user_id,))
         conn.commit()
         c.close()
   def explore(self, tweepy, api, conn):
      friends = tweepy.Cursor(api.friends, id=self.user_id)
      numExplored = 0
      user = username(self.user_id, conn)
      logging.info("Exploring friends of %s" % user)
      for friend in friends.items():
         if numExplored > 99:
            break
         numExplored += 1
         #print ("adding friend of %s \t:\t%s" % (user, friend.screen_name))
         c = conn.cursor()
         c.execute("select * from users where id=?;",
               (friend.id,));
         r = c.fetchone() 
         if r == None:
            c.execute("select distanceFromSource from users where id=?", (self.user_id,))
            res = c.fetchone()
            distance = res[0]+1
            logging.debug("Inserting friend %d" % friend.id)
            c.execute("insert into users values (?, ?, ?, ?, ?, ?, NULL)",
                  (friend.id
                  , friend.screen_name
                  , friend.followers_count
                  , friend.friends_count
                  , distance
                  , int(time.time())))
            outLinks = 0
            logging.debug("Inserting user to do: %d" % friend.id)
            c.execute('''insert into todo values (?, ?, NULL, ?);''', (friend.id, "user", distance))
            
            #tweets = TweetsFetcher(friend.id)
            #tweets.explore(tweepy, api, conn)

            logging.debug("Inserting tweet todo with id=%d" % friend.id)
            c.execute('''insert into todo values (?, ?, ?, ?);''', 
                  (friend.id, "tweet", None, distance))
         else:
            c.execute("select distanceFromSource from users where id=?", (self.user_id,))
            res = c.fetchone()
            c.execute("update users set distanceFromSource=? where id=?", (res[0]+1, friend.id))

         conn.commit()
         c.close()
         c = conn.cursor()
         c.execute("select * from following where source=? and target=?",
               (self.user_id, friend.id))
         if c.fetchone() == None:
            c.execute("insert into following values (?, ?, ?, NULL)",
                  (self.user_id, friend.id, int(time.time())))
            #c.execute("update users set inLinks=(select inLinks from users where id=?)+1 where id=?", (friend.id, friend.id))
            #c.execute("update users set outLinks=(select outLinks from users where id=?)+1 where id=?", (self.user_id, self.user_id))
            c.execute('''select * from 
                     following as f left join 
                     users as u on 
                     u.id=f.source where f.target=?''', (self.user_id,))
            newMaxSel = c.fetchall()
            c.execute('''select min(u.distanceFromSource) from 
                     following as f left join 
                     users as u on 
                     u.id=f.source where f.target=?''', (self.user_id,))
            newMaxSel = c.fetchone()
            newMax = 0
            if newMaxSel[0] != None:
               newMax = newMaxSel[0]+1
            c.execute('''update users set distanceFromSource=? where id=?''', (newMax, self.user_id))
            conn.commit()
            c.close()
      c = conn.cursor()
      c.execute('''delete from todo where id=? and kind='user';''', (self.user_id, ))
      conn.commit()
      c.close()
   def __str__(self):
      return "User %s " % (self.user_id)

class TweetsFetcher(object):
   def __init__(self, user_id, before_id=None, clear_todo=True):
      self.user_id = user_id
      self.before_id = before_id
      self.clear_todo = clear_todo

   def remove(self, conn):
         c = conn.cursor()
         c.execute("delete from todo where id=? and kind='tweet'; ", 
               (self.user_id,))
         conn.commit()
         c.close()
   def explore(self, tweepy, api, conn):
      logging.info("Exploring tweets of user %d %s before tweet %s" %
         (self.user_id, username(self.user_id, conn), str(self.before_id)))
      if self.before_id:
         tweets = api.user_timeline(id=self.user_id, max_id=self.before_id)
      else:
         tweets = api.user_timeline(id=self.user_id)
      lastID = None
      logging.debug("Before %s" % self.before_id)
      for tweet in tweets:
         if tweet.id == self.before_id:
            logging.debug("Continuing")
            continue
         #if '#' in tweet.text or \
            #'@' in tweet.text or \
            #'http://' in tweet.text:
            #tweet = TweetFetcher(tweet.id)
            #tweet.explore(tweepy, api, conn)
            #lastID = tweet.id
         logging.debug("inserting tweet with id %s and author %s" % (tweet.id, self.user_id))
         c = conn.cursor()
         c.execute('''insert into tweets values (
            ?, -- id
            ?, -- author
            ?, -- text
            ?, -- added
            ?, -- tweeted
            ?, -- inReplyToUser
            ?, -- inReplyToTweet
            ?, -- retweet
            ?); -- retweets
            ''', (tweet.id 
              , self.user_id
              , tweet.text
              , time.time()
              , time.mktime(tweet.created_at.timetuple())
              , None
              , None
              , tweet.retweeted
              , tweet.retweet_count
            ))
         lastID = tweet.id
         logging.debug("LastID: %s" % lastID)
         conn.commit()
         c.close()
      c = conn.cursor()
      newPriority_fetch = c.execute(
            "select priority from todo where id=?", (self.user_id,))
      newPriority = c.fetchone()[0]*(1.2)
      c.close()
      c = conn.cursor()
      c.execute('''delete from todo where id=? and kind='tweet';''', (self.user_id, ))
      logging.debug("LastID: %s" % str(lastID))
      if (lastID):
         c.execute('''insert into todo values (?, ?, ?, ?);''', 
               (self.user_id, "tweet", lastID, newPriority))
      conn.commit()
      c.close()
   def __str__(self):
      return "Tweets of %s before ID: %s" % (self.user_id, self.before_id)

class TweetFetcher(object):
   def __init__(self, id):
      self.id = id
   def explore(self, tweepy, api, conn):
      logging.info("Exploring tweet with id %d" % self.id)
      tweet = api.get_status(self.id)
      c = conn.cursor()
      c.execute('''insert into tweets values (
         ?, -- id
         ?, -- author
         ?, -- text
         ?, -- added
         ?, -- tweeted
         ?, -- inReplyToUser
         ?, -- inReplyToTweet
         ?, -- retweet
         ?); -- retweets
      ''', ( tweet.id 
           , tweet.author.id
           , tweet.text
           , time.time()
           , time.mktime(tweet.created_at.timetuple())
           , None
           , None
           , tweet.retweeted
           , tweet.retweet_count
         ))
      lastID = tweet.id
      conn.commit()
      c.close()
   def __str__(self):
      return "Tweet with ID: %s" % self.id
