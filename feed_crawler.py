import feedparser
import mysql.connector
import os.path
import yaml


class ConfigNotFoundError(Exception): pass


def read_yaml(filename):
    """

    :param filename:
    :return:
    """
    if not os.path.isfile(filename):
        raise ConfigNotFoundError("Could not find the file with the name: %s" % filename)

    yaml_doc = {}

    with open(filename, 'r') as f:
        yaml_doc = yaml.load(f)

    return yaml_doc


class Article:
    """
    Represents an article that is found within an RSS feed, and includes the functions necessary to determine if this
    is a new article, or if this is something that needs to be saved to the database.  It also includes the functions
    necessary to handle the database interactions.
    """
    def __init__(self, title, link, summary, published_date):
        self.title = title
        self.link = link
        self.summary = summary
        self.published_date = published_date

    def save(self):
        print "[Article.save] :: invoked."

        try:
            db_conn = mysql.connector.connect(**db_config)
            cursor = db_conn.cursor()
            query = ("INSERT INTO story (link, title, summary, published_date) VALUES (%s, %s, %s, %s)")
            data_article = (self.link, self.title, self.summary, self.published_date)
            cursor.execute(query, data_article)
            return_value = True
        except mysql.connector.Error as error:
            print "[save] :: error number=%s" % error.errno
            print "[save] :: error=%s" % error
            return_value = False
        else:
            db_conn.commit()
            cursor.close()
            db_conn.close()
        return return_value

    def exists(self):
        print "[exists] :: invoked."

        try:
            db_conn = mysql.connector.connect(**db_config)
            cursor = db_conn.cursor()
            query = "SELECT id FROM story WHERE link='%s'" % self.link
            cursor.execute(query)
            out = cursor.fetchall()

            if len(out) >= 1:
                return_value = True
            else:
                return_value = False

        except mysql.connector.Error as error:
            print "[exists] :: error number=%s" % error.errno
            print "[exists] :: error=%s" % error
            return_value = False
        else:
            db_conn.close()

        return return_value


def process_feed(feed_url):
    """
    Takes in the URL of an RSS feed and leverages the feedparser API to pull down the data and sub-functions to
    actually save the new stories into the central Daily News database table.
    :param feed_url: The URL of the RSS feed.
    :return: None.
    """
    print "[process_feed] :: invoked, feed=%s" % feed_url
    feed_contents = feedparser.parse(feed_url)
    try:
        feed_title = feed_contents.feed.title
    except AttributeError as ae:
        print "[process_feed] :: AttributeError found=%s" % ae
        feed_title = feed_url

    print "[process_feed] :: Processing Feed {%s} ... " % feed_title
    process_entries(feed_contents.entries)
    return None


def process_entries(feed_entries):
    """
    Handles the parsing of a specific entry within a feed, converting it into an article object, and then using a
    sub-function to determine whether or not the article should be saved into the Daily News Database.
    :param feed_entries:
    :return:
    """
    print "[process_entries] :: invoked, entries={%d}" % len(feed_entries)
    for entry in feed_entries:
        article = Article(entry.title, entry.link, entry.description, entry.published_parsed)
        does_exist = article.exists()
        if not does_exist:
            article.save()
        else:
            print "[process_entries] :: Article with link {%s} exists already, skipping." % article.link
    return None


print "IlliniBoard.com Daily News Feed Crawler Starting Up ..."

config = read_yaml('db_config.yml')
db_config = config.get('database').get('development')

print "Database Configuration: %s" % db_config

with open('feed_list.txt') as feed_list:
    feeds = feed_list.readlines()
    feeds = [feed.rstrip('\n') for feed in feeds]

for url in feeds:
    process_feed(url)