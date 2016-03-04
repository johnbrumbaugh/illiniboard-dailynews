import feedparser
import mysql.connector
import os.path
import yaml
from HTMLParser import HTMLParser


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


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


class Article:
    """
    Represents an article that is found within an RSS feed, and includes the functions necessary to determine if this
    is a new article, or if this is something that needs to be saved to the database.  It also includes the functions
    necessary to handle the database interactions.
    """
    def __init__(self, title, link, summary, published_date, site_id):
        self.title = strip_tags(title)
        self.link = link
        self.summary = strip_tags(summary)
        self.published_date = published_date
        self.site_id = site_id

    def save(self):
        try:
            db_conn = mysql.connector.connect(**db_config)
            cursor = db_conn.cursor()
            query = ("INSERT INTO story (link, title, summary, published_date, associated_site) VALUES (%s, %s, %s, %s, %s)")
            data_article = (self.link, self.title, self.summary, self.published_date, self.site_id)
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


def process_feed(feed_url, site_id):
    """
    Takes in the URL of an RSS feed and leverages the feedparser API to pull down the data and sub-functions to
    actually save the new stories into the central Daily News database table.
    :param feed_url: The URL of the RSS feed.
    :param site_id: The ID of the site which will be added to the Article when saved in the database.
    :rtype: (int, int)
    :return
        feed_list_size: The size of the full article list in the feed
        saved_count: The count of articles actually saved.
    """
    feed_contents = feedparser.parse(feed_url)
    feed_list_size = len(feed_contents.entries)
    saved_count = 0
    for entry in feed_contents.entries:
        article = Article(entry.title, entry.link, entry.description, entry.published_parsed, site_id)
        does_exist = article.exists()
        if not does_exist:
            article.save()
            saved_count += 1

    return feed_list_size, saved_count

print "IlliniBoard.com Daily News Feed Crawler Starting Up ..."

config = read_yaml('db_config.yml')
db_config = config.get('database').get('development')

print "Database Configuration: %s" % db_config

try:
    db_conn = mysql.connector.connect(**db_config)
    cursor = db_conn.cursor()
    query = "SELECT * FROM daily_news_site_list"
    cursor.execute(query)

    for (site_id, site_name, feed_url) in cursor:
        print "Processing %s at %s" % ( site_name, feed_url)
        feed_status = process_feed(feed_url, site_id)
        print "Processed %d articles, saved %d." % ( feed_status[0], feed_status[1])

except mysql.connector.Error as error:
        print "error number=%s" % error.errno
        print "error=%s" % error

else:
    db_conn.close()
