import feedparser, opengraph, mysql.connector, os.path, yaml
from util.amazon_s3 import download_image, upload_file_to_s3
from HTMLParser import HTMLParser
from PIL import Image


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

config = read_yaml('config.yml')
db_config = config.get('database').get('development')


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
    def __init__(self, title, link, summary, published_date, site_id, image_url=""):
        self.title = strip_tags(title)
        self.link = link
        self.summary = strip_tags(summary)
        self.published_date = published_date
        self.site_id = site_id

        # Use the Open Graph to pull what the site actually wants as the description and the image.
        open_graph_data = opengraph.OpenGraph(url=self.link)
        if open_graph_data.is_valid():
            if open_graph_data.get('description'):
                self.summary = open_graph_data.get('description')

            if open_graph_data.get('image'):
                self.image_url = open_graph_data.get('image')
        else:
            self.image_url = image_url

    def save(self):
        try:
            db_conn = mysql.connector.connect(**db_config)
            cursor = db_conn.cursor()
            query = ("INSERT INTO story (link, title, summary, published_date, associated_site, thumbnail_image_url) \
                        VALUES (%s, %s, %s, %s, %s, %s)")
            data_article = (self.link, self.title, self.summary, self.published_date, self.site_id, self.image_url)
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

    def save_image_to_s3(self):
        """
        Downloads an image based on the URL within the Article to the local system, turns it into a thumbnail image
        and then saves it into the S3 Bucket set up for the account.
        :return: s3_file_name: The file name of the image.
        """
        # TODO: Handle exceptions in the Download / Upload Process
        local_file_path = download_image(self.image_url)
        if not local_file_path == '':
            thumbnail_file, extension = os.path.splitext(local_file_path)
            print "[generate_thumbnail] :: local_file_path, extension=[%s, %s]" % (thumbnail_file, extension)
            img = Image.open(local_file_path)
            img_size = 400, 400
            img.thumbnail(img_size)
            img.save(thumbnail_file + ".thumbnail" + extension, "JPEG", quality=90)
            local_file_path = thumbnail_file + ".thumbnail" + extension
        s3_file_name = ""
        if not local_file_path == '':
            s3_file_name = upload_file_to_s3(local_file_path)

        self.image_url = s3_file_name
        return None


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
    print "[process_feed] :: url=%s" % feed_url
    feed_contents = feedparser.parse(feed_url)
    feed_list_size = len(feed_contents.entries)
    saved_count = 0
    for entry in feed_contents.entries:
        article = Article(entry.title, entry.link, entry.description, entry.published_parsed, site_id)
        does_exist = article.exists()
        if not does_exist:
            article.save_image_to_s3()
            article.save()
            saved_count += 1

    return feed_list_size, saved_count

print "IlliniBoard.com Daily News Feed Crawler Starting Up ..."

print "Database Configuration: %s" % db_config

try:
    db_conn = mysql.connector.connect(**db_config)
    cursor = db_conn.cursor()
    query = "SELECT * FROM daily_news_site_list"
    cursor.execute(query)

    for (site_id, url, site_name) in cursor:
        print "Processing %s at %s" % (site_name, url)
        feed_status = process_feed(url, site_id)
        print "Processed %d articles, saved %d." % (feed_status[0], feed_status[1])

except mysql.connector.Error as error:
        print "error number=%s" % error.errno
        print "error=%s" % error

else:
    db_conn.close()
