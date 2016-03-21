import urllib, boto3, os.path, yaml
from os import rename
from urlparse import urlparse

__author__ = "John Brumbaugh"

with open('./config.yml', 'r') as f:
    config = yaml.load(f)


def get_bucket_url():
    bucket_url = "%s/%s" % (config.get('amazon').get('s3').get('url'),
                            config.get('amazon').get('s3').get('bucket_name'))
    return bucket_url


def download_image(image_url, link=""):
    """
    Downloads the image from a specified URL, saves it into the generic downloads folder, and will then provide back
    the full file location as to where the image is on the local machine.
    :param image_url:
    :param link:
    :return:
    """
    print "[download_image] :: invoked, image_url=%s" % image_url
    parsed_url = urlparse(image_url)
    if parsed_url.netloc == '':
        print "[download_image] :: Skipping Image Download as URL doesn't point to a remote location."
        parsed_link = urlparse(link)
        if parsed_link.netloc == '':
            return ""
        else:
            image_url = "%s://%s%s" % (parsed_link.scheme, parsed_link.netloc, image_url)
            print "[download_image] :: Changed the image_url to %s" % image_url

    save_location = config.get('local').get('file').get('image_download_location') % image_url.split('/')[-1]
    file_location, headers = urllib.urlretrieve(image_url, save_location)
    content_type = headers.get('Content-Type')
    if content_type == 'image/jpeg':
        if (".jpg" or ".jpeg") not in file_location:
            new_location = "%s.jpg" % file_location
            print "[download_image] :: renaming file to %s" % new_location
            rename(file_location, new_location)
            file_location = new_location
    return file_location


def upload_file_to_s3(file_path, file_name=None):
    """
    Uploads an image in the S3 bucket defined in the overall configuration file.
    :param file_path: The path of the image on the local hard drive.
    :param file_name: The name of the image to set within the S3 bucket.
    :return: The URL of the image as stored within the S3 bucket.
    """
    if file_name is None:
        file_name = file_path.split('/')[-1]

    s3_client = boto3.client('s3')
    s3_client.upload_file(file_path, config.get('amazon').get('s3').get('bucket_name'), file_name)
    s3_url = "%s/%s" % (get_bucket_url(), file_name)
    return s3_url
