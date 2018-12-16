"""
AID ... XML processing tool
Adds to BigQuery Table and Posts to content collector endpoint
"""
import logging
import math
import re
import json
from os import environ
import xml.etree.ElementTree as ET
import threading
from threading import Thread
import datetime
from google.cloud import bigquery
import requests
from flask import Flask


APP = Flask(__name__)

# Logging
LOGGER = logging.getLogger()
HANDLER = logging.StreamHandler()
FORMATTER = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)

# Auth
KEY = environ['KEY']

class DocumentProcess:
    """
    XML to GCS
    """

    def __init__(self, filename):

        self.existing_products = []
        self.filename = filename
        self.xml_data = requests
        self.is_new = []
        self.pre_batched = []
        self.gcs = bigquery.Client()
        self.session_timestamp = str(datetime.datetime.now())


    def send_to_content(self):
        """
        Send to content add endpoint
        """

        # Product Object Values that are sent as-is
        magic_words = ["version", "url", "sku"]

        white_list = [
            "brand", "google_product_category",
            "product_type", "gender", "color",
            "category", "sub_category", "basic_type"
            ]

        for product in self.pre_batched:

            # Transform values to keys and prepend a string to
            # leverage the match() lql function, and set value to keys as 1
            payload = {
                "ca." + str(key) + " - " + str(value): 1
                for key, value in product.items()
                if key in white_list
                }

            for word in magic_words:
                payload[word] = product[word]

            LOGGER.debug(json.dumps(payload))

            create = requests.post(
                '...',
                data=json.dumps(payload),
                headers={
                    'Authorization': KEY,
                    'Content-type': 'application/json'
                    },
                timeout=10)
            try:
                create_response = create.json()

            except Exception as error_desc:
                create_response = {'status': 999}
                LOGGER.error("An exception has occured: %s", error_desc)

            LOGGER.debug(str(create_response))

            if create_response['status'] > 299:
                LOGGER.error(str(create_response))


    def build_query(self, is_new_set):
        """
        Build query against product table
        """

        table_name = "..."

        schema = self.gcs.get_table(table_name).schema

        schema_field_names = [field.name for field in schema]

        all_new_rows = []

        # Format product objects into list with item index matching schema index
        for product in is_new_set:

            indexed = ["\"\"" for field in schema_field_names]

            for prop in product:

                for index, field in enumerate(schema_field_names):

                    if prop == field:
                        try:
                            indexed[index] = "\"" + product[prop] + "\""
                            break

                        except IndexError:
                            LOGGER.warning("""could not find schema index in row
                                            formatter at position %s""", str(index))


            formatted = "(" + ",".join(indexed) + ")"

            all_new_rows.append(formatted)

        # Build out query text
        query = "INSERT INTO `" + table_name + "` ("
        query += ", ".join(schema_field_names) + ") "
        query += "VALUES"
        query += ", ".join(all_new_rows)

        LOGGER.debug(query)

        return query


    def update_product_table(self, query):
        """Post Query to GCS"""
        update_query_job = self.gcs.query(query)

        # Query Result
        return update_query_job.result()


    def get_existing_products(self):
        """
        Get existing SKUs from product table
        """

        existing_sku = ('SELECT sku FROM `...')

        LOGGER.debug("Query: %s", existing_sku)

        # Initiate Query
        read_query_job = self.gcs.query(existing_sku)

        # Query Result
        read_rows = read_query_job.result()

        for row in read_rows:
            self.existing_products.append(row['sku'])

        LOGGER.info("Found %i existing products.", len(self.existing_products))

        if len(self.existing_products) == 0:
            LOGGER.error("Did not read products from table")
            return False

        return True


    def get_xml_document(self):
        """Get XML File"""
        LOGGER.info("Obtaining %s...", self.filename)

        try:
            self.xml_data = requests.get(
                '...'
                + self.filename, timeout=10)

            self.xml_data.raise_for_status()

        except requests.exceptions.HTTPError:
            LOGGER.error(str(self.xml_data.status_code))

        except requests.exceptions.Timeout:
            LOGGER.error("The attempt to get XML file timed out.")

        except ConnectionError:
            LOGGER.error("A connection error has occurred. Please try again.")

        if self.xml_data.status_code < 299:
            LOGGER.info("XML document downloaded.")
            return True

        LOGGER.error("XML document %s could not be accessed. ", self.filename)
        return False

    def process_xml(self):
        """iterate through XML document, sort into keep and dump"""

        # Parse raw XML
        tree = ET.fromstring(self.xml_data.text)

        # List of products not sent to BQ, they are already there
        is_not_new = []

        for index, product in enumerate(tree[0].findall('item')):

            if index % 10000 == 0:
                LOGGER.info("processing product index: %s", str(index))

            prod_obj = {
                'new': False,
                'version': "4",
                'date_added': self.session_timestamp
                }

            for prop in product:

                tagname = prop.tag.replace("{http://base.google.com/ns/1.0}", "")

                tagtext = re.sub('(\n|\")', '', prop.text)

                if tagname == "id":

                    # Set flag to new if id is not in the table already
                    prod_obj['new'] = bool(tagtext not in self.existing_products)

                    # Special Transform for id -> sku, for schema simplicity
                    try:
                        prod_obj["sku"] = tagtext

                    except Exception as ex:
                        LOGGER.error("Error in appending product id/sku to object: %s", str(ex))

                # Special Transform for link -> url, for schema simplicity
                if tagname == "link":
                    try:
                        santized_text = tagtext
                        prod_obj["url"] = santized_text.strip()

                    except Exception as ex:
                        LOGGER.error("Error in appending product link/url to object: %s", str(ex))

                # Append All other properties to the object
                else:
                    try:
                        santized_text = tagtext
                        prod_obj[tagname] = santized_text.strip()

                    except Exception as ex:
                        LOGGER.error("Error in appending product property to object: %s", str(ex))

            # Keep products to post to BQ in pre_batched
            if prod_obj['new']:
                self.pre_batched.append(prod_obj)

            else:
                is_not_new.append(prod_obj)


        LOGGER.info("found %s new products and %s existing products",
                    len(self.pre_batched), len(is_not_new))

    def split_into_batches(self):
        """
         Create Batches of products to send to BQ to reduce query size
        """

        # maximum of 200 products per query, keeps query char length under 256k max.
        number_of_batches = math.ceil(len(self.pre_batched) / 200)

        number_of_products = len(self.pre_batched)

        LOGGER.debug("number of products total %s:", str(number_of_products))

        # Determine number of products per group,
        # set to 1 in case less than number of batches
        if  number_of_products >= number_of_batches:
            group_size = int(round(number_of_products / number_of_batches))

        else:
            group_size = 1

        LOGGER.debug("Number of products per group: %s", str(group_size))

        # Create empty batches, with group size indicating number of
        # products per batch
        while number_of_products > 0:

            if number_of_products > group_size:
                self.is_new.append({"group_size":group_size,
                                    "products": []
                                    })
                number_of_products -= group_size

            if number_of_products <= group_size:
                self.is_new.append({"group_size":number_of_products,
                                    "products": []
                                    })
                number_of_products = 0

        LOGGER.debug("Number of groups created: %s", len(self.is_new))

        if LOGGER.debug:
            for index, group in enumerate(self.is_new):
                LOGGER.debug("Group %s size: %s", str(index), str(group['group_size']))

        # Split out pre-batched products into groups
        for group in self.is_new:

            # Add products if less than or equal to number needed in batch
            for index, product in enumerate(self.pre_batched):
                if index + 1 <= group["group_size"]:
                    group["products"].append(product)

            #Now remove the added products from the pre_batched list
            for product in group["products"]:
                self.pre_batched.remove(product)



def run_convert_and_update(filename):
    """Run"""
    xml = DocumentProcess(filename)

    # Download file and process
    if xml.get_xml_document():

        xml.get_existing_products()
        xml.process_xml()

    # Send to table if products to send
    if xml.pre_batched:

        xml.send_to_content()

        LOGGER.info("Products sent to collection endpoint.")

        xml.split_into_batches()

        for group in xml.is_new:
            query = xml.build_query(group['products'])
            xml.update_product_table(query)

        LOGGER.info("Table update complete.")

    else:
        LOGGER.info("No new products found.")


@APP.route('/')
def base():
    return 'XML Processing Service'


@APP.route('/go/<file_name>', methods=['GET', 'POST'])
def convhandler(file_name):
    thread = Thread(target=run_convert_and_update,
                    args=(file_name,),
                    name="Processing For File: " + file_name)
    thread.start()
    return 'XML processing request received.'


@APP.route('/checkthreads')
def check_threads():
    LOGGER.info("Checking running threads...")
    check_thread = []
    for thread in threading.enumerate():
        check_thread.append((thread.name, thread.is_alive()))
    return str(check_thread)


@APP.errorhandler(500)
def server_error(e):
    LOGGER.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    APP.run(host='127.0.0.1', port=8080, debug=True)
