
import json
import random
import time

import requests
from bs4 import BeautifulSoup

NUM_INITIALIZATIONS = 1


def find_values(id, my_json):
    results = []

    def _decode_dict(a_dict):
        try:
            results.append(a_dict[id])
        except KeyError:
            pass
        for val in a_dict.values():
            if isinstance(val, dict):
                _decode_dict(val)
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        _decode_dict(item)
        return a_dict

    _decode_dict(my_json)
    return results


def fetch_product_listings():
    # The URL you want to scrape
    url = "https://www.depop.com/search/?q=lip+service&sort=newlyListed"

    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content of the page with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # print out all the HTML content of the page
    # print(soup.prettify())

    # Find the script tag with type="application/json"
    script_tag = soup.find('script', {'type': 'application/json'})

    # The contents of the script tag will be a string of JSON, which you can parse with the json module
    data = json.loads(script_tag.string)
    product_listings = find_values('products', data)

    # merge all arrays together from product_listings
    return [item for sublist in product_listings for item in sublist]


def initialize_ids(ids):
    # get all listing ids from product_listings and add to ids set
    for i in range(0, NUM_INITIALIZATIONS):
        print(f"Initializing ids... {i + 1}/{NUM_INITIALIZATIONS}")
        for product_listing in fetch_product_listings():
            ids.add(product_listing['id'])


def main():
    ids = set()
    initialize_ids(ids)
    while True:
        for listing in fetch_product_listings():
            if listing['id'] in ids:
                continue
            ids.add(listing['id'])
            title = listing['slug']
            size = listing['sizes'][0]
            price = round(float(listing['price']['priceAmount']) + float(listing['price']['nationalShippingCost']), 2)
            image_link = listing['preview']['150']

            print(f"New listing! Title: {title} size: {size} price: ${price} image_link: {image_link}")
        # get random amount between 10 and 30.
        # This will be the amount of seconds to wait before checking for new listings
        sleep_interval_seconds = random.randint(10, 30)
        print(f"Waiting {sleep_interval_seconds} seconds before checking for new listings...")
        time.sleep(sleep_interval_seconds)


if __name__ == "__main__":
    main()
