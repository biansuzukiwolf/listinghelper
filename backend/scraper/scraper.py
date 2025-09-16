import json
from collections import Counter
from datetime import datetime
from multiprocessing import Pool

import wget as wget
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
import time

import requests
from bs4 import BeautifulSoup

DEPOP_CATEGORY_BASE_URL = 'https://www.depop.com/category/'
# mens/tops subcategories: cardigans, t-shirts,  shirts, polo shirts, t-shirts, hoodies, sweatshirts
# mens/bottoms subcategories: jeans, sweatpants, shorts

# womens/tops subcategories: cardigans, sweatshirts, t-shirts, blouses, hoodies, sweaters, shirts
# womens/bottoms subcategories: jeans, sweatpants, pants, shorts, skirts
# womens/dresses (no subcats)
CATEGORY_URL_PATHS = [
	'mens/tops/sweatshirts', 'mens/tops/t-shirts', 'mens/tops/polo-shirts', 'mens/tops/hoodies',
	'womens/tops/sweatshirts', 'womens/tops/t-shirts', 'womens/tops/blouses', 'womens/tops/hoodies', 'womens/tops/sweaters',
	'mens/bottoms/jeans', 'mens/bottoms/shorts',
	'womens/bottoms/sweatpants', 'womens/bottoms/shorts', 'womens/bottoms/skirts',
	'womens/dresses'
]
EXTRA_URL_PATHS = [
	'search/?q=jeans&categories=10&sort=relevance', #womens jeans
]
CATEGORY_ROOTS = {'Womenswear', 'Menswear'}
CATEGORY_PRODUCT_URL_LIMIT = 200
DEPOP_BASE_URL = 'http://www.depop.com'
S3_BASE_DIR = 's3://custom-labels-console-us-east-1-f1536dd445/assets/images/'

PROXY_LIST = [
	'wmhzspws:sed3wrsjyu5e@154.21.11.196:5473',
	'wmhzspws:sed3wrsjyu5e@38.154.210.49:9170',
	'wmhzspws:sed3wrsjyu5e@154.37.193.5:5338',
	'wmhzspws:sed3wrsjyu5e@154.13.135.235:5517',
	'wmhzspws:sed3wrsjyu5e@184.174.57.74:5360',
	'wmhzspws:sed3wrsjyu5e@104.239.126.18:5286',
	'wmhzspws:sed3wrsjyu5e@154.38.34.234:5246',
	'wmhzspws:sed3wrsjyu5e@154.13.10.7:5599',
	'wmhzspws:sed3wrsjyu5e@154.12.140.104:5751',
	'wmhzspws:sed3wrsjyu5e@104.239.125.118:5504',
	'wmhzspws:sed3wrsjyu5e@45.61.119.4:5394',
	'wmhzspws:sed3wrsjyu5e@154.12.132.15:6145',
	'wmhzspws:sed3wrsjyu5e@154.9.200.46:6111',
	'wmhzspws:sed3wrsjyu5e@154.12.99.180:5520',
	'wmhzspws:sed3wrsjyu5e@104.239.126.137:5405',
	'wmhzspws:sed3wrsjyu5e@154.12.143.96:6521',
	'wmhzspws:sed3wrsjyu5e@154.12.102.123:6250',
	'wmhzspws:sed3wrsjyu5e@154.37.176.244:6364',
	'wmhzspws:sed3wrsjyu5e@154.37.177.44:6420',
	'wmhzspws:sed3wrsjyu5e@23.229.110.3:8531',
	'wmhzspws:sed3wrsjyu5e@104.148.28.239:6516',
	'wmhzspws:sed3wrsjyu5e@134.73.174.57:5493',
	'wmhzspws:sed3wrsjyu5e@154.12.98.61:5145',
	'wmhzspws:sed3wrsjyu5e@154.37.181.5:5839',
	'wmhzspws:sed3wrsjyu5e@154.21.61.82:5356',
	'wmhzspws:sed3wrsjyu5e@161.123.54.200:5584',
	'wmhzspws:sed3wrsjyu5e@154.55.92.90:5107',
	'wmhzspws:sed3wrsjyu5e@45.127.249.47:5384',
	'wmhzspws:sed3wrsjyu5e@154.12.99.48:5388',
	'wmhzspws:sed3wrsjyu5e@154.12.102.168:6295',
	'wmhzspws:sed3wrsjyu5e@154.38.35.236:5504',
	'wmhzspws:sed3wrsjyu5e@184.174.29.47:5379',
	'wmhzspws:sed3wrsjyu5e@154.12.133.214:6600',
	'wmhzspws:sed3wrsjyu5e@154.12.97.104:6457',
	'wmhzspws:sed3wrsjyu5e@134.73.174.188:5624',
	'wmhzspws:sed3wrsjyu5e@154.37.173.9:6396',
	'wmhzspws:sed3wrsjyu5e@96.8.118.229:5595',
	'wmhzspws:sed3wrsjyu5e@45.61.120.95:5448',
	'wmhzspws:sed3wrsjyu5e@157.52.148.87:5974',
	'wmhzspws:sed3wrsjyu5e@104.232.208.83:5410',
	'wmhzspws:sed3wrsjyu5e@104.223.152.92:6057',
	'wmhzspws:sed3wrsjyu5e@104.148.28.68:6345',
	'wmhzspws:sed3wrsjyu5e@154.21.60.93:5111',
	'wmhzspws:sed3wrsjyu5e@154.55.89.166:6022',
	'wmhzspws:sed3wrsjyu5e@154.37.180.15:5593',
	'wmhzspws:sed3wrsjyu5e@134.73.174.160:5596',
	'wmhzspws:sed3wrsjyu5e@154.12.96.237:6334',
	'wmhzspws:sed3wrsjyu5e@154.12.103.79:6462',
	'wmhzspws:sed3wrsjyu5e@45.152.7.192:6540',
	'wmhzspws:sed3wrsjyu5e@198.46.209.124:5701'
]

proxy_ignore_set = set()

last_error = None

def fetch_product_listing_urls(category_path):
	url = DEPOP_CATEGORY_BASE_URL + category_path
	service = Service()
	options = webdriver.ChromeOptions()
	options.add_argument('--blink-settings=imagesEnabled=false')
	driver = webdriver.Chrome(service=service, options=options)
	driver.get(url)
	driver.find_element(By.XPATH, '//*[@id="__next"]/div/div[3]/div[2]/button[2]').click()
	time.sleep(3)
	while True:
		driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
		time.sleep(.5)
		soup = BeautifulSoup(driver.page_source, 'html.parser')
		product_tags = soup.find_all('a', attrs={"data-testid": 'product__item'})
		product_urls = [DEPOP_BASE_URL + str(tag.get('href')) for tag in product_tags]
		if len(product_urls) > CATEGORY_PRODUCT_URL_LIMIT:
			driver.quit()
			return product_urls


def fetch_info(product_url, i):
	info = dict()
	# Get the difference in seconds
	global last_error
	global proxy_ignore_set
	if last_error is not None:
		difference = abs(last_error - datetime.timestamp(datetime.now()))
		if difference < 10:
			time.sleep(10 - difference)
	attempt_counter = 0
	while True:
		p = PROXY_LIST[i % len(PROXY_LIST)]
		if p in proxy_ignore_set:
			i += 1
			continue
		proxy = {"http": "http://" + p, "https": "http://" + p}
		response = requests.get(product_url, proxies=proxy)
		if response.status_code == 200:
			break
		elif response.status_code == 301:
			print('301 redirect for proxy... adding to ignore list ')
			proxy_ignore_set.add(p)
			continue
		elif response.status_code == 407:
			print('proxy authentication error... adding to ignore list ')
			proxy_ignore_set.add(p)
			continue
		attempt_counter += 1
		if attempt_counter > 2:
			return None
		last_error = datetime.timestamp(datetime.now())
		print('error fetching product url... sleeping for 10 seconds: ' + product_url + ' status code: ' + str(response.status_code))
		time.sleep(10)
	soup = BeautifulSoup(response.content, 'html.parser')
	script_tag = soup.find('script', id='__NEXT_DATA__')
	product = json.loads(script_tag.string)['props']['initialReduxState']['product']['product']

	info['categories'] = fetch_categories(soup)
	info['styles'] = [x['name'] for x in product['style']] if 'style' in product else None
	info['hq_images'] = [x[-1]['url'] for x in product['pictures']] if 'pictures' in product else None
	info['condition'] = product['condition']['name'] if 'condition' in product else None
	info['colors'] = [x['name'] for x in product['colour']] if 'colour' in product else None
	info['ages'] = [x['name'] for x in product['age']] if 'age' in product else None
	info['source'] = [x['name'] for x in product['source']] if 'source' in product else None
	info['attributes'] = product['attributes'] if 'attributes' in product else None
	info['brand'] = product['brandName'] if 'brandName' in product else None
	info['slug'] = product['slug']
	info['price'] = product['price'] if 'price' in product else None
	return info


def fetch_categories(soup):
	breadcrumb_div = soup.find('div', {'class': 'styles__BreadcrumbItemsScrollable-sc-6311e212-1'})
	breadcrumb_items = breadcrumb_div.find_all('li')
	breadcrumbs = [item.find('span', {'itemprop': 'name'}).text for item in breadcrumb_items]

	def sublist_starting_with_first_in_set(lst, st):
		for i in range(len(lst)):
			if lst[i] in st:
				return lst[i:]
		return []

	return sublist_starting_with_first_in_set(breadcrumbs, CATEGORY_ROOTS)


def fetch_image(image_url, slug):
	wget.download(image_url, f'./images/{slug}.jpg')


def create_manifest(listings_info):
	manifest = []
	prices = []
	for info in listings_info:
		labels = []
		category_label = 'category-' + ' > '.join(info['categories'])
		styles_labels = ['style-' + x for x in info['styles']] if info['styles'] is not None else []
		colors_labels = ['color-' + x for x in info['colors']] if info['colors'] is not None else []
		ages_labels = ['age-' + x for x in info['ages']] if info['ages'] is not None else []
		sources_labels = ['source-' + x for x in info['source']] if info['source'] is not None else []
		condition_label = 'condition-' + info['condition'] if info['condition'] is not None else None
		prices.append(info['price'])

		labels.append(category_label)
		if condition_label is not None:
			labels.append(condition_label)
		labels.extend(styles_labels)
		labels.extend(colors_labels)
		labels.extend(ages_labels)
		labels.extend(sources_labels)

		manifest_entry = dict()
		manifest_entry['source-ref'] = S3_BASE_DIR + info['slug'] + '.jpg'
		for i in range(len(labels)):
			manifest_entry[str(i)] = i
			manifest_entry[str(i) + '-metadata'] = {
				'class-name': labels[i],
				'confidence': 1.0,
				'type': 'groundtruth/image-classification',
				'human-annotated': 'yes',
				'creation-date': '2021-07-01T00:00:00.000000'
			}
		manifest.append(manifest_entry)
	with open("./manifest/depop.manifest", "w") as f:
		for manifest_entry in manifest:
			f.write(json.dumps(manifest_entry))
			f.write('\n')

	with open('./prices/prices.txt', 'w') as f:
		for price in prices:
			f.write(json.dumps(price))
			f.write('\n')


def get_product_listing_info(args):
	product_listing_url, i, total = args
	try:
		info = fetch_info(product_listing_url, i)
		if info is None:
			return None
		if info['categories'] is None or len(info['categories']) == 0:
			print('no categories found for slug: ' + product_listing_url)
			return None
		if info['hq_images'] is None or len(info['hq_images']) == 0:
			print('no images found for slug: ' + product_listing_url + ' skipping')
			return None
		empty_info_count = sum(1 for x in info.values() if x is None or len(x) == 0)
		if empty_info_count >= 5:
			print('not enough info for slug: ' + product_listing_url + '.. skipping')
			return None
		fetch_image(info['hq_images'][0], info['slug'])
		print(
			f"New listing! index: {i} / {total} url: {product_listing_url} categories: {str(info['categories'])} empty_info_count: {empty_info_count}")
		return info
	except Exception as e:
		print('error fetching info for slug: ' + product_listing_url + '.. skipping')
		return None


def main():
	# for p in PROXY_LIST:
	# 	proxy = {"http": "http://" + p, "https": "http://" + p}
	# 	response = requests.get('https://www.depop.com/products/andyphamz-pink-vans-size-75mens-9/', proxies=proxy, allow_redirects=True)
	# 	if response.status_code != 200:
	# 		print(str(response.status_code) + ' ' + p)
	with Pool(processes=20) as pool:
		product_listing_urls = pool.map(fetch_product_listing_urls, CATEGORY_URL_PATHS)
	print('done fetching product listings: ' + str(len(product_listing_urls)))

	product_listing_urls = [item for sublist in product_listing_urls for item in sublist]
	total = len(product_listing_urls)
	inputs = [(url, i, total) for i, url in enumerate(product_listing_urls)]
	with Pool(processes=100) as pool:
		results = pool.map(get_product_listing_info, inputs)
	# remove None from results
	listings_info = [result for result in results if result is not None]

	counter = Counter()
	limit_listings_info = []

	for info in listings_info:
		category = ' '.join(info['categories'])
		if counter[category] < 80:
			limit_listings_info.append(info)
			counter[category] += 1

	create_manifest(limit_listings_info)


if __name__ == "__main__":
	main()
