import json
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

# tripp nyc, burberry, abercrombie and fitch, lip service, hysteric glamour
BRAND_URL_PATHS = ['/brands/tripp-nyc/', '/brands/burberry/', '/brands/abercrombie-fitch/', '/brands/lip-service/', '/brands/hysteric-glamour/']
CATEGORY_ROOTS = {'Womenswear', 'Menswear'}
PRODUCT_URL_LIMIT = 200
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

def fetch_product_listing_urls(path):
	url = DEPOP_BASE_URL + path
	service = Service()
	options = webdriver.ChromeOptions()
	options.add_argument('--blink-settings=imagesEnabled=false')
	driver = webdriver.Chrome(service=service, options=options)
	driver.get(url)
	driver.find_element(By.XPATH, '//*[@id="__next"]/div/div[3]/div[2]/button[2]').click()
	prev_len = 0
	counter = 0
	while True:
		time.sleep(2)
		driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
		soup = BeautifulSoup(driver.page_source, 'html.parser')
		product_tags = soup.find_all('a', attrs={"data-testid": 'product__item'})
		product_urls = [DEPOP_BASE_URL + str(tag.get('href')) for tag in product_tags]
		if prev_len == len(product_urls):
			counter += 1
			if counter == 5:
				driver.quit()
				return product_urls
			continue
		if len(product_urls) > PRODUCT_URL_LIMIT:
			driver.quit()
			return product_urls
		prev_len = len(product_urls)
		counter = 0


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


def create_csv(listings_info):

	all_colors = set()
	for info in listings_info:
		if info['colors'] is not None:
			all_colors.update(info['colors'])
	if None in all_colors:
		all_colors.remove(None)
	color_categories = [s.lower() for s in sorted(list(all_colors))]

	all_styles = set()
	for info in listings_info:
		if info['styles'] is not None:
			all_styles.update(info['styles'])
	if None in all_styles:
		all_styles.remove(None)
	style_categories = [s.lower() for s in sorted(list(all_styles))]

	all_sources = set()
	for info in listings_info:
		if info['source'] is not None:
			all_sources.update(info['source'])
	if None in all_sources:
		all_sources.remove(None)
	source_categories = [s.lower() for s in sorted(list(all_sources))]

	# get all possible styles.
	all_categories = ['category', 'brand', 'condition', 'age']
	all_categories.extend(style_categories)
	all_categories.extend(color_categories)
	all_categories.extend(source_categories)
	all_categories.extend(['price', 'shipping_cost', 'total_price'])
	lines = []
	lines.append(','.join(all_categories))
	for info in listings_info:
		line = []
		category_label = ' > '.join(info['categories'])
		style_labels = [x for x in info['styles']] if info['styles'] is not None else []
		age_label = info['ages'][0] if info['ages'] is not None and len(info['ages']) > 0 else None
		condition_label = info['condition'] if info['condition'] is not None else None
		brand_label = info['brand'] if info['brand'] is not None else None
		color_labels = [x for x in info['colors']] if info['colors'] is not None else []
		source_labels = [x for x in info['source']] if info['source'] is not None else []
		price = round(float(info['price']['priceAmount']), 2)
		shipping_cost = round(float(info['price']['nationalShippingCost']), 2) if 'nationalShippingCost' in info['price'] else None
		total_price = price
		if shipping_cost is not None:
			total_price += shipping_cost
			total_price = round(total_price, 2)

		line.append(category_label)
		line.append(brand_label)
		line.append(condition_label)
		line.append(age_label)
		for style_category in style_categories:
			if style_category in [s.lower() for s in style_labels]:
				line.append('1')
			else:
				line.append('0')
		for color_category in color_categories:
			if color_category in [s.lower() for s in color_labels]:
				line.append('1')
			else:
				line.append('0')
		for source_category in source_categories:
			if source_category in [s.lower() for s in source_labels]:
				line.append('1')
			else:
				line.append('0')
		line.append(str(price))
		line.append(str(shipping_cost))
		line.append(str(total_price))
		lines.append(','.join([str(x) if x is not None else '' for x in line]))

	with open("./manifest/depop_prices.csv", "w") as f:
		for line in lines:
			f.write(line)
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
		product_listing_urls = pool.map(fetch_product_listing_urls, BRAND_URL_PATHS)
	print('done fetching product listings: ' + str(len(product_listing_urls)))

	product_listing_urls = [item for sublist in product_listing_urls for item in sublist]
	total = len(product_listing_urls)
	inputs = [(url, i, total) for i, url in enumerate(product_listing_urls)]
	with Pool(processes=75) as pool:
		results = pool.map(get_product_listing_info, inputs)
	# remove None from results
	listings_info = [result for result in results if result is not None]

	create_csv(listings_info)


if __name__ == "__main__":
	main()
