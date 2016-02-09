#!/usr/bin/env python3
#
# Run in the background with screen voodoo or just with nohup:
#     nohup python3 icso.py &
# Or suppress the nohup.out file, since the log should be tracking everything:
#     nohup python3 icso.py &>/dev/null &
#
import config
import logging
import re
import requests
from bs4 import BeautifulSoup
from random import choice, gauss
from time import sleep
from tinydb import TinyDB, Query

# Load database
org_ids = TinyDB(config.DB_FILE_ORG_LIST)
orgs_full = TinyDB(config.DB_FILE_ORG)
q = Query()

# Start log
logger = logging.getLogger(__name__)

# Groups of special fields
faux_contact_headers = ['headquarters_address', 'preferred_mailing_address']
contact_keys = ['address', 'phone', 'fax', 'email']
list_only_fields = ['languages', 'millennium_development_goals',
                    'funding_structure', 'country_geographical_area_of_activity']


def generate_list(list_html):
    # The site sets a fake login cookie behind the scenes, but requests doesn't
    # pick up on it. The `showProfileDetail.do` CGI script allows for a
    # `sessionCheck=false` variable, which makes the page accessible without
    # the fake login cookie (which is most importance, since that script
    # generates the full database listing for each organization), but the
    # `displayConsulativeStatusSearch doesn't work with that variable.
    #
    # So, the official list of all organizations needs to be saved manually
    # from a browser:
    #   http://esango.un.org/civilsociety/displayConsultativeStatusSearch.do?method=list&show=28000&from=list&col=&order=&searchType=&index=0
    #
    # The `show=x` and `index=0` are critical.

    with open(list_html, 'r') as f:
        full_list_raw = BeautifulSoup(f.read(), 'html.parser')

    full_list_table = full_list_raw.select('#content table.result')[0]

    all_orgs = []
    for row in full_list_table.find_all('tr'):
        first_column = row.find_all('td')
        if first_column:
            link = first_column[0].find('a')
            if link and 'showProfileDetail' in link.get('href'):
                profile_id = re.search(r"profileCode=(\d+)",
                                       link.get('href')).group(1)
                all_orgs.append({'org_name': link.text, 'org_id': profile_id})

    org_ids.insert_multiple(all_orgs)

def clean_key(cell, contact_type):
    # Convert to lowercase, remove whitespace, replace spaces with _, and strip
    # out anything that's not a letter, digits, or _
    key_clean = re.sub(r"\W", "", cell.strip().lower().replace(' ', '_'))

    # Remove trailing _s and _yyyy; fix double__
    key_clean = re.sub("_$|_yyyy$", "", key_clean.replace('__', '_'))

    # If the key relates to contact information, add a prefix
    if key_clean in contact_keys:
        key_clean = contact_type + '_' + key_clean

    return(key_clean)

def clean_value_text(cell):
    value_clean = cell.strip().replace('\t', '').replace('\n\n', '\n')

    return(value_clean)

def clean_value_list(cell):
    li_elements_raw = cell.find_all('li')
    li_elements = [clean_value_text(li.text) for li in li_elements_raw]

    return(li_elements)

def clean_activity_list(cell):
    elements = cell.find_all(['b', 'li'])

    area_key = ''
    activities = {}

    for element in elements:
        # If the tag is a fake header, make it a key in the activities
        # dictionary and initialize an empty list
        if element.name == 'b':
            area_key = clean_key(element.text, None)
            activities[area_key] = []
        # If the tag is an li append it to the dictionary under the correct key
        else:
            activities[area_key].append(clean_value_text(element.text))

    return(activities)

def get_parse_org(org_id, i):
    if i > 0:
        wait = abs(gauss(config.wait_mu, config.wait_sd))
        logger.info("Pausing for {0:.2f} seconds.".format(wait))
        sleep(wait)

    url = (config.BASE_URL + 'showProfileDetail.do?method=printProfile' +
           '&tab=1&profileCode={0}&sessionCheck=false')
    r = requests.get(url.format(org_id['org_id']),
                     headers={"User-Agent": choice(config.user_agents)})
    soup = BeautifulSoup(r.text, 'html.parser')

    raw = soup.select('form')

    raw_tables = raw[0].find_all('table')

    organization = {}
    organization['org_id'] = org_id['org_id']

    # Every table has two columns: the first acts as the key and the second
    # acts as the value.
    for table in raw_tables:
        rows = table.find_all('tr')

        contact_type = 'hq'
        for row in rows:
            cells_raw = row.find_all(["th", "td"])

            if len(cells_raw) == 2:
                key_raw, value_raw = cells_raw

                if key_raw.text == "Preferred mailing address":
                    contact_type = 'preferred'

                key = clean_key(key_raw.text, contact_type)

                if key not in faux_contact_headers:
                    # These fields contain unordered lists
                    if key in list_only_fields:
                        organization[key] = clean_value_list(value_raw)
                    # This field contains multiple unorderd lists, separated by
                    # a heading wrapped in <b>
                    elif key == 'areas_of_expertise_fields_of_activity':
                        organization[key] = clean_activity_list(value_raw)
                    # All other fields are just plain text
                    else:
                        organization[key] = clean_value_text(value_raw.text)

    orgs_full.insert(organization)


if __name__ == '__main__':
    if len(org_ids) == 0:
        logger.info("Extracting list of organization names and IDs.")
        generate_list(config.LIST_FILE)

    # for i, org_id in enumerate(org_ids.all()):
    for i, org_id in enumerate(org_ids.all()[0:2]):
        # Take a longer break every X organizations
        if i > 0 and i % config.long_wait_gap == 0:
            long_wait = abs(gauss(config.long_wait_mu, config.long_wait_sd))
            logger.info("Long break; pausing for {0:.2f} seconds."
                        .format(long_wait))
            sleep(long_wait)

        # Only scrape and parse the organization if it hasn't been done already
        if len(orgs_full.search(q.org_id == org_id['org_id'])) == 0:
            logger.info("Getting {0} ({1}): {2}"
                        .format(org_id['org_id'], i, org_id['org_name']))
            get_parse_org(org_id, i)
        else:
            logger.info("Skipping; already got information for {0}: {1}"
                        .format(org_id['org_id'], org_id['org_name']))

    logger.info("All done!")
