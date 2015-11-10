import os.path
import sys
import json
import urllib2

working_dir = os.path.dirname(sys.argv[0])

class API(object):
  def __init__(self, access_token):
    super(API, self).__init__()
    self.access_token = access_token

  def fetch_user(self):
    return self.__get_json('current_user')

  def fetch_campaign_and_patrons(self):
    return self.__get_json('current_user/campaigns?include=rewards,creator,goals,pledges')

  def __get_json(self, suffix):
    url = "https://api.patreon.com/oauth2/api/{}".format(suffix)
    headers = {'Authorization': "Bearer {}".format(self.access_token)}
    request = urllib2.Request(url, headers=headers)
    contents = urllib2.urlopen(request).read()
    return json.loads(contents)


with open("{}/access_token.txt".format(working_dir), 'r') as access_token_file:
    access_token = access_token_file.read()
    api_client = API(access_token)
    response = api_client.fetch_campaign_and_patrons()
    main_campaign = response['data'][0]
    creator_id = main_campaign['relationships']['creator']['data']['id']
    included = response.get('included')
    pledges = [obj for obj in included
               if obj['type'] == 'pledge' and obj['relationships']['creator']['data']['id'] == creator_id]
    patron_ids = [pledge['relationships']['patron']['data']['id'] for pledge in pledges]
    patrons = [obj for obj in included
               if obj['type'] == 'user' and obj['id'] in patron_ids]
    patron_email_map = {patron['id']: patron['attributes']['email']
                        for patron in patrons
                        if 'email' in patron['attributes']}
    patronage_map = {patron_email_map[pledge['relationships']['patron']['data']['id']]: pledge['attributes']['amount_cents']
                     for pledge in pledges
                     if pledge['relationships']['patron']['data']['id'] in patron_email_map and 'amount_cents' in pledge['attributes']}

    with open("{}/output.txt".format(working_dir), 'w') as out_file:
        out_file.write("{}\n".format(json.dumps(patronage_map)))