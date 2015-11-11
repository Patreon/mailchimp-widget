import os.path
import sys
import json
import urllib2
import base64
import time
import md5

working_dir = os.path.dirname(sys.argv[0])

class PatreonAPI(object):
    def __init__(self, access_token):
        super(PatreonAPI, self).__init__()
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


class MailChimpAPI(object):
    def __init__(self, api_key, list_id):
        super(MailChimpAPI, self).__init__()
        self.api_key = api_key
        self.list_id = list_id

    def create_tags_and_update_emails(self, email_map):
        tags_result = self.create_tags()
        emails_result = self.update_emails(email_map)
        return [tags_result, emails_result]

    def create_tags(self):
        create_operations = [self.craft_single_mergevar_creation('PTR_AMT', 'Patreon - Pledge Amount (Cents)', 'number'),
                             self.craft_single_mergevar_creation('PTR_NAME', 'Patreon - Full Name', 'text'),
                             self.craft_single_mergevar_creation('PTR_EXISTS', 'Patreon - Is Patron', 'radio')]
        return self.__batch_operations(create_operations)

    def craft_single_mergevar_creation(self, key, name, type_):
        single_tag_creation_body = {
            'tag': key,
            'type': type_,
            'name': name,
            'required': False,
            'public': False
        }
        if type_ == 'radio':
            single_tag_creation_body['options'] = {
                'choices': ['true', '']
            }
        return {
            'method': 'POST',
            'path': '/lists/{}/merge-fields'.format(self.list_id),
            'operation_id': 'create mergevar {}'.format(key),
            'body': json.dumps(single_tag_creation_body)
        }

    def update_emails(self, email_map):
        update_operations = [self.craft_single_email_update(email, patronage_amount_and_full_name['amount_cents'],
                                                              patronage_amount_and_full_name['full_name'])
                             for email, patronage_amount_and_full_name in email_map.items()]
        update_result = self.__batch_operations(update_operations)
        # TODO: we could http-GET update_result['response_body_url'], un-gzip & un-tar it, iterate over the responses, and retry any operation_id that had a non-200 status_code
        # ... or, we could just issue create requests for all the emails, and trust that the intersection of failures for the updates and failures for the creations is size 0
        creation_result = None
        if update_result['errored_operations'] != 0:
            create_operations = [self.craft_single_email_creation(email, patronage_amount_and_full_name['amount_cents'],
                                                                  patronage_amount_and_full_name['full_name'])
                                 for email, patronage_amount_and_full_name in email_map.items()]
            creation_result = self.__batch_operations(create_operations)
        return [update_result, creation_result]

    def craft_single_email_update(self, email, patronage_amount, full_name):
        single_email_update_body = {
            'email_address': email,
            'merge_fields': {
                'PTR_EXISTS': 'true' if (patronage_amount > 0) else '',
                'PTR_AMT': patronage_amount,
                'PTR_NAME': full_name
            }
        }
        return {
            'method': 'PATCH',
            'path': self.path_for_email(email),
            'operation_id': 'update {}'.format(email),
            'body': json.dumps(single_email_update_body)
        }

    def craft_single_email_creation(self, email, patronage_amount, full_name):
        single_email_create_body = {
            'email_address': email,
            'status': 'subscribed',
            'merge_fields': {
                'PTR_EXISTS': 'true' if (patronage_amount > 0) else '',
                'PTR_AMT': patronage_amount,
                'PTR_NAME': full_name
            }
        }
        return {
            'method': 'POST',
            'path': self.path_for_email(email, is_create=True),
            'operation_id': 'update {}'.format(email),
            'body': json.dumps(single_email_create_body)
        }

    def datacenter(self):
        return "us{}".format(self.api_key.split('us')[-1])

    def path_for_email(self, email, is_create=False):
        basic_path = '/lists/{}/members'.format(self.list_id)
        if is_create:
            return basic_path
        else:
            email_md5 = md5.new(email.lower())
            return '{}/{}'.format(basic_path, email_md5.hexdigest())

    def __mailchimp_request(self, suffix, json_payload):
        url = "https://{}.api.mailchimp.com/3.0/{}".format(self.datacenter(), suffix)
        print('url',url)
        basic_auth = base64.encodestring('{}:{}'.format('user', self.api_key)).replace('\n', '')
        headers = {'Authorization': "Basic {}".format(basic_auth)}
        if json_payload:
            payload = json.dumps(json_payload)
            request = urllib2.Request(url, data=payload, headers=headers)
        else:
            request = urllib2.Request(url, headers=headers)
        try:
            contents = urllib2.urlopen(request).read()
            return json.loads(contents)
        except urllib2.HTTPError as e:
            print('http error',e.read())
            raise e

    def __batch_operations(self, operations):
        batch_request_state = self.__mailchimp_request('batches', {'operations': operations})
        batch_request_id = batch_request_state['id']
        while batch_request_state['status'] != 'finished':
            time.sleep(3)
            batch_request_state = self.__mailchimp_request('batches/{}'.format(batch_request_id), None)
        return batch_request_state


with open("{}/Resources/Config/config.csv".format(working_dir), 'r') as config_file:
    access_token, mailchimp_api_key, mailchimp_list_id = config_file.read().split(',')
    access_token = access_token.strip()
    mailchimp_api_key = mailchimp_api_key.strip()
    mailchimp_list_id = mailchimp_list_id.strip()
    results = [access_token, mailchimp_api_key, mailchimp_list_id]

    # # GET from Patreon
    patreon_api_client = PatreonAPI(access_token)
    response = patreon_api_client.fetch_campaign_and_patrons()
    main_campaign = response['data'][0]
    creator_id = main_campaign['relationships']['creator']['data']['id']
    included = response.get('included')
    pledges = [obj for obj in included
               if obj['type'] == 'pledge' and obj['relationships']['creator']['data']['id'] == creator_id]
    patron_ids = [pledge['relationships']['patron']['data']['id'] for pledge in pledges]
    patrons = [obj for obj in included
               if obj['type'] == 'user' and obj['id'] in patron_ids]
    patron_attributes_map = {patron['id']: patron['attributes']
                             for patron in patrons
                             if 'email' in patron['attributes']}
    patronage_map = {}
    for pledge in pledges:
        if pledge['relationships']['patron']['data']['id'] in patron_attributes_map:
            patron_attributes = patron_attributes_map[pledge['relationships']['patron']['data']['id']]
            if 'email' in patron_attributes and 'amount_cents' in pledge['attributes']:
                relevant_info = {
                    'amount_cents': pledge['attributes']['amount_cents']
                }
                if 'full_name' in patron_attributes:
                    relevant_info['full_name'] = patron_attributes['full_name']
                patronage_map[patron_attributes['email']] = relevant_info

    # POST and PATCH to MailChimp
    mailchimp_api_client = MailChimpAPI(api_key=mailchimp_api_key,
                                        list_id=mailchimp_list_id)
    email, amt_and_name = patronage_map.items()[0]
    # results = mailchimp_api_client.update_one_email(email, amt_and_name['amount_cents'], amt_and_name['full_name'])
    # results = mailchimp_api_client.create_tags()
    results = mailchimp_api_client.create_tags_and_update_emails(patronage_map)
