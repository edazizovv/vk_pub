#

import vk
import time
import pandas
import datetime
import requests.exceptions

#


login_source = 'C:/Users/MainUser/Desktop/vk_API_login.txt'
crs = open(login_source, "r")
for columns in (raw.strip().split() for raw in crs):
    login = columns[0]

pw_source = 'C:/Users/MainUser/Desktop/vk_API_pw.txt'
crs = open(pw_source, "r")
for columns in (raw.strip().split() for raw in crs):
    password = columns[0]

token_source = 'C:/Users/MainUser/Desktop/vk_API_token.txt'
crs = open(token_source, "r")
for columns in (raw.strip().split() for raw in crs):
    tok = columns[0]

id_source = 'C:/Users/MainUser/Desktop/vk_API_id.txt'
crs = open(id_source, "r")
for columns in (raw.strip().split() for raw in crs):
    vk_id = columns[0]

v = '5.122'

session = vk.AuthSession(access_token=tok)
vkapi = vk.API(session)

import numpy
def check_closed(x):
    ac = vkapi.users.get(user_ids=x, v=v)[0]
    time.sleep(1)
    if 'is_closed' in ac.keys():
        act = ac['is_closed']
        if act:
            return True
        else:
            return False
    else:
        return False


#
def get_subs(public_name, freeze=0):

    result = vkapi.groups.getMembers(group_id=public_name, v=v)['items']

    return result


def get_subd(user_id, freeze=0):

    import vk.exceptions
    time.sleep(freeze)
    try:
        ac = vkapi.users.get(user_ids=user_id, v=v)[0]
        if 'is_closed' in ac.keys():
            act = ac['is_closed']
            if act:
                result = None
                status = 'closed'
            else:
                time.sleep(freeze)
                result = vkapi.users.getSubscriptions(user_id=user_id, v=v)['groups']['items']
                status = 'opened'
        else:
            result = None
            status = 'dead'
    except Exception as e:
        if isinstance(e, vk.exceptions.VkAPIError):
            print(e)
            print("Error user_id={0}".format(user_id))
            result = []
            status = str(e)
            # raise e
        elif isinstance(e, requests.exceptions.ReadTimeout):
            print(e)
            time.sleep(60)
            result, status = get_subd(user_id)
        elif isinstance(e, requests.exceptions.ConnectionError):
            print(e)
            time.sleep(60)
            result, status = get_subd(user_id)
        else:
            raise e


    return result, status


names = ['olya_voodoo', 'tut_zhivet_ah0ra', 'twitcholyashaa', 'igromania', 'widewide',
         'ybicanoooobov', 'dendiboss', 'straydota', 's1mple41l', 'dreadztv', 'alohadancee']

freeze = 1

while True:

    megabus = []

    for name in names:

        wow_now = datetime.datetime.now()

        subs_ = get_subs(name, freeze=0.5)

        subs_subd_ = {name: subs_}
        for s in subs_:
            subs_subd_res_, status_ = get_subd(s, freeze=0.5)
            if status_ == 'opened':
                subs_subd_[s] = subs_subd_res_

        subs_resc_ = [[x[0]] * len(x[1]) for x in list(subs_subd_.items())]
        subs = [y for x in subs_resc_ for y in x]
        subd = [y for x in list(subs_subd_.items()) for y in x[1]]
        n_ = len(subs)

        subs_bus = pandas.DataFrame(data={'timestamp': [wow_now] * n_, 'from': subs, 'to': subd, 'from_status': ['opened'] * n_})
        megabus.append(subs_bus)

    megabus = pandas.concat(megabus, axis=0, ignore_index=True)

    to = './megabus.csv'
    megabus.to_csv(to, index=False, mode='a', header=False)
