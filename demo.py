# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""pyDataverse workshop Tromso functions."""

import json
from pyDataverse.utils import read_file
import subprocess as sp
import time


def parse_dataset_keys(dataset, data, terms_filename):
    ds_tmp = {}
    ds_id = None
    ds_tmp['termsOfAccess'] = read_file(terms_filename)
    for key, val in dataset.items():
        if not val == '':
            if key == 'organization.dataset_id':
                ds_id = val
            elif key == 'dataverse.title':
                ds_tmp['title'] = val
            elif key == 'dataverse.subtitle':
                ds_tmp['subtitle'] = val
            elif key == 'dataverse.author':
                ds_tmp['author'] = json.loads(val)
            elif key == 'dataverse.dsDescription':
                ds_tmp['dsDescription'] = []
                ds_tmp['dsDescription'].append({'dsDescriptionValue': val})
            elif key == 'dataverse.keywordValue':
                ds_tmp['keyword'] = json.loads(val)
            elif key == 'dataverse.topicClassification':
                ds_tmp['topicClassification'] = json.loads(val)
            elif key == 'dataverse.language':
                ds_tmp['language'] = json.loads(val)
            elif key == 'dataverse.subject':
                ds_tmp['subject'] = []
                ds_tmp['subject'].append(val)
            elif key == 'dataverse.kindOfData':
                ds_tmp['kindOfData'] = json.loads(val)
            elif key == 'dataverse.datasetContact':
                ds_tmp['datasetContact'] = json.loads(val)
    data[ds_id] = {'metadata': ds_tmp}
    return data


def create_dataset(api, ds, dv_alias, mapping_dsid2pid, ds_id, base_url):
    resp = api.create_dataset(dv_alias, ds.json())
    pid = resp.json()['data']['persistentId']
    mapping_dsid2pid[ds_id] = pid
    time.sleep(1)
    print('{0}/dataset.xhtml?persistentId={1}&version=DRAFT'.format(base_url,
                                                                    pid))
    return resp, mapping_dsid2pid


def import_datafile(datafile, data):
    df_tmp = {}
    df_id = None
    ds_id = None
    for key, val in datafile.items():
        if not val == '':
            if key == 'dataverse.description':
                df_tmp['description'] = val
            elif key == 'organization.filename':
                df_tmp['filename'] = val
            elif key == 'organization.datafile_id':
                df_tmp['datafile_id'] = val
                df_id = val
            elif key == 'organization.dataset_id':
                ds_id = val
                df_tmp['dataset_id'] = ds_id
            elif key == 'dataverse.categories':
                df_tmp['categories'] = json.loads(val)
    if 'datafiles' not in data[ds_id]:
        data[ds_id]['datafiles'] = {}
    if df_id not in data[ds_id]['datafiles']:
        data[ds_id]['datafiles'][df_id] = {}
    if 'metadata' not in data[ds_id]['datafiles'][df_id]:
        data[ds_id]['datafiles'][df_id]['metadata'] = {}
    data[ds_id]['datafiles'][df_id]['metadata'] = df_tmp
    return data


def upload_datafile(api, pid, filename, df):
    path = api.native_api_base_url
    path += '/datasets/:persistentId/add?persistentId={0}'.format(pid)
    shell_command = 'curl -H "X-Dataverse-key: {0}"'.format(api.api_token)
    shell_command += ' -X POST {0} -F file=@{1}'.format(path, filename)
    shell_command += " -F 'jsonData={0}'".format(df.json())
    result = sp.run(shell_command, shell=True, stdout=sp.PIPE)
    if filename[-4:] == '.sav' or filename[-4:] == '.dta':
        time.sleep(20)
    else:
        time.sleep(2)
    return result


def publish_dataset(pid, api):
    resp = api.publish_dataset(pid, 'major')
    print(resp.json())
    return resp


def delete_dataset(pid, api):
    resp = api.delete_dataset(pid)
    time.sleep(1)
    return resp
