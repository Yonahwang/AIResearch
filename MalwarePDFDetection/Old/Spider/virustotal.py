#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
params = {'apikey': '-YOUR API KEY HERE-'}
files = {'file': ('F:\PDFdata\VirusShare_PDF_20170404\cve2010-2883', open('F:\PDFdata\VirusShare_PDF_20170404\cve2010-2883', 'rb'))}
response = requests.post('https://www.virustotal.com/vtapi/v2/file/scan', files=files, params=params)
json_response = response.json()