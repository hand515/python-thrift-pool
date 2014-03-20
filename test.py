# -*- coding: utf-8 -*-

import os,sys

from client import ThriftClientPool,ThriftClientConfig
from test import TestService

config = ThriftClientConfig("127.0.0.1",9090)

pool = ThriftClientPool(config,clientInterface=TestService.Client)


client = pool.getClient()
print client.test("123")
print client.test("1234")
print client.test("1235")