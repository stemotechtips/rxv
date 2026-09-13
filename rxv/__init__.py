#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import logging

import ssdp
from rxv import RXV
from legacy_receivers import *

__all__ = ['RXV']

# disable default logging of warnings to stderr. If a consuming
# application sets up logging, it will work as expected.
#logging.getLogger('rxv').addHandler(logging.NullHandler())


async def find(timeout=1.5):
    """Find all Yamaha receivers on local network using SSDP search."""
    ri = ssdp.discover(timeout=timeout)

    if len(ri) == 0:
        print("No receivers found through conventional search - now trying legacy search")
        legacy_results = await get_legacy_devices()
        if len(legacy_results) != 0:
            print("Huzzah! Found a legacy receiver")
            ri.extend(legacy_results)
        
    #print(ri)
    return [RXV(**ri[0]._asdict())]

rx_list = asyncio.run(find())
print(rx_list)
rx = rx_list[0]
rx.on = True