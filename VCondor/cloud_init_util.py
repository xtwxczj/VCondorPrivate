#!/user/bin/env python
# vim:set expandtab ts=4 sw=4:

# Copyright (C) 2016 IHEP-CC

## Auth: Cheng ZhenJing. 7/3/2016

import os
import logging
import urllib2
import config as config

logging.basicConfig(filename=config.log_file,format='%(asctime)s  [%(levelname)s] : %(module)s - %(message)s',level=logging.ERROR)
 
