#!/user/bin/env python
# vim:set expandtab ts=4 sw=4:

# Copyright (C) 2016 IHEP-CC

## Auth: Cheng ZhenJing. 7/3/2016

class SendNullException(Exception):
    "Exception : when try to send a null string to vr."
    def __init__(self):
        Exception.__init__(self)
