#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from nti.app.analytics.scripts.constructor import Constructor

logger = __import__('logging').getLogger(__name__)


class FailProcessor(Constructor):

    def process_args(self, args):
        setattr(args, 'failed_jobs', True)
        super(FailProcessor, self).process_args(args)


def main():
    return FailProcessor()()


if __name__ == '__main__':
    main()
