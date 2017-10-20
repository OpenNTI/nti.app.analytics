#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import logging

from zope import component

from zope.container.contained import Contained

from z3c.autoinclude.zcml import includePluginsDirective

from nti.async.utils.processor import Processor

from nti.analytics import QUEUE_NAMES

from nti.analytics.interfaces import IObjectProcessor

from nti.analytics.resolvers import logger as resolvers_logger

from nti.analytics.resource_views import logger as resource_view_logger

from nti.analytics.sessions import logger as sessions_logger

from nti.analytics.users import logger as users_logger

from nti.dataserver.utils.base_script import create_context

logger = __import__('logging').getLogger(__name__)


class PluginPoint(Contained):

    def __init__(self, name):
        self.__name__ = name

PP_ANALYTICS = PluginPoint('nti.analytics')
PP_ANALYTICS_GRAPHDB = PluginPoint('nti.analytics_graphdb')


class Constructor(Processor):

    def set_log_formatter(self, args):
        super(Constructor, self).set_log_formatter(args)
        if args.verbose:
            for _, module in component.getUtilitiesFor(IObjectProcessor):
                module.logger.setLevel(logging.DEBUG)
            for logger in (resource_view_logger, users_logger,
                           sessions_logger, resolvers_logger):
                logger.setLevel(logging.DEBUG)

    def extend_context(self, context):
        includePluginsDirective(context, PP_ANALYTICS)
        includePluginsDirective(context, PP_ANALYTICS_GRAPHDB)

    def create_context(self, env_dir, args=None):
        context = create_context(env_dir, with_library=True)
        self.extend_context(context)
        return context

    def process_args(self, args):
        setattr(args, 'redis', True)
        setattr(args, 'library', True)  # load library
        setattr(args, 'queue_names', QUEUE_NAMES)
        super(Constructor, self).process_args(args)


def main():
    return Constructor()()


if __name__ == '__main__':
    main()
