#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=W0221

import logging

from zope import component
from zope import interface

from zope.location.interfaces import IContained

from z3c.autoinclude.zcml import includePluginsDirective

from nti.app.asynchronous.processor import Processor

from nti.analytics import QUEUE_NAMES

from nti.analytics.interfaces import IObjectProcessor

from nti.analytics.resolvers import logger as resolvers_logger

from nti.analytics.resource_views import logger as resource_view_logger

from nti.analytics.sessions import logger as sessions_logger

from nti.analytics.users import logger as users_logger

from nti.dataserver.utils.base_script import create_context

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IContained)
class PluginPoint(object):

    __parent__ = None

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
            for clazz_logger in (resource_view_logger, users_logger,
                                 sessions_logger, resolvers_logger):
                clazz_logger.setLevel(logging.DEBUG)

    def extend_context(self, context):
        includePluginsDirective(context, PP_ANALYTICS)
        includePluginsDirective(context, PP_ANALYTICS_GRAPHDB)

    def create_context(self, env_dir, unused_args=None):
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
