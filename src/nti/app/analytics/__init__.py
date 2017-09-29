#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import zope.i18nmessageid
MessageFactory = zope.i18nmessageid.MessageFactory(__name__)

VIEW_STATS = 'view_stats'

ANALYTICS = 'analytics'
SYNC_PARAMS = 'sync_params'
ANALYTICS_TITLE = 'Analytics'
BATCH_EVENTS = 'batch_events'
ANALYTICS_SESSIONS = 'sessions'
ANALYTICS_SESSION = 'analytics_session'
END_ANALYTICS_SESSION = 'end_analytics_session'
HISTORICAL_SESSIONS_VIEW_NAME = 'HistoricalSessions'
