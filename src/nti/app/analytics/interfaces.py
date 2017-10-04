#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from nti.appserver.workspaces.interfaces import IContainerCollection

from nti.appserver.workspaces.interfaces import IWorkspace

class IAnalyticsWorkspace(IWorkspace):
	"""
	A workspace containing data for analytics.
	"""

class ISessionsCollection(IContainerCollection):
    """
    A collection of analytics sessions
    """

class IEventsCollection(IContainerCollection):
    """
    A collection of analytics events
    """
