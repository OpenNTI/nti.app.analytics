#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from nti.appserver.workspaces.interfaces import IContainerCollection

from nti.appserver.workspaces.interfaces import IWorkspace


class IAnalyticsWorkspace(IWorkspace):
    """
    A workspace containing data for analytics.
    """

class IAnalyticsCollection(IContainerCollection):
    """
    An analytics related collection
    """


class ISessionsCollection(IAnalyticsCollection):
    """
    A collection of analytics sessions
    """


class IEventsCollection(IAnalyticsCollection):
    """
    A collection of analytics events
    """
