#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import interface

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


class IAnalyticsContext(interface.Interface):
    """
    A marker interface for things that are "analytics aware"
    and can provide analytics information specific to their context
    """
IAnalyticsContext.setTaggedValue('_ext_is_marker_interface', True)


class IAnalyticsContextACLProvider(interface.Interface):
    """
    Queried as an adapter on IAnalyticsContext
    to provide additional aces to include in the workspaces acl
    """

    def aces():
        """
        A set of aces that should be added to the workspaces acl
        """
