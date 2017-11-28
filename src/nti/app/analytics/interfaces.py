#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import interface

from nti.app.products.courseware.interfaces import ICourseInstanceEnrollment

from nti.appserver.workspaces.interfaces import IContainerCollection

from nti.appserver.workspaces.interfaces import IWorkspace

from nti.contenttypes.courses.interfaces import ICourseInstance

from nti.dataserver.interfaces import IUser


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

# IUser is an appropriate analytics context
IUser.__bases__ += (IAnalyticsContext, )

# ICourseInstance can be a context analytics scoped to the course
ICourseInstance.__bases__ += (IAnalyticsContext, )

# ICourseInstanceEnrollmentRecord provides analytics context for a user in course
ICourseInstanceEnrollment.__bases__ += (IAnalyticsContext, )
