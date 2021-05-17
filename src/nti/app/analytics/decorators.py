#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Externalization decorators.

.. $Id: decorators.py 122490 2017-09-29 03:36:57Z chris.utz $
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component
from zope import interface

from zope.location.interfaces import ILocation

from nti.analytics import has_analytics

from nti.analytics.interfaces import IAnalyticsSession

from nti.app.analytics import ANALYTICS
from nti.app.analytics import ANALYTICS_SESSIONS
from nti.app.analytics import HISTORICAL_SESSIONS_VIEW_NAME

from nti.app.analytics.interfaces import IAnalyticsContext
from nti.app.analytics.interfaces import IAnalyticsWorkspace

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.appserver.pyramid_authorization import has_permission

from nti.contenttypes.courses.interfaces import ICourseInstance

from nti.contenttypes.presentation.interfaces import INTIVideo

from nti.dataserver import authorization as nauth

from nti.dataserver.authorization import is_admin_or_site_admin

from nti.dataserver.interfaces import IUser

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.links.links import Link

LINKS = StandardExternalFields.LINKS

logger = __import__('logging').getLogger(__name__)


class _AnalyticsEnabledDecorator(AbstractAuthenticatedRequestAwareDecorator): # pylint: disable=abstract-method

    def _predicate(self, unused_context, unused_result):
        return self._is_authenticated and has_analytics()


@component.adapter(ICourseInstance)
@interface.implementer(IExternalMappingDecorator)
class _CourseVideoProgressLinkDecorator(_AnalyticsEnabledDecorator):
    """
    Return a link on the course in which the client can retrieve
    all video progress for a user.
    """

    def _do_decorate_external(self, context, result):
        links = result.setdefault(LINKS, [])
        link = Link(context, rel="VideoProgress", elements=('VideoProgress',))
        interface.alsoProvides(link, ILocation)
        link.__name__ = ''
        link.__parent__ = context
        links.append(link)

@component.adapter(INTIVideo)
@interface.implementer(IExternalMappingDecorator)
class _CourseVideoWatchInfo(_AnalyticsEnabledDecorator):
    """
    Return links on videos to resume and segment data
    when we have appropriate context
    """

    def _do_decorate_external(self, context, result):
        course = ICourseInstance(self.request, None)
        if not course:
            return
        
        links = result.setdefault(LINKS, [])

        for name in ('resume_info', 'watched_segments',):
            link = Link(course, rel=name, elements=('assets', context.ntiid, '@@'+name,))
            interface.alsoProvides(link, ILocation)
            link.__name__ = ''
            link.__parent__ = context
            links.append(link)


@component.adapter(ICourseInstance)
@interface.implementer(IExternalMappingDecorator)
class _GeoLocationsLinkDecorator(_AnalyticsEnabledDecorator):
    """
    Add a geo location link on the given course.
    """

    def _do_decorate_external(self, context, result):
        links = result.setdefault(LINKS, [])
        link = Link(context, rel='GeoLocations', elements=('GeoLocations',))
        interface.alsoProvides(link, ILocation)
        link.__name__ = ''
        link.__parent__ = context
        links.append(link)


@component.adapter(IAnalyticsContext)
@interface.implementer(IExternalMappingDecorator)
class _AnalyticsContextLink(_AnalyticsEnabledDecorator):

    def _do_decorate_external(self, context, result):
        context = IAnalyticsContext(context)
        workspace = IAnalyticsWorkspace(context, None)

        if workspace is None or not has_permission(nauth.ACT_READ, workspace):
            return

        links = result.setdefault(LINKS, [])
        link = Link(workspace,
                    rel=ANALYTICS)
        interface.alsoProvides(link, ILocation)
        link.__name__ = ''
        link.__parent__ = context
        links.append(link)


@component.adapter(IUser)
@interface.implementer(IExternalMappingDecorator)
class _UserSessionDecorator(_AnalyticsEnabledDecorator):

    def _predicate(self, context, unused_result):
        return super(_UserSessionDecorator, self)._predicate(context, unused_result) \
           and (   self.remoteUser == context
                or is_admin_or_site_admin(self.remoteUser))

    def _do_decorate_external(self, context, result):
        # This is also the best place to decorate a link to fetch recent
        # sessions
        links = result.setdefault(LINKS, [])
        link = Link(context,
                    rel=HISTORICAL_SESSIONS_VIEW_NAME,
                    elements=(ANALYTICS, ANALYTICS_SESSIONS, ))
        interface.alsoProvides(link, ILocation)
        link.__name__ = ''
        link.__parent__ = context
        links.append(link)


@component.adapter(IAnalyticsSession)
@interface.implementer(IExternalMappingDecorator)
class _SessionDetailsDecorator(_AnalyticsEnabledDecorator):

    def _predicate(self, context, unused_result):
        # pylint: disable=no-member
        return super(_SessionDetailsDecorator, self)._predicate(context, unused_result) \
           and (   self.remoteUser.username == context.Username
                or is_admin_or_site_admin(self.remoteUser))

    def _do_decorate_external(self, context, result):
        for field in ('Username', 'UserAgent', 'GeographicalLocation'):
            result[field] = getattr(context, field, None)
