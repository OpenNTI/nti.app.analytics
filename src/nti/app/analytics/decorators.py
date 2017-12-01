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

from nti.analytics.progress import get_topic_progress

from nti.analytics.sessions import get_recent_user_sessions

from nti.app.analytics import ANALYTICS
from nti.app.analytics import ANALYTICS_SESSIONS
from nti.app.analytics import HISTORICAL_SESSIONS_VIEW_NAME

from nti.app.analytics.interfaces import IAnalyticsCollection
from nti.app.analytics.interfaces import IAnalyticsContext

from nti.app.analytics.workspaces import AnalyticsWorkspace

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.appserver.pyramid_authorization import has_permission

from nti.contenttypes.courses.interfaces import ICourseInstance
from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode

from nti.dataserver import authorization as nauth

from nti.dataserver.authorization import is_admin_or_site_admin

from nti.dataserver.contenttypes.forums.interfaces import ITopic

from nti.dataserver.interfaces import IUser

from nti.externalization.externalization import to_external_object

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.links.links import Link

LINKS = StandardExternalFields.LINKS

logger = __import__('logging').getLogger(__name__)

class _AnalyticsEnabledDecorator(AbstractAuthenticatedRequestAwareDecorator):

    def _predicate(self, unused_context, unused_result):
        return self._is_authenticated and has_analytics()

@component.adapter(ICourseOutlineContentNode)
@interface.implementer(IExternalMappingDecorator)
class _CourseOutlineNodeProgressLinkDecorator(_AnalyticsEnabledDecorator):
    """
    Return a link on the content node in which the client can retrieve
    progress information for a user.
    """

    def _do_decorate_external(self, context, result):
        links = result.setdefault(LINKS, [])
        link = Link(context, rel="Progress", elements=('Progress',))
        interface.alsoProvides(link, ILocation)
        link.__name__ = ''
        link.__parent__ = context
        links.append(link)


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


@component.adapter(ITopic)
@interface.implementer(IExternalMappingDecorator)
class _TopicProgressDecorator(_AnalyticsEnabledDecorator):
    """
    Return progress for the outbound Topic.  Generally useful for noting
    the last time a user viewed a topic to determine if there is new
    user content.
    """

    def _do_decorate_external(self, context, result):
        progress = get_topic_progress(self.remoteUser, context)
        result['Progress'] = to_external_object(progress)


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
        workspace = AnalyticsWorkspace(None, root=context)

        if not has_permission(nauth.ACT_READ, workspace):
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
        most_recent_sessions = get_recent_user_sessions(context, limit=1)
        session = most_recent_sessions[0] if most_recent_sessions else None
        if session:
            session = IAnalyticsSession(session)
        result['MostRecentSession'] = session

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
        return super(_SessionDetailsDecorator, self)._predicate(context, unused_result) \
           and (   self.remoteUser.username == context.Username
                or is_admin_or_site_admin(self.remoteUser))

    def _do_decorate_external(self, context, result):
        for field in ('Username', 'UserAgent', ):
            result[field] = getattr(context, field, None)
