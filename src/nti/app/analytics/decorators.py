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

from nti.appserver.interfaces import IEditLinkMaker

from nti.appserver.pyramid_authorization import has_permission

from nti.contenttypes.courses.interfaces import ICourseInstance
from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode

from nti.dataserver import authorization as nauth

from nti.dataserver.authorization import is_admin_or_site_admin

from nti.dataserver.contenttypes.forums.interfaces import ITopic

from nti.dataserver.interfaces import ILinkExternalHrefOnly
from nti.dataserver.interfaces import IUser
from nti.dataserver.interfaces import IShouldHaveTraversablePath

from nti.externalization.externalization import to_external_object

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.links.externalization import render_link
from nti.links.links import Link

from nti.ntiids.oids import to_external_ntiid_oid

LINKS = StandardExternalFields.LINKS

logger = __import__('logging').getLogger(__name__)


@component.adapter(ICourseOutlineContentNode)
@interface.implementer(IExternalMappingDecorator)
class _CourseOutlineNodeProgressLinkDecorator(AbstractAuthenticatedRequestAwareDecorator):
    """
    Return a link on the content node in which the client can retrieve
    progress information for a user.
    """

    def _predicate(self, unused_context, unused_result):
        return self._is_authenticated and has_analytics()

    def _do_decorate_external(self, context, result):
        links = result.setdefault(LINKS, [])
        link = Link(context, rel="Progress", elements=('Progress',))
        interface.alsoProvides(link, ILocation)
        link.__name__ = ''
        link.__parent__ = context
        links.append(link)


@component.adapter(ICourseInstance)
@interface.implementer(IExternalMappingDecorator)
class _CourseVideoProgressLinkDecorator(AbstractAuthenticatedRequestAwareDecorator):
    """
    Return a link on the course in which the client can retrieve
    all video progress for a user.
    """

    def _predicate(self, unused_context, unused_result):
        return self._is_authenticated and has_analytics()

    def _do_decorate_external(self, context, result):
        links = result.setdefault(LINKS, [])
        link = Link(context, rel="VideoProgress", elements=('VideoProgress',))
        interface.alsoProvides(link, ILocation)
        link.__name__ = ''
        link.__parent__ = context
        links.append(link)


@component.adapter(ITopic)
@interface.implementer(IExternalMappingDecorator)
class _TopicProgressDecorator(AbstractAuthenticatedRequestAwareDecorator):
    """
    Return progress for the outbound Topic.  Generally useful for noting
    the last time a user viewed a topic to determine if there is new
    user content.
    """

    def _predicate(self, unused_context, unused_result):
        return self._is_authenticated and has_analytics()

    def _do_decorate_external(self, context, result):
        progress = get_topic_progress(self.remoteUser, context)
        result['Progress'] = to_external_object(progress)


@component.adapter(ICourseInstance)
@interface.implementer(IExternalMappingDecorator)
class _GeoLocationsLinkDecorator(AbstractAuthenticatedRequestAwareDecorator):
    """
    Add a geo location link on the given course.
    """

    def _predicate(self, unused_context, unused_result):
        return self._is_authenticated and has_analytics()

    def _do_decorate_external(self, context, result):
        links = result.setdefault(LINKS, [])
        link = Link(context, rel='GeoLocations', elements=('GeoLocations',))
        interface.alsoProvides(link, ILocation)
        link.__name__ = ''
        link.__parent__ = context
        links.append(link)

@component.adapter(IAnalyticsCollection)
@interface.implementer(IExternalMappingDecorator)
class _AnalyticsCollectionHrefRewritter(AbstractAuthenticatedRequestAwareDecorator):
    """
    Collections by default use normal_get_resource to render their href. However
    depending on the context we are in that may not be good enough (we likely
    have a context that doesn't have a traversable path.)  Rewrite the href
    for our collections using our IEditLinkMaker which knows how to account
    for these contextually sensitive traversals
    """

    def _do_decorate_external(self, context, result):
        link_maker = IEditLinkMaker(context)
        link = link_maker.make(context)
        if link:
            interface.alsoProvides(link, ILinkExternalHrefOnly)
            result['href'] = render_link(link)

@component.adapter(IAnalyticsContext)
@interface.implementer(IExternalMappingDecorator)
class _AnalyticsContextLink(AbstractAuthenticatedRequestAwareDecorator):

    def _predicate(self, context, unused_result):
        return self._is_authenticated \
           and has_analytics()

    def _do_decorate_external(self, context, result):
        workspace = AnalyticsWorkspace(None, root=context)

        if not has_permission(nauth.ACT_READ, workspace):
            return

        links = result.setdefault(LINKS, [])

        # some things aren't traversable, but also don't expose an `ntiid`
        # on them. That leads to a link being rendered using lineage that can't be
        # traversed later.  ICourseInstanceEnrollmentRecord is one such example.
        # TODO: that is probably something that needs to be resolved rather than
        # worked around here.  Options seem to be make the enrollment record container
        # traversable from the course or give the record an ntiid
        if not IShouldHaveTraversablePath.providedBy(context):
            oid_ntiid = to_external_ntiid_oid(context)
            if oid_ntiid is not None:
                context = oid_ntiid

        link = Link(context,
                    elements=('analytics',),
                    rel=ANALYTICS)
        interface.alsoProvides(link, ILocation)
        link.__name__ = ''
        link.__parent__ = context
        links.append(link)

@component.adapter(IUser)
@interface.implementer(IExternalMappingDecorator)
class _UserSessionDecorator(AbstractAuthenticatedRequestAwareDecorator):

    def _predicate(self, context, unused_result):
        return self._is_authenticated \
           and has_analytics() \
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
