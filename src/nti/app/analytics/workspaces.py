#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Implementation of an Atom/OData workspace and collection for analytics.

.. $Id: workspaces.py 122769 2017-10-04 21:56:03Z chris.utz $
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component
from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.location.interfaces import IContained

from zope.traversing.interfaces import IPathAdapter

from nti.analytics import has_analytics

from nti.analytics.common import should_create_analytics

from nti.app.analytics import ANALYTICS
from nti.app.analytics import SYNC_PARAMS
from nti.app.analytics import BATCH_EVENTS
from nti.app.analytics import ANALYTICS_TITLE
from nti.app.analytics import ANALYTICS_SESSION
from nti.app.analytics import ANALYTICS_SESSIONS
from nti.app.analytics import ACTIVE_SESSION_COUNT
from nti.app.analytics import ACTIVE_TIMES_SUMMARY
from nti.app.analytics import END_ANALYTICS_SESSION
from nti.app.analytics import ACTIVITY_SUMMARY_BY_DATE
from nti.app.analytics import ACTIVE_USERS

from nti.app.analytics.interfaces import IAnalyticsContext
from nti.app.analytics.interfaces import IAnalyticsWorkspace
from nti.app.analytics.interfaces import IEventsCollection
from nti.app.analytics.interfaces import ISessionsCollection
from nti.app.analytics.interfaces import IAnalyticsContextACLProvider

from nti.app.authentication import get_remote_user

from nti.appserver.workspaces.interfaces import IWorkspace
from nti.appserver.workspaces.interfaces import IUserService

from nti.dataserver.authorization import ACT_READ
from nti.dataserver.authorization import ACT_CREATE
from nti.dataserver.authorization import is_admin_or_site_admin

from nti.dataserver.authorization_acl import ace_denying
from nti.dataserver.authorization_acl import ace_allowing
from nti.dataserver.authorization_acl import acl_from_aces

from nti.dataserver.interfaces import IUser
from nti.dataserver.interfaces import IDataserverFolder
from nti.dataserver.interfaces import EVERYONE_USER_NAME

from nti.links.links import Link

from nti.traversal.traversal import find_interface

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IWorkspace)
@component.adapter(IUserService)
def AnalyticsWorkspace(user_service, root=None, request=None):
    root = root or user_service.__parent__
    analytics_ws = _AnalyticsWorkspace(parent=root, request=request)
    assert analytics_ws.__parent__
    return analytics_ws


@interface.implementer(IPathAdapter)
def analytics_path_adapter(ds_root, request):
    return AnalyticsWorkspace(None, root=ds_root, request=request)


def _workspace_link(ctx, rel, name=None):
    elements = ('@@' + name, ) if name else None
    return Link(ctx, rel=rel, elements=elements)


@interface.implementer(IAnalyticsWorkspace, IContained)
@component.adapter(IDataserverFolder)
class _AnalyticsWorkspace(object):
    """
    A workspace that currently exposes links and collections that may be useful
    to analytics clients.
    """

    mime_type = mimeType = 'application/vnd.nextthought.analytics.workspace'

    __name__ = ANALYTICS
    name = ANALYTICS_TITLE

    __parent__ = None

    def __init__(self, parent=None, request=None):
        super(_AnalyticsWorkspace, self).__init__()
        if parent:
            self.__parent__ = parent
        self.events = EventsCollection(self)
        self.sessions = SessionsCollection(self)
        self._request = request

    def __acl__(self):
        aces = []
        # admins always have read access on this.
        user = get_remote_user()
        if is_admin_or_site_admin(user):
            aces.append(ace_allowing(user, ACT_READ, type(self)))

        context = find_interface(self, IAnalyticsContext, strict=False)
        aces_provider = IAnalyticsContextACLProvider(context, None)
        if aces_provider:
            aces.extend(aces_provider.aces())
        aces.append(ace_denying(EVERYONE_USER_NAME, ACT_READ, type(self)))

        return acl_from_aces(aces)

    @property
    def collections(self):
        if not has_analytics():
            return ()
        return (self.events, self.sessions)

    @property
    def links(self):
        if not has_analytics():
            return ()

        result = []
        for rel in (ACTIVITY_SUMMARY_BY_DATE, ACTIVE_TIMES_SUMMARY, ACTIVE_USERS):
            result.append(_workspace_link(self, rel, name=rel))

        result.append(
            _workspace_link(self, SYNC_PARAMS, name=SYNC_PARAMS)
        )

        # For BWC provide workspace level links to our events and sessions
        # collection
        if should_create_analytics(self._request):
            result.append(_workspace_link(self.events, BATCH_EVENTS))
            result.append(_workspace_link(self.sessions, ANALYTICS_SESSIONS))

            # For BWC surface some links for sessions at the workspace level
            for rel in (ANALYTICS_SESSION, END_ANALYTICS_SESSION):
                result.append(_workspace_link(self.sessions, rel, name=rel))
        return result

    def __getitem__(self, key):
        """
        Make us traversable to collections.
        """
        for i in self.collections:
            if i.__name__ == key:
                return i
        raise KeyError(key)

    def __len__(self):
        return len(self.collections)


class AnalyticsCollectionACLMixin(object):

    def __acl__(self):
        user_context = find_interface(self, IUser, strict=False)

        # If we are in root (no user context) everyone can create,
        # otherwise the user can create
        if user_context is None:
            user_context = EVERYONE_USER_NAME

        aces = [ace_allowing(user_context, ACT_CREATE, type(self))]
        return acl_from_aces(aces)


@interface.implementer(IEventsCollection)
class EventsCollection(AnalyticsCollectionACLMixin):
    """
    Pseudo-collection of analytics events.
    """

    __name__ = 'batch_events'
    name = 'Events'

    accepts = ()

    def __init__(self, parent):
        self.__parent__ = parent

    @Lazy
    def container(self):
        return ()


@interface.implementer(ISessionsCollection)
class SessionsCollection(AnalyticsCollectionACLMixin):
    """
    Pseudo-collection of analytics sessions.
    """

    __name__ = 'sessions'
    name = 'Sessions'

    accepts = ()

    def __init__(self, parent):
        self.__parent__ = parent

    @Lazy
    def container(self):
        return ()

    @Lazy
    def links(self):
        if not has_analytics():
            return ()
        links = []

        if is_admin_or_site_admin(get_remote_user()):
            links.append(
                _workspace_link(self, ACTIVE_SESSION_COUNT,
                                name=ACTIVE_SESSION_COUNT)
            )

        for rel in (ANALYTICS_SESSION, END_ANALYTICS_SESSION):
            links.append(_workspace_link(self, rel, name=rel))
        return links
