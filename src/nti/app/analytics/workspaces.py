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
from zope.location.interfaces import ILocationInfo

from zope.traversing.interfaces import IPathAdapter

from nti.analytics import has_analytics
from nti.analytics.interfaces import IAnalyticsContext

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

from nti.app.analytics.interfaces import IEventsCollection
from nti.app.analytics.interfaces import ISessionsCollection
from nti.app.analytics.interfaces import IAnalyticsWorkspace

from nti.app.authentication import get_remote_user

from nti.appserver.interfaces import IEditLinkMaker
from nti.appserver.pyramid_renderers_edit_link_decorator import DefaultEditLinkMaker

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
from nti.dataserver.interfaces import IShouldHaveTraversablePath
from nti.dataserver.interfaces import EVERYONE_USER_NAME

from nti.links.links import Link

from nti.ntiids.oids import to_external_ntiid_oid

from nti.traversal.traversal import find_interface

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IWorkspace)
@component.adapter(IUserService)
def AnalyticsWorkspace(user_service, root=None):
    root = root or user_service.__parent__
    analytics_ws = _AnalyticsWorkspace(parent=root)
    assert analytics_ws.__parent__
    return analytics_ws


@interface.implementer(IPathAdapter)
def analytics_path_adapter(ds_root, unused_request):
    return AnalyticsWorkspace(None, root=ds_root)


def _workspace_link(ctx, rel, name=None, analytics_context=None):
    """
    Generates a link with the providd rel, name, and elements.  This function
    accounts for the fact tht while our analytics links are traversable beneath
    our analytics_context, the context itself may not always be traversable.
    In that case we need to render a hybrid link to the object using ntiid
    based traversal, then standard path traversal after that.
    """

    if analytics_context is None:
        analytics_context = find_interface(ctx, IAnalyticsContext, strict=False)

    # If we have no analytics_context, the link context is the
    # analytics_context, or the analytics_context is traversable
    # we can just render a normal link
    if analytics_context is None \
            or IShouldHaveTraversablePath.providedBy(analytics_context) \
            or ctx == analytics_context:
        elements = ('@@' + name, ) if name else None
        return Link(ctx, rel=rel, elements=elements)


    # Look at our location and figure out the elements we need to append
    # after our analtyics context. This is the part we are responsible
    # for knowing can be traversed.
    location_info = ILocationInfo(ctx)
    elements = (location_info.getName(), )
    for parent in location_info.getParents():
        if parent == analytics_context:
            break
        elements += (getattr(parent, '__name__', None), )

    # We dug up through parents so we need to reverse this
    elements = elements[::-1]

    # If we have a named link, append that as an element as well
    if name:
        elements += ('@@'+name, )

    return Link(to_external_ntiid_oid(analytics_context), rel=rel, elements=elements)


@interface.implementer(IEditLinkMaker)
class _AnalyticsRelativeLinkMaker(DefaultEditLinkMaker):

    __slots__ = ('context',)

    def __init__(self, context=None):
        self.context = context

    def make(self, context, unused_request=None, allow_traversable_paths=True, link_method=None):
        return _workspace_link(context, 'edit')




@interface.implementer(IAnalyticsWorkspace, IContained)
@component.adapter(IDataserverFolder)
class _AnalyticsWorkspace(object):
    """
    A workspace that currently exposes links and collections that may be useful
    to analytics clients.
    """

    __name__ = ANALYTICS
    name = ANALYTICS_TITLE

    __parent__ = None

    def __init__(self, parent=None):
        super(_AnalyticsWorkspace, self).__init__()
        if parent:
            self.__parent__ = parent
        self.events = EventsCollection(self)
        self.sessions = SessionsCollection(self)

    def __acl__(self):
        aces = []
        # admins always have read access on this.
        user = get_remote_user()
        if is_admin_or_site_admin(user):
            aces.append(ace_allowing(user, ACT_READ, type(self)))

        # If our parent is a user we want to grant that user
        # read access but deny read access to others. The user lets
        # members of the shared community have read access
        # so we have to explicitly deny
        if IUser.providedBy(self.__parent__):
            aces.append(ace_allowing(self.__parent__, ACT_READ, type(self)))
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
        for rel in (ACTIVITY_SUMMARY_BY_DATE, ACTIVE_TIMES_SUMMARY, ):
            result.append(_workspace_link(self, rel, name=rel))

        result.append(
            _workspace_link(self, SYNC_PARAMS, name=SYNC_PARAMS)
        )

        # For BWC provide workspace level links to our events and sessions
        # collection
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
