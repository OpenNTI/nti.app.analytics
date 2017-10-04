#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Implementation of an Atom/OData workspace and collection for analytics.

.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.container.contained import Contained

from zope.location.interfaces import ILocation

from zope.traversing.interfaces import IPathAdapter

from nti.analytics import has_analytics

from nti.app.analytics import ANALYTICS
from nti.app.analytics import SYNC_PARAMS
from nti.app.analytics import BATCH_EVENTS
from nti.app.analytics import ANALYTICS_TITLE
from nti.app.analytics import ANALYTICS_SESSION
from nti.app.analytics import ANALYTICS_SESSIONS
from nti.app.analytics import END_ANALYTICS_SESSION

from nti.app.analytics.interfaces import IAnalyticsWorkspace
from nti.app.analytics.interfaces import IEventsCollection
from nti.app.analytics.interfaces import ISessionsCollection

from nti.appserver.workspaces.interfaces import IWorkspace
from nti.appserver.workspaces.interfaces import IUserService
from nti.appserver.workspaces.interfaces import IContainerCollection

from nti.dataserver.interfaces import IDataserverFolder

from nti.links.links import Link

@interface.implementer(IWorkspace)
@component.adapter(IUserService)
def AnalyticsWorkspace(user_service, root=None):
	root = root or user_service.__parent__
	analytics_ws = _AnalyticsWorkspace(parent=root)
	assert analytics_ws.__parent__
	return analytics_ws

@interface.implementer(IPathAdapter)
def analytics_path_adapter(ds_root, request):
	return AnalyticsWorkspace(None, root=ds_root)

@interface.implementer(IAnalyticsWorkspace)
@component.adapter(IDataserverFolder)
class _AnalyticsWorkspace(Contained):
	"""
	A workspace that currently, does not have any collections, but
	exposes links that may be useful to analytics clients.
	"""

	__name__ = ANALYTICS
	name = ANALYTICS_TITLE

	__parent__ = None

	def __init__(self, parent=None):
		super(_AnalyticsWorkspace, self).__init__()
		if parent:
			self.__parent__ = parent

	@property
	def collections(self):
		return (EventsCollection(self), SessionsCollection(self))

	@property
	def links(self):
		result = []
		if has_analytics():
			link_names = (BATCH_EVENTS, ANALYTICS_SESSION, ANALYTICS_SESSIONS,
						  END_ANALYTICS_SESSION, SYNC_PARAMS)
			for name in link_names:
				link = Link(ANALYTICS, rel=name, elements=(name,))
				link.__name__ = link.target
				link.__parent__ = self.__parent__
				interface.alsoProvides(link, ILocation)
				result.append(link)

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

@interface.implementer(IEventsCollection)
class EventsCollection(object):
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
class SessionsCollection(object):
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

