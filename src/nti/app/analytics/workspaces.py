#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Implementation of an Atom/OData workspace and collection for analytics.

.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface
from zope import component
from zope.container.contained import Contained
from zope.location.interfaces import ILocation

from nti.appserver.interfaces import IWorkspace
from nti.appserver.interfaces import IUserService
from nti.appserver.interfaces import ICollection

from nti.dataserver.links import Link
from nti.dataserver.interfaces import IDataserverFolder

from nti.utils.property import alias
from nti.utils.property import Lazy

from . import ANALYTICS
from . import ANALYTICS_TITLE
from . import ANALYTICS_SESSION
from . import ANALYTICS_SESSIONS
from . import BATCH_EVENTS
from . import SYNC_PARAMS

from .interfaces import IAnalyticsWorkspace

@interface.implementer(IWorkspace)
@component.adapter(IUserService)
def AnalyticsWorkspace(user_service):
	analytics_ws = _AnalyticsWorkspace(parent=user_service.__parent__)
	assert analytics_ws.__parent__
	return analytics_ws

@interface.implementer(IAnalyticsWorkspace)
@component.adapter(IDataserverFolder)
class _AnalyticsWorkspace(Contained):
	"""
	A workspace that currently, does not have any collections, but
	exposes links that may be useful to analytics clients.
	"""

	__name__ = ANALYTICS_TITLE
	name = alias('__name__', __name__)

	__parent__ = None

	def __init__(self, parent=None):
		super(_AnalyticsWorkspace,self).__init__()
		if parent:
			self.__parent__ = parent

	@property
	def collections(self):
		return ( BatchEventsCollection(self), SessionsCollection(self) )


	def links(self):
		result = []
		link_names = [BATCH_EVENTS, ANALYTICS_SESSION, ANALYTICS_SESSIONS, SYNC_PARAMS]
		for name in link_names:
			link = Link(ANALYTICS, rel=name, elements=(name,))
			link.__name__ = link.target
			link.__parent__ = self.__parent__
			interface.alsoProvides(link, ILocation)
			result.append(link)
		return result

@interface.implementer(ICollection)
class BatchEventsCollection(object):

	__name__ = 'batch_events'
	name = alias('__name__', __name__)

	accepts = ()

	def __init__(self, parent):
		self.__parent__ = parent

	@Lazy
	def container(self):
		return ()

@interface.implementer(ICollection)
class SessionsCollection(object):

	__name__ = 'sessions'
	name = alias('__name__', __name__)

	accepts = ()

	def __init__(self, parent):
		self.__parent__ = parent

	@Lazy
	def container(self):
		return ()

