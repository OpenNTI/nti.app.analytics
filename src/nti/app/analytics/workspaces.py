#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Implementation of an Atom/OData workspace and collection
for badges.

.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface
from zope import component
from zope.container import contained
from zope.location import interfaces as loc_interfaces

import nti.appserver.interfaces as app_interfaces

from nti.dataserver import links
from nti.dataserver import interfaces as nti_interfaces

from nti.utils.property import Lazy
from nti.utils.property import alias

from . import ANALYTICS
from . import ANALYTICS_TITLE
from . import BATCH_EVENTS

from nti.app.analytics.interfaces import IAnalyticsWorkspace

@interface.implementer(app_interfaces.IWorkspace)
@component.adapter(app_interfaces.IUserService)
def AnalyticsWorkspace( user_service ):
	analytics_ws = _AnalyticsWorkspace( parent=user_service.__parent__ )
	assert analytics_ws.__parent__
	return analytics_ws

@interface.implementer(IAnalyticsWorkspace)
@component.adapter(nti_interfaces.IDataserverFolder)
class _AnalyticsWorkspace(contained.Contained):
	"""
	A workspace that currently, does not have any collections, but
	exposes links that may be useful to analytics clients.
	"""

	__name__ = ANALYTICS_TITLE
	name = alias('__name__', __name__)

	__parent__ = None

	collections = ()

	def __init__(self, parent=None):
		super(_AnalyticsWorkspace,self).__init__()
		if parent:
			self.__parent__ = parent

	@property
	def links(self):
		result = []
		#ds2/analytics/@@batch_events
		link = links.Link(ANALYTICS, rel=BATCH_EVENTS, elements=(BATCH_EVENTS,))
		link.__name__ = link.target
		link.__parent__ = self.__parent__
		interface.alsoProvides(link, loc_interfaces.ILocation)
		result.append(link)
		return result
