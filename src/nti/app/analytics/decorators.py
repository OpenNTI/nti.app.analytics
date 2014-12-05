#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Externalization decorators.

.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope import interface
from zope.location.interfaces import ILocation

from nti.app.analytics.interfaces import IAnalyticsWorkspace

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode

from nti.dataserver.links import Link

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator
from nti.externalization.interfaces import IExternalObjectDecorator
from nti.externalization.singleton import SingletonDecorator

LINKS = StandardExternalFields.LINKS
BATCH_EVENT_SIZE_NAME = 'RecommendedBatchEventsSize'
BATCH_EVENT_SIZE = 100
BATCH_EVENT_FREQUENCY_NAME = 'RecommendedBatchEventsSendFrequency'
# In seconds
BATCH_EVENT_FREQUENCY = 60

@component.adapter(ICourseOutlineContentNode)
@interface.implementer(IExternalMappingDecorator)
class _CourseOutlineNodeProgressLinkDecorator(AbstractAuthenticatedRequestAwareDecorator):
	"""
	Return a link on the content node in which the client can retrieve
	progress information for a user.
	"""

	def _do_decorate_external(self, context, result):
		links = result.setdefault(LINKS, [])
		link = Link( context, rel="Progress", elements=('Progress',) )
		interface.alsoProvides(link, ILocation)
		link.__name__ = ''
		link.__parent__ = context
		links.append(link)

@component.adapter(IAnalyticsWorkspace)
@interface.implementer(IExternalObjectDecorator)
class _AnalyticsMetaWorkspaceDecorator(object):

	__metaclass__ = SingletonDecorator

	def decorateExternalObject( self, original, external ):
		external[BATCH_EVENT_SIZE_NAME] = BATCH_EVENT_SIZE
		external[BATCH_EVENT_FREQUENCY_NAME] = BATCH_EVENT_FREQUENCY
