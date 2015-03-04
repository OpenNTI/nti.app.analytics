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

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.contenttypes.courses.interfaces import ICourseInstance
from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode

from nti.dataserver.contenttypes.forums.interfaces import ITopic

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator
from nti.externalization.externalization import to_external_object

from nti.links.links import Link

from nti.analytics import has_analytics
from nti.analytics.progress import get_topic_progress

LINKS = StandardExternalFields.LINKS

@component.adapter(ICourseOutlineContentNode)
@interface.implementer(IExternalMappingDecorator)
class _CourseOutlineNodeProgressLinkDecorator(AbstractAuthenticatedRequestAwareDecorator):
	"""
	Return a link on the content node in which the client can retrieve
	progress information for a user.
	"""

	def _do_decorate_external(self, context, result):
		if has_analytics():
			links = result.setdefault(LINKS, [])
			link = Link( context, rel="Progress", elements=('Progress',) )
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

	def _do_decorate_external(self, context, result):
		if has_analytics():
			links = result.setdefault(LINKS, [])
			link = Link( context, rel="VideoProgress", elements=('VideoProgress',) )
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

	def _do_decorate_external(self, context, result):
		if has_analytics():
			progress = get_topic_progress( self.remoteUser, context )
			result['Progress'] = to_external_object( progress )
