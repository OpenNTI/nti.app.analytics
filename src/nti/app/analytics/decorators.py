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

from nti.assessment.interfaces import IQAssignment
from nti.assessment.interfaces import IQuestionSet

from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode

from nti.dataserver.links import Link

from nti.externalization.interfaces import IExternalObjectDecorator
from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.analytics.interfaces import IProgress

LINKS = StandardExternalFields.LINKS

PROGRESS_NAME = 'AbsoluteProgress'
PROGRESS_MAX_NAME = 'MaxProgressPossible'
HAS_PROGRESS = 'HasProgress'

@component.adapter(IQAssignment)
@component.adapter(IQuestionSet)
@interface.implementer(IExternalObjectDecorator)
class _AssignmentProgressNodeDecorator(AbstractAuthenticatedRequestAwareDecorator):
	"""
	Decorate the context with the user's relative progress in said context.
	"""

	def _do_decorate_external(self, context, result):
		user = self.remoteUser if self._is_authenticated else None
		if not user:
			return

		progress = component.queryMultiAdapter( (user, context), IProgress )

		if progress:
			result[PROGRESS_NAME] = progress.AbsoluteProgress
			result[PROGRESS_MAX_NAME] = progress.MaxProgressPossible
			result[HAS_PROGRESS] = progress.HasProgress

@component.adapter(ICourseOutlineContentNode)
@interface.implementer(IExternalMappingDecorator)
class _CourseOutlineNodeProgressLinkDecorator(AbstractAuthenticatedRequestAwareDecorator):
	"""
	Return a link on the content node in which the client can retrieve
	progress information for a user.
	"""

	def _do_decorate_external(self, context, result):
		# TODO Should I specify etag and last mod here?
		links = result.setdefault(LINKS, [])
		link = Link( context, rel="Progress", elements=('Progress',) )
		interface.alsoProvides(link, ILocation)
		link.__name__ = ''
		link.__parent__ = context
		links.append(link)
