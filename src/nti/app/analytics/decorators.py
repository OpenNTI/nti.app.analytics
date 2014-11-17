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

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.assessment.interfaces import IQAssignment
from nti.assessment.interfaces import IQuestionSet

from nti.externalization.interfaces import IExternalObjectDecorator

from nti.analytics.interfaces import IProgress

PROGRESS_NAME = 'AbsoluteProgress'
PROGRESS_MAX_NAME = 'MaxProgressPossible'
HAS_PROGRESS = 'HasProgress'

@component.adapter(IQAssignment)
@component.adapter(IQuestionSet)
@interface.implementer(IExternalObjectDecorator)
class _AssignmentProgressNodeDecorator(AbstractAuthenticatedRequestAwareDecorator):

	def _do_decorate_external(self, context, result):
		user = self.remoteUser if self._is_authenticated else None
		if not user:
			return

		progress = component.queryMultiAdapter( (user, context), IProgress )

		if progress:
			result[PROGRESS_NAME] = progress.AbsoluteProgress
			result[PROGRESS_MAX_NAME] = progress.MaxProgressPossible
			result[HAS_PROGRESS] = progress.HasProgress

