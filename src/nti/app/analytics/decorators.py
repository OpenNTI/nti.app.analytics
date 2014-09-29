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

from nti.app.assessment.interfaces import ICourseAssignmentCatalog
from nti.app.assessment.interfaces  import get_course_assignment_predicate_for_user

from nti.externalization.interfaces import IExternalObjectDecorator

from nti.dataserver.traversal import find_interface

from nti.ntiids import ntiids

from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode
from nti.contenttypes.courses.interfaces import ICourseInstance

from nti.analytics.assessments import get_assignments_for_user

PROGRESS_NAME = 'NodeAssignmentProgressCountCompleted'
PROGRESS_MAX_NAME = 'NodeAssignmentProgressCountAvailable'

@interface.implementer(IExternalObjectDecorator)
@component.adapter(ICourseOutlineContentNode)
class _CourseOutlineAssignmentProgressNodeDecorator(AbstractAuthenticatedRequestAwareDecorator):

	def _do_decorate_external(self, context, result):
		# TODO Should we do this at the CourseOutlineNode level?
		# Can we since we're recursively pulling nodes?
		# TODO Improve with caching?
		user = self.remoteUser if self._is_authenticated else None
		course = find_interface( context, ICourseInstance )
		if not user or not course:
			return

		catalog = ICourseAssignmentCatalog( course )
		uber_filter = get_course_assignment_predicate_for_user( user, course )

		content_ntiid_to_assignments = dict()
		# Build up a map of content ntiids to assignments
		for asg in (x for x in catalog.iter_assignments() if uber_filter(x)):
			# The assignment's __parent__ is always the 'home'content unit.
			unit = asg.__parent__
			content_ntiid_to_assignments.setdefault( unit.ntiid, set() ).add( asg )

		ntiid = context.ContentNTIID
		content_unit = ntiids.find_object_with_ntiid( ntiid )

		def _recur_get_assignments( unit, accum ):
			found_assignments = content_ntiid_to_assignments.get( unit.ntiid )
			if found_assignments:
				accum.update( (x.ntiid for x in found_assignments) )
			# TODO Embedded as well?
			for child in unit.children:
				_recur_get_assignments( child, accum )

		assignments_for_node = set()
		_recur_get_assignments( content_unit, assignments_for_node )

		assignments_taken = get_assignments_for_user( user, course )
		assignments_taken = {x for x in assignments_taken
							if x.Submission.assignmentId in assignments_for_node}

		total_user_progress = len( assignments_taken )
		total_progress_max = len( assignments_for_node ) if assignments_for_node else 0

		result[PROGRESS_NAME] = total_user_progress
		result[PROGRESS_MAX_NAME] = total_progress_max

