# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from datetime import datetime

from hamcrest import assert_that
from hamcrest import not_none
from hamcrest import has_entry
from hamcrest import contains
from hamcrest import has_property
from hamcrest import has_length
from hamcrest import has_key

from fudge import patch_object

from nti.analytics import identifier

from nti.analytics.database import get_analytics_db
from nti.analytics.database.users import create_user
from nti.analytics.database.assessments import AssignmentsTaken
from nti.analytics.database.assessments import SelfAssessmentsTaken

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.assessment.assignment import QAssignment
from nti.assessment.question import QQuestionSet

from nti.contenttypes.courses.courses import CourseInstance
from nti.contenttypes.courses.outlines import CourseOutlineNode
from nti.contenttypes.courses.outlines import CourseOutlineContentNode

from nti.app.analytics.decorators import _CourseOutlineNodeProgressLinkDecorator
from nti.app.analytics.decorators import _AssignmentProgressNodeDecorator

from nti.dataserver.users import User

from nti.analytics.tests import NTIAnalyticsApplicationTestLayer
from nti.analytics.tests import TestIdentifier

from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

class TestDecorators( ApplicationLayerTest ):

	layer = NTIAnalyticsApplicationTestLayer

	def test_node_decorator(self):
		inst = CourseInstance()
		outline = inst.Outline
		node = CourseOutlineNode()
		outline.append(node)
		node2 = CourseOutlineContentNode(ContentNTIID='tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.lec:01_LESSON',
										AvailableBeginning=datetime.now() )
		result = {}
		decorator = _CourseOutlineNodeProgressLinkDecorator( object(), None )
		decorator._do_decorate_external( node2, result )

		assert_that( result, not_none() )
		assert_that(result, has_entry('Links',
								contains(has_property('rel', 'Progress' ))))


class TestAssignmentDecorators( ApplicationLayerTest ):

	layer = NTIAnalyticsApplicationTestLayer

	def setUp(self):
		self.analytics_db = get_analytics_db()

		self.patches = [
			patch_object( identifier.SessionId, 'get_id', TestIdentifier.get_id ),
			patch_object( identifier._DSIdentifier, 'get_id', TestIdentifier.get_id ),
			patch_object( identifier._NtiidIdentifier, 'get_id', TestIdentifier.get_id ) ]

	def tearDown(self):
		for patch in self.patches:
			patch.restore()

	def _get_assignment(self):
		new_assignment = QAssignment()
		new_assignment.ntiid = self.assignment_id = 'tag:ntiid1'
		return new_assignment

	def _get_self_assessment(self):
		assessment = QQuestionSet()
		assessment.ntiid = self.question_set_id = 'tag:question_set1'
		return assessment

	def _install_user(self, username):
		self.user = User.create_user( username=username, dataserver=self.ds )
		self.user_id = create_user( self.user ).user_id
		return self.user

	def _install_assignment(self, db):
		new_object = AssignmentsTaken( 	user_id=1,
									session_id=2,
									timestamp=datetime.utcnow(),
									course_id=1,
									assignment_id=self.assignment_id,
									submission_id=2,
									time_length=10 )
		db.session.add( new_object )
		db.session.flush()

	def _install_self_assessment(self, db, submit_id=1 ):
		new_object = SelfAssessmentsTaken( 	user_id=1,
										session_id=2,
										timestamp=datetime.utcnow(),
										course_id=1,
										assignment_id=self.question_set_id,
										submission_id=submit_id,
										time_length=10 )
		db.session.add( new_object )
		db.session.flush()

	@WithMockDSTrans
	def test_assessment_progress(self):
		user_id = 'sjohnson@nextthought.com'
		self._install_user( user_id )

		class Request(object):
			authenticated_userid = user_id
			_is_authenticated = True

		request = Request()
		assignment = self._get_assignment()

		decorator = _AssignmentProgressNodeDecorator( assignment, request )
		result = {}
		decorator._do_decorate_external( assignment, result )
		assert_that( result, has_length( 0 ))

		# Take assignment
		self._install_assignment( self.analytics_db )
		decorator = _AssignmentProgressNodeDecorator( assignment, request )
		result = {}
		decorator._do_decorate_external( assignment, result )
		assert_that( result, has_length( 1 ))
		assert_that( result, has_key( 'Progress' ))

		resource_progress = result.get( 'Progress' )
		assert_that( resource_progress, has_entry('MaxPossibleProgress', 1 ) )
		assert_that( resource_progress, has_entry('AbsoluteProgress', 1 ) )
		assert_that( resource_progress, has_entry('HasProgress', True ) )



