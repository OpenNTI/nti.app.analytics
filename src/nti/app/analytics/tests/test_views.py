#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import fudge
from fudge import patch_object

import time
from datetime import datetime

from webob.datetime_utils import serialize_date

from zope import component

from hamcrest import assert_that
from hamcrest import has_length
from hamcrest import is_
from hamcrest import none
from hamcrest import not_none
from hamcrest import contains_inanyorder
from hamcrest import contains
from hamcrest import has_key
from hamcrest import has_entry
from hamcrest import has_entries

from nti.contenttypes.courses.interfaces import ICourseInstance

from nti.dataserver.tests import mock_dataserver

from nti.dataserver.users import User

from nti.externalization import internalization
from nti.externalization.externalization import toExternalObject

from nti.assessment.assignment import QAssignment

from nti.app.testing.decorators import WithSharedApplicationMockDS
from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.analytics import identifier

from nti.analytics.common import timestamp_type

from nti.analytics.interfaces import DEFAULT_ANALYTICS_BATCH_SIZE
from nti.analytics.interfaces import DEFAULT_ANALYTICS_FREQUENCY

from nti.analytics.model import CourseCatalogViewEvent
from nti.analytics.model import ResourceEvent
from nti.analytics.model import BlogViewEvent
from nti.analytics.model import NoteViewEvent
from nti.analytics.model import TopicViewEvent
from nti.analytics.model import SkipVideoEvent
from nti.analytics.model import WatchVideoEvent
from nti.analytics.model import BatchResourceEvents
from nti.analytics.model import AnalyticsSessions
from nti.analytics.model import AnalyticsSession

from nti.analytics.sessions import _get_cookie_id
from nti.analytics.sessions import get_current_session_id
from nti.analytics.sessions import ANALYTICS_SESSION_HEADER

from nti.analytics.database.database import AnalyticsDB
from nti.analytics.database import interfaces as analytic_interfaces
from nti.analytics.database.assessments import AssignmentsTaken
from nti.analytics.database.boards import TopicsViewed
from nti.analytics.database.blogs import BlogsViewed
from nti.analytics.database.enrollments import CourseCatalogViews
from nti.analytics.database.resource_tags import NotesViewed
from nti.analytics.database.resource_views import VideoEvents
from nti.analytics.database.resource_views import CourseResourceViews
from nti.analytics.database.resource_views import create_course_resource_view
from nti.analytics.database.resource_views import create_video_event
from nti.analytics.database.root_context import get_root_context_id
from nti.analytics.database.sessions import Sessions
from nti.analytics.database.users import create_user

from nti.app.analytics import SYNC_PARAMS

from nti.analytics.tests import TestIdentifier

from nti.app.analytics.tests import LegacyInstructedCourseApplicationTestLayer

from nti.ntiids.ntiids import find_object_with_ntiid

from nti.testing.time import time_monotonically_increases

timestamp = time.mktime( datetime.utcnow().timetuple() )
user = 'sjohnson@nextthought.com'
course = 'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.course_info'
context_path = ['DASHBOARD', 'ntiid:tag_blah']
resource_id = 'tag:nextthought.com,2011-10:OU-HTML-ENGR1510_Intro_lesson1'
time_length = 30
video_start_time = 13
video_end_time = 39
with_transcript = True

blog_id='1111'
note_id='with_container'
topic_id='with_parent'

blog_event = BlogViewEvent(user=user,
				timestamp=timestamp,
				blog_id=blog_id,
				Duration=time_length)

note_event = NoteViewEvent(user=user,
				timestamp=timestamp,
				RootContextID=course,
				note_id=note_id,
				Duration=time_length)

topic_event = TopicViewEvent(user=user,
				timestamp=timestamp,
				RootContextID=course,
				topic_id=topic_id,
				Duration=time_length)

course_catalog_event = CourseCatalogViewEvent(user=user,
				timestamp=timestamp,
				RootContextID=course,
				Duration=time_length)

video_event = SkipVideoEvent(user=user,
				timestamp=timestamp,
				RootContextID=course,
				context_path=context_path,
				resource_id=resource_id,
				Duration=time_length,
				video_start_time=video_start_time,
				video_end_time=video_end_time,
				with_transcript=with_transcript)

# Essentially a video start event
watch_video_event = WatchVideoEvent(user=user,
				timestamp=timestamp,
				RootContextID=course,
				context_path=context_path,
				resource_id=resource_id,
				Duration=None,
				video_start_time=video_start_time,
				video_end_time=None,
				with_transcript=with_transcript)

resource_event = ResourceEvent(user=user,
					timestamp=timestamp,
					RootContextID=course,
					context_path=context_path,
					resource_id=resource_id,
					Duration=time_length)

def _internalize( ext ):
	factory = internalization.find_factory_for( ext )
	_object = factory()
	internalization.update_from_external_object( _object, ext )
	return _object

class _AbstractTestViews( ApplicationLayerTest ):

	layer = LegacyInstructedCourseApplicationTestLayer

	def setUp(self):
		self.db = AnalyticsDB( dburi='sqlite://', testmode=True, defaultSQLite=True )
		component.getGlobalSiteManager().registerUtility( self.db, analytic_interfaces.IAnalyticsDB )
		self.session = self.db.session

		self.patches = [
			patch_object( identifier.RootContextId, 'get_id', TestIdentifier.get_id ),
			patch_object( identifier._DSIdentifier, 'get_id', TestIdentifier.get_id ),
			patch_object( identifier._NtiidIdentifier, 'get_id', TestIdentifier.get_id ),
			patch_object( identifier.RootContextId, 'get_object', TestIdentifier.get_object ),
			patch_object( identifier._DSIdentifier, 'get_object', TestIdentifier.get_object ),
			patch_object( identifier._NtiidIdentifier, 'get_object', TestIdentifier.get_object ) ]

	def tearDown(self):
		component.getGlobalSiteManager().unregisterUtility( self.db, provided=analytic_interfaces.IAnalyticsDB )
		self.session.close()

		for patch in self.patches:
			patch.restore()

class TestBatchEvents( _AbstractTestViews ):

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	@fudge.patch( 'nti.analytics.resource_views._get_object' )
	@fudge.patch( 'nti.analytics.resource_views._get_course' )
	@fudge.patch( 'nti.analytics.database.blogs._get_blog_id' )
	@fudge.patch( 'nti.analytics.database.resource_tags._get_note_id' )
	@fudge.patch( 'nti.analytics.database.boards._get_forum_id_from_forum' )
	@fudge.patch( 'nti.analytics.database.boards._get_topic_id_from_topic' )
	def test_batch_event( self, mock_get_object, mock_get_course, mock_get_blog, mock_get_note, mock_get_forum, mock_get_topic ):
		mock_parent = mock_get_object.is_callable().returns_fake()
		mock_parent.has_attr( __parent__=201 )
		mock_parent.has_attr( containerId=333 )

		mock_course = mock_get_course.is_callable().returns_fake()
		mock_course.has_attr( intid=999 )

		mock_get_blog.is_callable().returns( 1 )
		mock_get_note.is_callable().returns( 2 )
		mock_get_forum.is_callable().returns( 3 )
		mock_get_topic.is_callable().returns( 4 )

		# Event specified session id
		course_catalog_session_id = 11111
		course_catalog_event.SessionID = course_catalog_session_id

		io = BatchResourceEvents( events=[ 	video_event, resource_event, course_catalog_event,
											blog_event, note_event, topic_event ] )

		ext_obj = toExternalObject(io)

		# Add a session header
		session_id = 9999
		headers = { ANALYTICS_SESSION_HEADER : str( session_id ) }

		# Upload our events
		batch_url = '/dataserver2/analytics/batch_events'
		self.testapp.post_json( batch_url,
								ext_obj,
								headers=headers,
								status=200 )

		results = self.session.query( VideoEvents ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		results = self.session.query( CourseCatalogViews ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( course_catalog_session_id ))

		results = self.session.query( CourseResourceViews ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		results = self.session.query( BlogsViewed ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		results = self.session.query( NotesViewed ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		results = self.session.query( TopicsViewed ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		# We should be able to send the same events without error or duplicates
		# in database.
		self.testapp.post_json( batch_url,
								ext_obj,
								headers=headers,
								status=200 )

		results = self.session.query( VideoEvents ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		results = self.session.query( CourseCatalogViews ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( course_catalog_session_id ))

		results = self.session.query( CourseResourceViews ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		results = self.session.query( BlogsViewed ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		results = self.session.query( NotesViewed ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		results = self.session.query( TopicsViewed ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, is_( session_id ))

		# Send a video start event successfully
		io = BatchResourceEvents( events=[ watch_video_event ] )
		ext_obj = toExternalObject(io)
		session_id = 9999
		headers = { ANALYTICS_SESSION_HEADER : str( session_id ) }

		self.testapp.post_json( batch_url,
								ext_obj,
								headers=headers,
								status=200 )

		results = self.session.query( VideoEvents ).all()
		assert_that( results, has_length( 2 ) )

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	@fudge.patch( 'nti.analytics.resource_views._get_object' )
	@fudge.patch( 'nti.analytics.resource_views._get_course' )
	def test_malformed_event( self, mock_get_object, mock_get_course ):
		mock_parent = mock_get_object.is_callable().returns_fake()
		mock_parent.has_attr( __parent__=201 )
		mock_parent.has_attr( containerId=333 )

		mock_course = mock_get_course.is_callable().returns_fake()
		mock_course.has_attr( intid=999 )

		# This event is now malformed
		resource_event.RootContextID = None

		io = BatchResourceEvents( events=[ 	video_event, resource_event, course_catalog_event ] )

		ext_obj = toExternalObject(io)

		# Upload our events
		batch_url = '/dataserver2/analytics/batch_events'
		self.testapp.post_json( batch_url,
								ext_obj,
								status=200 )

		results = self.session.query( VideoEvents ).all()
		assert_that( results, has_length( 1 ) )

		results = self.session.query( CourseCatalogViews ).all()
		assert_that( results, has_length( 1 ) )

		# We insert all but the single malformed event
		results = self.session.query( CourseResourceViews ).all()
		assert_that( results, has_length( 0 ) )

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	def test_batch_params( self ):
		batch_url = '/dataserver2/analytics/' + SYNC_PARAMS
		result = self.testapp.get( batch_url, status=200 )
		result = result.json_body
		assert_that( result, has_entries(
								'RecommendedAnalyticsSyncInterval', DEFAULT_ANALYTICS_FREQUENCY,
								'RecommendedBatchEventsSendFrequency', DEFAULT_ANALYTICS_FREQUENCY,
								'RecommendedBatchEventsSize', DEFAULT_ANALYTICS_BATCH_SIZE,
								'RecommendedBatchSessionsSendFrequency', DEFAULT_ANALYTICS_FREQUENCY,
								'RecommendedBatchSessionsSize', DEFAULT_ANALYTICS_BATCH_SIZE ) )

class TestAnalyticsSession( _AbstractTestViews ):

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	def test_session( self ):
		results = self.session.query( Sessions ).all()
		assert_that( results, has_length( 0 ) )

		# New session
		session_url = '/dataserver2/analytics/analytics_session'
		self.testapp.post_json( session_url,
								None,
								status=200 )

		cookie_id = first_session_id = _get_cookie_id( self.testapp )
		assert_that( cookie_id, is_( 1 ))

		results = self.session.query( Sessions ).all()
		assert_that( results, has_length( 1 ) )
		assert_that( results[0].session_id, first_session_id )
		assert_that( results[0].end_time, none() )

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user( self.extra_environ_default_user )
			current_session_id = get_current_session_id( user )
			assert_that( current_session_id, none() )

		# New session #2
		self.testapp.post_json( session_url,
								None,
								status=200 )

		cookie_id = _get_cookie_id( self.testapp )
		assert_that( cookie_id, is_( 2 ))

		results = self.session.query( Sessions ).all()
		assert_that( results, has_length( 2 ) )
		# This last call implicitly ends the previous session.
		first_session = self.session.query( Sessions ).filter(
								Sessions.session_id == first_session_id ).one()
		assert_that( first_session.end_time, not_none() )

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user( self.extra_environ_default_user )
			current_session_id = get_current_session_id( user )
			assert_that( current_session_id, none() )

		# End our session
		end_session_url = '/dataserver2/analytics/end_analytics_session'

		timestamp = timestamp_type( 1 )
		self.testapp.post_json( end_session_url,
								{ 'session_id' : 2, 'timestamp' : 1 },
								status=204 )

		# This cookie is set to expire.
		# How to test that?
# 		cookie_id = _get_cookie_id( self.testapp )
# 		assert_that( cookie_id, none() )

		session_record = self.session.query( Sessions ).filter( Sessions.session_id == 2 ).first()
		assert_that( session_record, not_none() )
		assert_that( session_record.end_time, is_( timestamp ) )

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user( self.extra_environ_default_user )
			current_session_id = get_current_session_id( user )
			assert_that( current_session_id, none() )

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	def test_sessions( self ):
		results = self.session.query( Sessions ).all()
		assert_that( results, has_length( 0 ) )

		# No end time
		session = AnalyticsSession( SessionStartTime=timestamp )
		sessions = [ session, session, session ]

		session_count = len( sessions )
		io = AnalyticsSessions( sessions=sessions )
		ext_obj = toExternalObject(io)

		# Send our sessions over
		session_url = '/dataserver2/analytics/sessions'
		result = self.testapp.post_json( session_url,
										ext_obj,
										status=200 )

		new_sessions = [ _internalize( x ) for x in result.json_body ]
		session_ids = [x.SessionID for x in new_sessions]
		assert_that( new_sessions, has_length( session_count ))
		assert_that( session_ids, contains_inanyorder( 1, 2, 3 ))

		results = self.session.query( Sessions ).all()
		assert_that( results, has_length( 3 ) )

		# This is header driven.
		current_session_id = get_current_session_id( user )
		assert_that( current_session_id, none() )

		# Now update with an endtime
		session = new_sessions[0]
		session_id = session.SessionID

		db_session = self.session.query( Sessions ).filter( Sessions.session_id == session_id ).one()
		assert_that( db_session, not_none() )
		assert_that( db_session.end_time, none() )

		end_time = timestamp + 1
		session.SessionEndTime = end_time
		sessions = [ session ]
		io = AnalyticsSessions( sessions=sessions )
		ext_obj = toExternalObject(io)

		session_url = '/dataserver2/analytics/sessions'
		result = self.testapp.post_json( session_url,
										ext_obj,
										status=200 )

		new_sessions = [ _internalize( x ) for x in result.json_body ]
		session_ids = [x.SessionID for x in new_sessions]
		assert_that( session_ids, has_length( 1 ))
		assert_that( session_ids[0], is_( session_id ))

		db_session = self.session.query( Sessions ).filter( Sessions.session_id == session_id ).one()
		assert_that( db_session, not_none() )
		assert_that( db_session.end_time, not_none() )

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	def test_update_session_with_invalid( self ):
		end_time = timestamp + 1

		new_session = AnalyticsSession( SessionStartTime=timestamp )
		session_with_made_up_id = AnalyticsSession(
							SessionID=99999, SessionStartTime=timestamp, SessionEndTime=end_time )
		sessions = [ new_session, session_with_made_up_id ]

		session_count = len( sessions )
		io = AnalyticsSessions( sessions=sessions )
		ext_obj = toExternalObject(io)

		# Send our sessions over
		session_url = '/dataserver2/analytics/sessions'
		result = self.testapp.post_json( session_url,
										ext_obj,
										status=200 )

		results = result.json_body
		assert_that( results, has_length( session_count ))

		# First session is valid
		valid_session = _internalize( results[0] )
		assert_that( valid_session, not_none() )
		assert_that( valid_session.SessionID, is_( 1 ))

		# Next is an error
		key = 'Error'
		assert_that( results[1], has_key( key ))

class TestProgressView( _AbstractTestViews ):

	def _create_course(self):
		content_unit = find_object_with_ntiid( course )
		course_obj = self.course = ICourseInstance( content_unit )
		get_root_context_id( self.db, course_obj, create=True )

	def _create_video_event(self, user, resource_val, max_time_length=None):
		test_session_id = 1
		time_length = 30
		video_event_type = 'WATCH'
		video_start_time = 30
		video_end_time = 60
		with_transcript = True
		event_time = time.time()
		context_path = ['Blah', 'Bleh' ]
		create_video_event( user,
							test_session_id, event_time,
							self.course, context_path,
							resource_val, time_length, max_time_length,
							video_event_type, video_start_time,
							video_end_time,  with_transcript )


	def _create_resource_view(self, user, resource_val):
		test_session_id = 1
		time_length = 30
		event_time = time.time()
		context_path = ['Blah', 'Bleh' ]
		create_course_resource_view( user,
									test_session_id, event_time,
									self.course, context_path,
									resource_val, time_length )

	def _get_assignment(self):
		new_assignment = QAssignment()
		new_assignment.ntiid = self.assignment_id = 'tag:ntiid1'
		return new_assignment

	def _install_user(self, user_id):
		with mock_dataserver.mock_db_trans(self.ds):
			self.user = User.get_user( user_id )
			self.user_id = create_user( self.user ).user_id
			return self.user

	def _install_assignment(self, assignment_id):
		db = self.db
		new_object = AssignmentsTaken(
									user_id=self.user_id,
									session_id=2,
									timestamp=timestamp_type( time.time() ),
									course_id=1,
									assignment_id=assignment_id,
									submission_id=2,
									time_length=10 )
		db.session.add( new_object )
		db.session.flush()

	def _do_get_url(self, url, status=200, response=None):
		"""
		Gets the url, using the given response (if available)
		as last modified.
		"""
		if response and response.last_modified:
			response = self.testapp.get( url,
										headers={'If-Modified-Since':
												serialize_date( response.last_modified )},
										status=status )
		else:
			response = self.testapp.get( url, status=status )
		return response

	def _get_progress(self, status=200, response=None):
		progress_url = '/dataserver2/users/CLC3403.ou.nextthought.com/LegacyCourses/CLC3403/Outline/2/1/Progress'
		return self._do_get_url( progress_url, status, response )

	def _get_video_progress(self, status=200, response=None):
		progress_url = '/dataserver2/users/CLC3403.ou.nextthought.com/LegacyCourses/CLC3403/VideoProgress'
		return self._do_get_url( progress_url, status, response )

	def _setup_mocks( self, mock_adapter, mock_find_object, mock_validate, assignment_id ):
		mock_validate.is_callable().returns( True )
		mock_adapter.is_callable().returns( object() )

		def _get_assignment( key ):
			"Get our assignment, or fallback to ntiids lookup."
			if key == assignment_id:
				assignment_object = self._get_assignment()
				assignment_object.ntiid = assignment_id
				return assignment_object
			return find_object_with_ntiid( key )

		assignment_object = self._get_assignment()
		assignment_object.ntiid = assignment_id
		mock_find_object.is_callable().calls( _get_assignment )

	@time_monotonically_increases
	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	@fudge.patch( 	'nti.app.products.courseware.adapters._content_unit_to_course',
					'nti.ntiids.ntiids.find_object_with_ntiid',
					'dm.zope.schema.schema.Object._validate' )
	def test_progress( self, mock_adapter, mock_find_object, mock_validate ):
		video1 = 'tag:nextthought.com,2011-10:OU-NTIVideo-CLC3403_LawAndJustice.ntivideo.video_10.03'
		video2 = 'tag:nextthought.com,2011-10:OU-NTIVideo-CLC3403_LawAndJustice.ntivideo.video_10.02'
		resource1 = 'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.lec:10_LESSON'
		assignment1 = 'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.sec:QUIZ_10.01'

		self._setup_mocks(mock_adapter, mock_find_object, mock_validate, assignment1)

		# Empty progress/empty video progress
		response = self._get_progress()
		result = response.json_body['Items']
		assert_that( result, has_length( 0 ))

		video_response = self._get_video_progress()
		result = video_response.json_body['Items']
		assert_that( result, has_length( 0 ))

		user_id = 'sjohnson@nextthought.com'
		user = self._install_user( user_id )

		# Now a video event
		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user( user_id )
			self._create_course()
			self._create_video_event( user=user, resource_val=video1 )

		response = self._get_progress( response=response )
		result = response.json_body['Items']
		assert_that( result, has_length( 1 ))
		assert_that( result, contains( video1 ))

		video_progress = result.get( video1 )
		assert_that( video_progress, has_entry('MaxPossibleProgress', None ) )
		assert_that( video_progress, has_entry('AbsoluteProgress', 30 ) )
		assert_that( video_progress, has_entry('HasProgress', True ) )

		# Video progress for course
		video_response = self._get_video_progress()
		result = video_response.json_body['Items']
		assert_that( result, has_length( 1 ))
		assert_that( result, contains( video1 ))

		# Same video event
		max_progress = 120
		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user( user_id )
			self._create_video_event( user=user, resource_val=video1, max_time_length=max_progress )
		response = self._get_progress( response=response )

		result = response.json_body['Items']
		assert_that( result, has_length( 1 ))
		assert_that( result, has_key( video1 ))

		video_progress = result.get( video1 )
		assert_that( video_progress, has_entry('MaxPossibleProgress', max_progress ) )
		assert_that( video_progress, has_entry('AbsoluteProgress', 60 ) )
		assert_that( video_progress, has_entry('HasProgress', True ) )

		video_response = self._get_video_progress()
		result = video_response.json_body['Items']
		assert_that( result, has_length( 1 ))
		assert_that( result, contains( video1 ))

		# New video doesn't affect old video
		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user( user_id )
			self._create_video_event( user=user, resource_val=video2 )
		response = self._get_progress( response=response )

		result = response.json_body['Items']
		assert_that( result, has_length( 2 ))
		assert_that( result, contains_inanyorder( video1, video2 ))

		video_progress = result.get( video1 )
		assert_that( video_progress, has_entry('MaxPossibleProgress', max_progress ) )
		assert_that( video_progress, has_entry('AbsoluteProgress', 60 ) )
		assert_that( video_progress, has_entry('HasProgress', True ) )

		video_response = self._get_video_progress()
		result = video_response.json_body['Items']
		assert_that( result, has_length( 2 ))
		assert_that( result, contains_inanyorder( video1, video2 ))

		# Now a resource view
		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user( user_id )
			self._create_resource_view( user=user, resource_val=resource1 )
		response = self._get_progress( response=response )

		result = response.json_body['Items']
		assert_that( result, has_length( 3 ))
		assert_that( result, contains_inanyorder( video1, video2, resource1 ))

		resource_progress = result.get( resource1 )
		assert_that( resource_progress, has_entry('MaxPossibleProgress', 1 ) )
		assert_that( resource_progress, has_entry('AbsoluteProgress', 1 ) )
		assert_that( resource_progress, has_entry('HasProgress', True ) )

		# Now a 304
		self._get_progress( response=response, status=304 )

		# Now an assignment
		with mock_dataserver.mock_db_trans(self.ds):
			self._install_assignment( assignment1 )

		response = self._get_progress( response=response )

		result = response.json_body['Items']
		assert_that( result, has_length( 4 ))
		assert_that( result, contains_inanyorder( video1, video2, resource1, assignment1 ))

		resource_progress = result.get( assignment1 )
		assert_that( resource_progress, has_entry('MaxPossibleProgress', 1 ) )
		assert_that( resource_progress, has_entry('AbsoluteProgress', 1 ) )
		assert_that( resource_progress, has_entry('HasProgress', True ) )

		# Now a 304 again
		self._get_progress( response=response, status=304 )
