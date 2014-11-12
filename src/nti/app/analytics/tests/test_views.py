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

from zope import component

from nti.dataserver.tests import mock_dataserver

from nti.dataserver.users import User

from nti.analytics.model import CourseCatalogViewEvent
from nti.analytics.model import ResourceEvent
from nti.analytics.model import BlogViewEvent
from nti.analytics.model import NoteViewEvent
from nti.analytics.model import TopicViewEvent
from nti.analytics.model import SkipVideoEvent
from nti.analytics.model import BatchResourceEvents

from nti.externalization import internalization
from nti.externalization.externalization import toExternalObject

from hamcrest import assert_that
from hamcrest import has_length
from hamcrest import is_
from hamcrest import none
from hamcrest import not_none
from hamcrest import contains_inanyorder
from hamcrest import has_key

from nti.app.testing.decorators import WithSharedApplicationMockDS
from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.analytics import identifier

from nti.analytics.model import AnalyticsSessions
from nti.analytics.model import AnalyticsSession

from nti.analytics.sessions import _get_cookie_id
from nti.analytics.sessions import get_current_session_id
from nti.analytics.sessions import ANALYTICS_SESSION_HEADER

from nti.analytics.database.database import AnalyticsDB
from nti.analytics.database import interfaces as analytic_interfaces
from nti.analytics.database.boards import TopicsViewed
from nti.analytics.database.blogs import BlogsViewed
from nti.analytics.database.resource_tags import NotesViewed
from nti.analytics.database.enrollments import CourseCatalogViews
from nti.analytics.database.resource_views import VideoEvents
from nti.analytics.database.resource_views import CourseResourceViews
from nti.analytics.database.sessions import Sessions
from nti.analytics.database.sessions import CurrentSessions

from nti.analytics.tests import TestIdentifier

from nti.app.analytics.tests import LegacyInstructedCourseApplicationTestLayer

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
				patch_object( identifier._DSIdentifier, 'get_id', TestIdentifier.get_id ),
				patch_object( identifier._NtiidIdentifier, 'get_id', TestIdentifier.get_id ),
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



class TestAnalyticsSession( _AbstractTestViews ):

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	def test_session( self ):
		results = self.session.query( Sessions ).all()
		assert_that( results, has_length( 0 ) )
		results = self.session.query( CurrentSessions ).all()
		assert_that( results, has_length( 0 ) )

		# New session
		session_url = '/dataserver2/analytics/analytics_session'
		self.testapp.post_json( session_url,
								None,
								status=200 )

		cookie_id = _get_cookie_id( self.testapp )
		assert_that( cookie_id, is_( 1 ))

		results = self.session.query( Sessions ).all()
		assert_that( results, has_length( 1 ) )
		results = self.session.query( CurrentSessions ).all()
		assert_that( results, has_length( 1 ) )

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
		results = self.session.query( CurrentSessions ).all()
		assert_that( results, has_length( 1 ) )

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user( self.extra_environ_default_user )
			current_session_id = get_current_session_id( user )
			assert_that( current_session_id, none() )

		# End our session
		end_session_url = '/dataserver2/analytics/end_analytics_session'

		self.testapp.post_json( end_session_url,
								{ 'session_id' : 2 },
								status=204 )

		# This cookie is set to expire.
		# How to test that?
# 		cookie_id = _get_cookie_id( self.testapp )
# 		assert_that( cookie_id, none() )

		results = self.session.query( CurrentSessions ).all()
		assert_that( results, has_length( 0 ) )
		session_record = self.session.query( Sessions ).filter( Sessions.session_id == 2 ).first()
		assert_that( session_record, not_none() )
		assert_that( session_record.end_time, not_none() )

		with mock_dataserver.mock_db_trans(self.ds):
			user = User.get_user( self.extra_environ_default_user )
			current_session_id = get_current_session_id( user )
			assert_that( current_session_id, none() )

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	def test_sessions( self ):
		results = self.session.query( Sessions ).all()
		assert_that( results, has_length( 0 ) )
		results = self.session.query( CurrentSessions ).all()
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
		results = self.session.query( CurrentSessions ).all()
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

		db_session = self.session.query( CurrentSessions ).filter( CurrentSessions.session_id == session_id ).one()
		assert_that( db_session, not_none() )

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

		db_session = self.session.query( CurrentSessions ).filter( CurrentSessions.session_id == session_id ).first()
		assert_that( db_session, none() )

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

