#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import unittest
import fudge

import time
from datetime import datetime

from zope import component
from nti.analytics.database.database import AnalyticsDB
from nti.analytics.database import interfaces as analytic_interfaces

from nti.analytics.model import CourseCatalogViewEvent
from nti.analytics.model import ResourceEvent
from nti.analytics.model import BlogViewEvent
from nti.analytics.model import NoteViewEvent
from nti.analytics.model import TopicViewEvent
from nti.analytics.model import SkipVideoEvent
from nti.analytics.model import BatchResourceEvents

from nti.externalization.externalization import toExternalObject

from hamcrest import assert_that
from hamcrest import has_length
from hamcrest import none
from hamcrest import not_none
from hamcrest import is_
from hamcrest import has_entry

from nti.app.testing.decorators import WithSharedApplicationMockDS
from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.analytics.database.boards import TopicsViewed
from nti.analytics.database.blogs import BlogsViewed
from nti.analytics.database.resource_tags import NotesViewed
from nti.analytics.database.enrollments import CourseCatalogViews
from nti.analytics.database.resource_views import VideoEvents
from nti.analytics.database.resource_views import CourseResourceViews

from . import LegacyInstructedCourseApplicationTestLayer

class TestBatchEvents( ApplicationLayerTest ):

	layer = LegacyInstructedCourseApplicationTestLayer

	def setUp(self):
		self.db = AnalyticsDB( dburi='sqlite://' )
		component.getGlobalSiteManager().registerUtility( self.db, analytic_interfaces.IAnalyticsDB )
		self.session = self.db.session

	def tearDown(self):
		component.getGlobalSiteManager().unregisterUtility( self.db, provided=analytic_interfaces.IAnalyticsDB )
		self.session.close()

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
	@fudge.patch( 'nti.analytics.resource_views._get_object' )
	@fudge.patch( 'nti.analytics.resource_views._get_course' )
 	def test_batch_event( self, mock_get_object, mock_get_course ):
 		mock_parent = mock_get_object.is_callable().returns_fake()
 		mock_parent.has_attr( __parent__=201 )
 		mock_parent.has_attr( containerId=333 )

 		mock_course = mock_get_course.is_callable().returns_fake()
 		mock_course.has_attr( intid=999 )

 		timestamp = time.mktime( datetime.utcnow().timetuple() )
		user = 'sjohnson@nextthought.com'
		course = 'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.course_info'
		context_path = 'DASHBOARD'
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
						time_length=time_length)

		note_event = NoteViewEvent(user=user,
						timestamp=timestamp,
						course=course,
						note_id=note_id,
						time_length=time_length)

		topic_event = TopicViewEvent(user=user,
						timestamp=timestamp,
						course=course,
						topic_id=topic_id,
						time_length=time_length)

		course_catalog_event = CourseCatalogViewEvent(user=user,
						timestamp=timestamp,
						course=course,
						time_length=time_length)

		video_event = SkipVideoEvent(user=user,
						timestamp=timestamp,
						course=course,
						context_path=context_path,
						resource_id=resource_id,
						time_length=time_length,
						video_start_time=video_start_time,
						video_end_time=video_end_time,
						with_transcript=with_transcript)

		resource_event = ResourceEvent(user=user,
							timestamp=timestamp,
							course=course,
							context_path=context_path,
							resource_id=resource_id,
							time_length=time_length)

		io = BatchResourceEvents( events=[ 	video_event, resource_event, course_catalog_event,
											blog_event, note_event, topic_event ] )

		ext_obj = toExternalObject(io)

 		# Upload our events
 		batch_url = '/dataserver2/analytics/batch_events'
		res = self.testapp.post_json( 	batch_url,
										ext_obj,
										status=200 )

		results = self.session.query( VideoEvents ).all()
		assert_that( results, has_length( 1 ) )

		results = self.session.query( CourseCatalogViews ).all()
		assert_that( results, has_length( 1 ) )

		results = self.session.query( CourseResourceViews ).all()
		assert_that( results, has_length( 1 ) )

		# TODO Broken until we mock up parent ref lookups.
# 		results = self.session.query( BlogsViewed ).all()
# 		assert_that( results, has_length( 1 ) )
#
# 		results = self.session.query( NotesViewed ).all()
# 		assert_that( results, has_length( 1 ) )
#
# 		results = self.session.query( TopicsViewed ).all()
# 		assert_that( results, has_length( 1 ) )
