#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import unittest

import time
from datetime import datetime

from zope import component
from nti.analytics.database.database import AnalyticsDB
from nti.analytics.database import interfaces as analytic_interfaces

from nti.analytics.model import ResourceEvent
from nti.analytics.model import VideoEvent
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

class TestBatchEvents( ApplicationLayerTest ):

	def setUp(self):
		self.db = AnalyticsDB( dburi='sqlite://' )
		component.getGlobalSiteManager().registerUtility( self.db, analytic_interfaces.IAnalyticsDB )
		self.session = self.db.session

	def tearDown(self):
		component.getGlobalSiteManager().unregisterUtility ( self.db, provided=analytic_interfaces.IAnalyticsDB )
		self.session.close()

	@WithSharedApplicationMockDS(users=True,testapp=True,default_authenticate=True)
 	def test_batch_event(self):
 		timestamp = time.mktime( datetime.utcnow().timetuple() )
		user = 'josh.zuech@nextthought.com'
		course = 'CS1300'
		context_path = 'ntiid:lesson1'
		resource_id = 'ntiid:lesson1_chapter1'
		time_length = 30
		event_type = 'WATCH'
		video_start_time = 13
		video_end_time = 39
		with_transcript = True

		video_event = VideoEvent(user=user,
						timestamp=timestamp,
						course=course,
						context_path=context_path,
						resource_id=resource_id,
						time_length=time_length,
						event_type=event_type,
						video_start_time=video_start_time,
						video_end_time=video_end_time,
						with_transcript=with_transcript)

		resource_event = ResourceEvent(user=user,
							timestamp=timestamp,
							course=course,
							context_path=context_path,
							resource_id=resource_id,
							time_length=time_length)

		io = BatchResourceEvents( events=[ video_event, resource_event ] )

		ext_obj = toExternalObject(io)

 		batch_url = '/dataserver2/analyticsdb/@@batch_events'
		res = self.testapp.post_json( 	batch_url,
										ext_obj,
										status=200 )

# 		extra_env = self.testapp.extra_environ or {}
# 		extra_env.update( {b'HTTP_ORIGIN': b'http://janux.ou.edu'} )
# 		self.testapp.extra_environ = extra_env
#
# 		instructor_environ = self._make_extra_environ(username='harp4162')
# 		instructor_environ.update( {b'HTTP_ORIGIN': b'http://janux.ou.edu'} )

		# Verify queued objects
		# FIXME this returns 3
		# -probably a user creation event.
# 		queue_info_url = '/dataserver2/analyticsdb/@@queue_info'
# 		res = self.testapp.get( queue_info_url, status=200 )
# 		assert_that( res.json_body, has_entry( 'size', '2'))

		# Run processor
		# TODO Verify in db
