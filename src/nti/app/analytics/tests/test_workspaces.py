#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import has_item
from hamcrest import has_entries
from hamcrest import assert_that

from nti.appserver.workspaces import UserService

from nti.app.analytics import ANALYTICS_TITLE
from nti.app.analytics import BATCH_EVENTS
from nti.app.analytics import ANALYTICS_SESSION
from nti.app.analytics import SYNC_PARAMS

from nti.dataserver import users

from nti.analytics.tests import NTIAnalyticsApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.externalization.externalization import toExternalObject

from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

class TestWorkspaces(ApplicationLayerTest):

	layer = NTIAnalyticsApplicationTestLayer

	@WithMockDSTrans
	def test_workspace_links(self):
		user = users.User.create_user( dataserver=self.ds, username='sjohnson@nextthought.com' )
		service = UserService( user )

		ext_object = toExternalObject( service )
		__traceback_info__ = ext_object
		toExternalObject( service.workspaces[0] )
		assert_that( ext_object['Items'], has_item(
											has_entries( 'Title', ANALYTICS_TITLE,
														'Links', has_item(
																	has_entries( 'rel', BATCH_EVENTS,
																				'rel', ANALYTICS_SESSION,
																				'rel', SYNC_PARAMS ) ),
														'Items', has_item(
																	has_entries( 'Title', 'Events',
																				'Title', 'Sessions' )) )) )

