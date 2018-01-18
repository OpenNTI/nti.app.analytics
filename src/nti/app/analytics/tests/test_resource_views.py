#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

import time

from hamcrest import is_
from hamcrest import not_none
from hamcrest import assert_that
from hamcrest import has_length

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.analytics.database import resource_views as db_views

from nti.analytics.resource_views import get_resource_views

from nti.analytics.tests import test_session_id

from nti.app.contentlibrary.tests import PersistentApplicationTestLayer

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.dataserver.tests import mock_dataserver

from nti.dataserver.users import User
from nti.ntiids.ntiids import find_object_with_ntiid


class TestBookViews(ApplicationLayerTest):

	layer = PersistentApplicationTestLayer

	@WithSharedApplicationMockDS(users=True, testapp=True)
	def test_book_views(self):
		with mock_dataserver.mock_db_trans(self.ds):
			self._create_user(username='test_user_book_views')
			self._create_user(username='user_without_views')

		time_length = 30
		event_time = time.time()
		package_ntiid = 'tag:nextthought.com,2011-10:NTI-HTML-PackageA'

		with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
			user = User.get_user('test_user_book_views')
			base_case_user = User.get_user('user_without_views')
			package = find_object_with_ntiid(package_ntiid)
			assert_that(package, not_none())
			db_views.create_resource_view(user,
										  test_session_id,
										  event_time,
										  package,
										  (package_ntiid,),
										  package_ntiid,
										  time_length)
			package_views = get_resource_views(user, package)
			assert_that(package_views, has_length(1))
			package_view = package_views[0]
			assert_that(package_view.RootContext, is_(package))
			assert_that(package_view.Duration, is_(time_length))
			assert_that(package_view.user, is_(user))
			assert_that(package_view.Title, is_(u'aaaaa'))

			package_views = get_resource_views(base_case_user, package)
			assert_that(package_views, has_length(0))
