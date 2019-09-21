#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import datetime

from hamcrest import assert_that
from hamcrest import has_length
from hamcrest import is_

from zope.event import notify

from nti.analytics.database.lti import get_launch_records_for_ntiid

from nti.analytics_database.tests import AnalyticsDatabaseTest

from nti.app.products.courseware.tests import InstructedCourseApplicationTestLayer

from nti.app.products.courseware_ims.interfaces import LTILaunchEvent

from nti.app.products.courseware_ims.lti import LTIExternalToolAsset

from nti.app.products.courseware_ims.tests import create_configured_tool

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.contenttypes.courses.courses import ContentCourseInstance

from nti.dataserver.tests import mock_dataserver

from nti.dataserver.users import User

logger = __import__('logging').getLogger(__name__)


class TestLTIAnalytics(ApplicationLayerTest, AnalyticsDatabaseTest):

    layer = InstructedCourseApplicationTestLayer

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_asset_analytics(self):

        with mock_dataserver.mock_db_trans(self.ds):
            tool = create_configured_tool()
            asset = LTIExternalToolAsset(ConfiguredTool=tool)
            asset.ntiid = u'tag:nextthought.com,2011:test'
            course = ContentCourseInstance()

            connection = mock_dataserver.current_transaction
            connection.add(course)
            course_ntiid = course.ntiid
            timestamp = datetime.datetime.today()

            ds = mock_dataserver.current_mock_ds
            user = User.create_user(ds, username=u'foonextthought1',
                                    password=u'TestPass')
            event = LTILaunchEvent(user=user,
                                   course=course,
                                   asset=asset,
                                   timestamp=timestamp)

            notify(event)

            launch_records = get_launch_records_for_ntiid(asset.ntiid)

            assert_that(launch_records, has_length(1))
            record = launch_records[0]

            assert_that(record.context_path, is_(course_ntiid))
            assert_that(record.lti_asset_launches_id, is_(1))
            assert_that(record.user, is_(user))
            assert_that(record.timestamp.year, is_(timestamp.year))  # Analytics snips off part of the full timestamp
