#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import has_item
from hamcrest import has_entries
from hamcrest import assert_that

from nti.app.analytics import SYNC_PARAMS
from nti.app.analytics import BATCH_EVENTS
from nti.app.analytics import ANALYTICS_TITLE
from nti.app.analytics import ANALYTICS_SESSION
from nti.app.analytics import END_ANALYTICS_SESSION

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.appserver.workspaces import UserService

from nti.analytics.tests import NTIAnalyticsApplicationTestLayer

from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.dataserver.users.users import User

from nti.externalization.externalization import toExternalObject


class TestWorkspaces(ApplicationLayerTest):

    layer = NTIAnalyticsApplicationTestLayer

    @WithMockDSTrans
    def test_workspace_links(self):
        user = User.create_user(dataserver=self.ds,
                                username=u'sjohnson@nextthought.com')
        service = UserService(user)

        ext_object = toExternalObject(service)
        __traceback_info__ = ext_object
        toExternalObject(service.workspaces[0])
        assert_that(ext_object['Items'],
                    has_item(has_entries('Title', ANALYTICS_TITLE,
                                         'Links', has_item(
                                                     has_entries('rel', BATCH_EVENTS,
                                                                 'rel', ANALYTICS_SESSION,
                                                                 'rel', END_ANALYTICS_SESSION,
                                                                 'rel', SYNC_PARAMS)),
                                         'Items', has_item(
                                                     has_entries('Title', 'Events',
                                                                 'Title', 'Sessions')))))
