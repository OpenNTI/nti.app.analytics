#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import not_none
from hamcrest import assert_that
from hamcrest import less_than_or_equal_to

from nti.analytics.interfaces import IUserResearchStatus

from nti.app.analytics.views import SET_RESEARCH_VIEW

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS
from nti.app.testing.decorators import WithSharedApplicationMockDSHandleChanges

from nti.dataserver.tests import mock_dataserver

from nti.dataserver.users.users import User


class TestAnalytics(ApplicationLayerTest):

    default_origin = 'http://platform.ou.edu'

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_user_research_study(self):
        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            user = User.create_user(username=u'new_user1', dataserver=self.ds,
                                    external_value={'realname': u'Jim Bob',
                                                    'email': u'foo@bar.com'})

            user_research = IUserResearchStatus(user)
            assert_that(user_research, not_none())
            assert_that(user_research.allow_research, is_(False))
            recent_mod_time = user_research.lastModified
            assert_that(recent_mod_time, not_none())

        url = '/dataserver2/users/new_user1/' + SET_RESEARCH_VIEW
        extra_environ = {'HTTP_ORIGIN': 'http://platform.ou.edu'}
        # Toggle
        data = {'allow_research': True}
        self.testapp.post_json(url, data, extra_environ=extra_environ)

        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            user = User.get_user('new_user1')
            user_research = IUserResearchStatus(user)

            assert_that(user_research, not_none())
            assert_that(user_research.allow_research, is_(True))
            assert_that(user_research.lastModified, not_none())
            assert_that(recent_mod_time, 
						less_than_or_equal_to(user_research.lastModified))
            recent_mod_time = user_research.lastModified

        # And back again
        data = {'allow_research': False}
        self.testapp.post_json(url, data, extra_environ=extra_environ)

        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            user = User.get_user('new_user1')
            user_research = IUserResearchStatus(user)
            assert_that(user_research, not_none())
            assert_that(user_research.allow_research, is_(False))
            assert_that(user_research.lastModified, not_none())
            assert_that(recent_mod_time, 
						less_than_or_equal_to(user_research.lastModified))

    @WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
    def test_get_research_stats(self):
        username = u'cald3307'
        with mock_dataserver.mock_db_trans(self.ds):
            self._create_user(username=username)

        environ = self._make_extra_environ()
        environ['HTTP_ORIGIN'] = 'http://platform.ou.edu'

        stats_url = '/dataserver2/analytics/user_research_stats'
        testapp = self.testapp
        res = testapp.get(stats_url,
                          None,
                          extra_environ=environ,
                          status=200)
        body = res.json_body
        assert_that(body['AllowResearchCount'], is_(0))
        assert_that(body['DenyResearchCount'], is_(0))
        assert_that(body['ToBePromptedCount'], is_(2))

        url = '/dataserver2/users/cald3307/' + SET_RESEARCH_VIEW
        # Set
        data = {'allow_research': True}
        testapp.post_json(url, data, extra_environ=environ)

        # Re-query
        res = testapp.get(stats_url,
                          None,
                          extra_environ=environ,
                          status=200)
        body = res.json_body
        assert_that(body['AllowResearchCount'], is_(1))
        assert_that(body['DenyResearchCount'], is_(0))
        assert_that(body['ToBePromptedCount'], is_(1))

        # Reverse
        data = {'allow_research': False}
        testapp.post_json(url, data, extra_environ=environ)

        # Re-query
        res = testapp.get(stats_url,
                          None,
                          extra_environ=environ,
                          status=200)
        body = res.json_body
        assert_that(body['AllowResearchCount'], is_(0))
        assert_that(body['DenyResearchCount'], is_(1))
        assert_that(body['ToBePromptedCount'], is_(1))

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_research_status_link(self):
        url = '/dataserver2/users/sjohnson@nextthought.com/' + SET_RESEARCH_VIEW
        res = self.resolve_user()
        href = self.require_link_href_with_rel(res, SET_RESEARCH_VIEW)
        assert_that(href, is_(url))

        # Set
        data = {'allow_research': True}
        self.testapp.post_json(url, data)

        # Subsequent call does not have link
        href = self.forbid_link_with_rel(self.resolve_user(),
                                         SET_RESEARCH_VIEW)
