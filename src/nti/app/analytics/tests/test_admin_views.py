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

import time

from zope import component

from nti.analytics.interfaces import IUserResearchStatus

from nti.analytics.model import SkipVideoEvent
from nti.analytics.model import WatchVideoEvent
from nti.analytics.model import BatchResourceEvents

from nti.analytics.tests import NTIAnalyticsApplicationTestLayer

from nti.analytics_database.database import AnalyticsDB

from nti.analytics_database.interfaces import IAnalyticsDB

from nti.app.analytics import VIEW_STATS
from nti.app.analytics import ANALYTICS_SESSION_HEADER

from nti.app.analytics.views import SET_RESEARCH_VIEW

from nti.app.products.courseware.tests import LegacyInstructedCourseApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS
from nti.app.testing.decorators import WithSharedApplicationMockDSHandleChanges

from nti.app.testing.webtest import TestApp

from nti.dataserver.tests import mock_dataserver

from nti.dataserver.users.users import User

from nti.externalization.externalization import to_external_object


class TestAnalytics(ApplicationLayerTest):

    layer = NTIAnalyticsApplicationTestLayer

    default_origin = 'http://platform.ou.edu'

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_user_research_study(self):
        with mock_dataserver.mock_db_trans(self.ds):
            self._create_user(username='new_user1')
            self._create_user(username='new_user2')

        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            user = User.get_user('new_user1')
            user_research = IUserResearchStatus(user)
            assert_that(user_research, not_none())
            assert_that(user_research.allow_research, is_(False))
            recent_mod_time = user_research.lastModified
            assert_that(recent_mod_time, not_none())

        user1_environ = self._make_extra_environ(user='new_user1')
        user2_environ = self._make_extra_environ(user='new_user2')
        url = '/dataserver2/users/new_user1/' + SET_RESEARCH_VIEW
        # Toggle
        data = {'allow_research': True}

        # Invalid permissions
        TestApp(self.app).post_json(url, data, status=401)
        self.testapp.post_json(url, data, extra_environ=user2_environ, status=403)

        self.testapp.post_json(url, data, extra_environ=user1_environ)

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
        self.testapp.post_json(url, data, extra_environ=user1_environ)

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

        environ = self._make_extra_environ(user=username)
        stats_url = '/dataserver2/analytics/user_research_stats'
        testapp = self.testapp
        res = testapp.get(stats_url)
        body = res.json_body
        assert_that(body['AllowResearchCount'], is_(0))
        assert_that(body['DenyResearchCount'], is_(0))
        assert_that(body['ToBePromptedCount'], is_(3))

        url = '/dataserver2/users/cald3307/' + SET_RESEARCH_VIEW
        # Set
        data = {'allow_research': True}
        testapp.post_json(url, data, extra_environ=environ)

        # Re-query
        res = testapp.get(stats_url)
        body = res.json_body
        assert_that(body['AllowResearchCount'], is_(1))
        assert_that(body['DenyResearchCount'], is_(0))
        assert_that(body['ToBePromptedCount'], is_(2))

        # Reverse
        data = {'allow_research': False}
        testapp.post_json(url, data, extra_environ=environ)

        # Re-query
        res = testapp.get(stats_url)
        body = res.json_body
        assert_that(body['AllowResearchCount'], is_(0))
        assert_that(body['DenyResearchCount'], is_(1))
        assert_that(body['ToBePromptedCount'], is_(2))

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


class TestStats(ApplicationLayerTest):
    """
    Validate we can retrieve a users stats on a resource
    and clear the data. This flow is for QA.
    """

    layer = LegacyInstructedCourseApplicationTestLayer

    default_origin = 'http://platform.ou.edu'

    video_ntiid = u"tag:nextthought.com,2011-10:OU-NTIVideo-CLC3403_LawAndJustice.ntivideo.video_17.02"

    def setUp(self):
        self.db = AnalyticsDB(dburi='sqlite://', autocommit=True)
        component.getGlobalSiteManager().registerUtility(self.db, IAnalyticsDB)

    def tearDown(self):
        component.getGlobalSiteManager().unregisterUtility(self.db)

    def _store_video_data(self, username):
        timestamp = time.time()
        course = u'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.course_info'
        context_path = [u'DASHBOARD', u'ntiid:tag_blah']
        video_event = SkipVideoEvent(user=username,
                                     timestamp=timestamp,
                                     RootContextID=course,
                                     context_path=context_path,
                                     ResourceId=self.video_ntiid,
                                     Duration=30,
                                     video_start_time=29,
                                     video_end_time=59,
                                     with_transcript=True)
        events = []
        events.append(video_event)

        # 760s of view time - 4 distinct events
        data = [(0, 10, 10, timestamp),
                (15, 35, 20, timestamp+1),
                (60, 90, 30, timestamp+2),
                (300, 1000, 700, timestamp+3)]
        for start, end, duration, timestamp in data:
            watch_video_event = WatchVideoEvent(user=username,
                                                timestamp=timestamp,
                                                RootContextID=course,
                                                context_path=context_path,
                                                ResourceId=self.video_ntiid,
                                                Duration=duration,
                                                video_start_time=start,
                                                video_end_time=end,
                                                with_transcript=True)
            events.append(watch_video_event)
        io = BatchResourceEvents(events=events)
        ext_obj = to_external_object(io)
        batch_url = '/dataserver2/analytics/batch_events'
        headers = {ANALYTICS_SESSION_HEADER: str(9999)}
        env = self._make_extra_environ(user=username)
        self.testapp.post_json(batch_url,
                               ext_obj,
                               headers=headers,
                               extra_environ=env)

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_user_stats(self):
        with mock_dataserver.mock_db_trans(self.ds):
            self._create_user(username='user_analytics_stats1')
            self._create_user(username='user_analytics_stats2')
        self._store_video_data('user_analytics_stats1')

        user1_stats_url = '/dataserver2/Objects/%s/%s?username=%s' % (self.video_ntiid, VIEW_STATS, 'user_analytics_stats1')
        user2_stats_url = '/dataserver2/Objects/%s/%s?username=%s' % (self.video_ntiid, VIEW_STATS, 'user_analytics_stats2')

        reset_url = '/dataserver2/analytics/remove_data'
        user1_reset_data = {'resource': self.video_ntiid,
                            'user': 'user_analytics_stats1'}
        user2_reset_data = {'resource': self.video_ntiid,
                            'user': 'user_analytics_stats2'}

        user1_environ = self._make_extra_environ(user='user_analytics_stats1')
        user2_environ = self._make_extra_environ(user='user_analytics_stats2')

        for user_env in (user1_environ, user2_environ):
            self.testapp.get(user1_stats_url, extra_environ=user_env, status=403)
            self.testapp.get(user2_stats_url, extra_environ=user_env, status=403)
            self.testapp.post_json(reset_url, user1_reset_data,
                                   extra_environ=user_env, status=403)

        res1 = self.testapp.get(user1_stats_url).json_body
        user1_stats = res1.get('Stats')
        assert_that(user1_stats.get('Count'), is_(4))
        assert_that(user1_stats.get('AggregateTime'), is_(760))
        assert_that(user1_stats.get('AverageDuration'), is_(190.0))

        res2 = self.testapp.get(user2_stats_url).json_body
        user2_stats = res2.get('Stats')
        assert_that(user2_stats.get('Count'), is_(0))
        assert_that(user2_stats.get('AggregateTime'), is_(0))
        assert_that(user2_stats.get('AverageDuration'), is_(0))

        # Now reset
        self.testapp.post_json(reset_url, user1_reset_data)
        self.testapp.post_json(reset_url, user2_reset_data)

        res1 = self.testapp.get(user1_stats_url).json_body
        user1_stats = res1.get('Stats')
        assert_that(user1_stats.get('Count'), is_(0))
        assert_that(user1_stats.get('AggregateTime'), is_(0))
        assert_that(user1_stats.get('AverageDuration'), is_(0))

        res2 = self.testapp.get(user2_stats_url).json_body
        user2_stats = res2.get('Stats')
        assert_that(user2_stats.get('Count'), is_(0))
        assert_that(user2_stats.get('AggregateTime'), is_(0))
        assert_that(user2_stats.get('AverageDuration'), is_(0))

        # Invalid
        self.testapp.post_json(reset_url, {'resource': self.video_ntiid,
                                           'user': "dne_user"}, status=422)
        self.testapp.post_json(reset_url, {'resource': u'tag:nextthought.com,2011-10:dne_ntiid',
                                           'user': "user_analytics_stats1"}, status=422)
