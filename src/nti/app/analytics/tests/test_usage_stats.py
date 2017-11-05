#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import contains_string

import fudge

from nti.app.analytics.usage_stats import ALL_USERS
from nti.app.analytics.usage_stats import CourseVideoUsageStats
from nti.app.analytics.usage_stats import CourseResourceUsageStats
from nti.app.analytics.usage_stats import UserCourseVideoUsageStats
from nti.app.analytics.usage_stats import UserCourseResourceUsageStats

from nti.app.analytics.tests import NTIAnalyticsTestCase

from nti.dataserver.tests.mock_dataserver import WithMockDSTrans


class MockUser(object):

    def __init__(self, username):
        self.username = username


class MockEvent(object):

    def __init__(self, username, resource_id, duration, session_id):
        self.user = MockUser(username)
        self.ResourceId = resource_id
        self.Duration = duration
        self.SessionID = session_id


class MockVideoEvent(MockEvent):

    def __init__(self, username, resource_id, duration, session_id, max_duration, end_time):
        super(MockVideoEvent, self).__init__(
            username, resource_id, duration, session_id)
        self.MaxDuration = max_duration
        self.VideoEndTime = end_time


class TestUsageStats(NTIAnalyticsTestCase):
    """
    Test the analytics stats on resources/videos for users in a
    course (that usually end up in reports).
    """

    #: Number of users to create per scope
    USER_SCOPE_COUNT = 5

    def _get_enrollment(self):
        result = {}
        result[ALL_USERS] = set()
        for key in ('Public', 'ForCredit'):
            result[key] = set()
            for i in range(self.USER_SCOPE_COUNT):
                val = '%s%s' % (key, i)
                # Lower like implementation does.
                val = val.lower()
                # Add to both our scope and all users scope
                result[key].add(val)
                result[ALL_USERS].add(val)
        return result

    def setUp(self):
        super(TestUsageStats, self).setUp()
        self.enrollment_dict = self._get_enrollment()

    def get_resource_stats(self):
        # We mock out our course attrs; so this is ignored
        course = object()
        stats = CourseResourceUsageStats(course)
        return stats

    def get_video_stats(self):
        # We mock out our course attrs; so this is ignored
        course = object()
        stats = CourseVideoUsageStats(course)
        return stats

    def get_user_resource_stats(self, username):
        # We mock out our course attrs; so this is ignored
        course = object()
        user = MockUser(username)
        stats = UserCourseResourceUsageStats(course, user)
        return stats

    def get_user_video_stats(self, username):
        # We mock out our course attrs; so this is ignored
        course = object()
        user = MockUser(username)
        stats = UserCourseVideoUsageStats(course, user)
        return stats

    @WithMockDSTrans
    @fudge.patch('nti.app.analytics.usage_stats.get_resource_views',
                 'nti.app.analytics.usage_stats._AbstractUsageStats._get_title',
                 'nti.app.analytics.usage_stats._AbstractUsageStats.instructor_usernames',
                 'nti.app.analytics.usage_stats._get_enrollment_scope_dict')
    def test_resource_usage_stats(self, mock_events, mock_get_title,
                                  mock_instructors, mock_enrollment):
        mock_get_title.is_callable().returns('test title')
        mock_instructors.is_callable().returns(set())
        mock_enrollment.is_callable().returns(self.enrollment_dict)
        mock_events.is_callable().returns(None)

        # Empty
        resource_stats = self.get_resource_stats()
        results = resource_stats.get_stats()
        assert_that(results, has_length(0))
        resource_stats = self.get_user_resource_stats('Public1')
        results = resource_stats.get_stats()
        assert_that(results, has_length(0))

        # Single event
        single_resource_id = u'ntiid1'
        event = MockEvent('Public1', single_resource_id, 20, 1)
        mock_events.is_callable().returns((event,))
        resource_stats = self.get_resource_stats()
        results = resource_stats.get_stats()
        assert_that(results, has_length(1))

        resource_stat = results[0]
        assert_that(resource_stat.ntiid, is_(single_resource_id))
        assert_that(resource_stat.session_count, is_(1))
        assert_that(resource_stat.view_event_count, is_(1))
        # 20s divided by 10 students ~ 2s
        assert_that(resource_stat.watch_times.average_total_watch_time,
                    contains_string(':02'))
        assert_that(resource_stat.watch_times.average_session_watch_time,
                    contains_string(str(event.Duration)))

        # User stats
        resource_stats = self.get_user_resource_stats('Public1')
        results = resource_stats.get_stats()
        assert_that(results, has_length(1))
        resource_stat = results[0]
        assert_that(resource_stat.ntiid, is_(single_resource_id))
        assert_that(resource_stat.session_count, is_(1))
        assert_that(resource_stat.view_event_count, is_(1))
        assert_that(resource_stat.watch_times.average_total_watch_time,
                    contains_string(':20'))
        assert_that(resource_stat.watch_times.average_session_watch_time,
                    contains_string(str(event.Duration)))

        resource_stats = self.get_user_resource_stats('Public2')
        results = resource_stats.get_stats()
        assert_that(results, has_length(0))

        # Multiple events, single resource
        # 7 events. 6 non-instructor events, 5 non-instructor sessions
        events = []
        avg_session_time = u':24'  # 120s divided by 5
        avg_watch_time = u':12'  # 120s divided by 10 students
        events.append(MockEvent('Public1', single_resource_id, 10, 1))
        events.append(MockEvent('Public1', single_resource_id, 10, 1))
        events.append(MockEvent('Public1', single_resource_id, 10, 2))
        events.append(MockEvent('ForCredit1', single_resource_id, 20, 3))
        events.append(MockEvent('ForCredit2', single_resource_id, 30, 4))
        events.append(MockEvent('ForCredit3', single_resource_id, 40, 5))
        # Instructor event ignored (not in our enrollment dict)
        events.append(MockEvent('instructor', single_resource_id, 40, 7))

        mock_events.is_callable().returns(events)
        resource_stats = self.get_resource_stats()
        results = resource_stats.get_stats()
        # Still only one resource
        assert_that(results, has_length(1))

        resource_stat = results[0]
        assert_that(resource_stat.ntiid, is_(single_resource_id))
        assert_that(resource_stat.session_count, is_(5))
        assert_that(resource_stat.view_event_count, is_(6))
        assert_that(resource_stat.watch_times.average_total_watch_time,
                    contains_string(avg_watch_time))
        assert_that(resource_stat.watch_times.average_session_watch_time,
                    contains_string(avg_session_time))

        # User stats
        resource_stats = self.get_user_resource_stats('Public1')
        results = resource_stats.get_stats()
        assert_that(results, has_length(1))
        resource_stat = results[0]
        assert_that(resource_stat.ntiid, is_(single_resource_id))
        assert_that(resource_stat.session_count, is_(2))
        assert_that(resource_stat.view_event_count, is_(3))
        assert_that(resource_stat.watch_times.average_total_watch_time,
                    contains_string(':30'))
        assert_that(resource_stat.watch_times.average_session_watch_time,
                    contains_string(':15'))

        resource_stats = self.get_user_resource_stats('Public2')
        results = resource_stats.get_stats()
        assert_that(results, has_length(0))

    @WithMockDSTrans
    @fudge.patch('nti.app.analytics.usage_stats.get_video_views',
                 'nti.app.analytics.usage_stats._AbstractUsageStats._get_title',
                 'nti.app.analytics.usage_stats._AbstractUsageStats.instructor_usernames',
                 'nti.app.analytics.usage_stats._get_enrollment_scope_dict')
    def test_video_usage_stats(self, mock_events, mock_get_title,
                               mock_instructors, mock_enrollment):
        mock_get_title.is_callable().returns('test title')
        mock_instructors.is_callable().returns(set())
        mock_enrollment.is_callable().returns(self.enrollment_dict)
        mock_events.is_callable().returns(None)

        # Empty
        video_stats = self.get_video_stats()
        results = video_stats.get_stats()
        assert_that(results, has_length(0))

        video_stats = self.get_user_video_stats('Public1')
        results = video_stats.get_stats()
        assert_that(results, has_length(0))

        # Single event
        single_resource_id = u'ntiid1'
        video_duration = 40
        event = MockVideoEvent('Public1', single_resource_id, 20, 1,
                               video_duration, 40)
        mock_events.is_callable().returns((event,))
        video_stats = self.get_video_stats()
        results = video_stats.get_stats()
        assert_that(results, has_length(1))

        video_stat = results[0]
        assert_that(video_stat.ntiid, is_(single_resource_id))
        assert_that(video_stat.video_duration,
                    contains_string(str(video_duration)))
        assert_that(video_stat.session_count, is_(1))
        assert_that(video_stat.view_event_count, is_(1))
        # 20s divided by 10 students ~ 2s
        assert_that(video_stat.watch_times.average_total_watch_time,
                    contains_string(':02'))
        assert_that(video_stat.watch_times.average_session_watch_time,
                    contains_string(str(event.Duration)))

        # User stats
        video_stats = self.get_user_video_stats('Public1')
        results = video_stats.get_stats()
        assert_that(results, has_length(1))

        video_stat = results[0]
        assert_that(video_stat.ntiid, is_(single_resource_id))
        assert_that(video_stat.video_duration,
                    contains_string(str(video_duration)))
        assert_that(video_stat.session_count, is_(1))
        assert_that(video_stat.view_event_count, is_(1))
        assert_that(video_stat.watch_times.average_total_watch_time,
                    contains_string(':20'))
        assert_that(video_stat.watch_times.average_session_watch_time,
                    contains_string(str(event.Duration)))

        video_stats = self.get_user_video_stats('Public2')
        results = video_stats.get_stats()
        assert_that(results, has_length(0))

        # Multiple events, single resource
        events = []
        events.append(
            MockVideoEvent('Public1', single_resource_id,
                           10, 1, video_duration, 10)
        )
        events.append(
            MockVideoEvent('Public1', single_resource_id,
                           10, 1, video_duration, 20)
        )
        events.append(
            MockVideoEvent('Public1', single_resource_id,
                           10, 2, video_duration, 30)
        )
        events.append(
            MockVideoEvent('Public1', single_resource_id,
                           10, 2, video_duration, 40)
        )
        events.append(
            MockVideoEvent('Public2', single_resource_id,
                           10, 3, video_duration, 10)
        )
        events.append(
            MockVideoEvent('Public2', single_resource_id,
                           10, 3, video_duration, 20)
        )
        events.append(
            MockVideoEvent('Public2', single_resource_id,
                           10, 4, video_duration, 30)
        )
        events.append(
            MockVideoEvent('Public2', single_resource_id,
                           10, 4, video_duration, 30)
        )
        events.append(
            MockVideoEvent('ForCredit1', single_resource_id,
                           20, 5, video_duration, 40)
        )
        events.append(
            MockVideoEvent('ForCredit2', single_resource_id,
                           30, 6, video_duration, 40)
        )
        events.append(
            MockVideoEvent('ForCredit3', single_resource_id,
                           40, 7, video_duration, 40)
        )
        events.append(
            MockVideoEvent('ForCredit4', single_resource_id,
                           40, 8, video_duration, 20)
        )
        total_watch_time = sum([x.Duration for x in events])
        session_count = len({x.SessionID for x in events})
        avg_session_time = u':%s' % (int(total_watch_time / session_count))
        avg_watch_time = u':%s' % (int(total_watch_time / 10))
        # 2 students out of 10 watched completely: Public1 in aggregate,
        # ForCredit3 in one event
        percentage_watched_completely = u'20%'

        mock_events.is_callable().returns(events)
        video_stats = self.get_video_stats()
        results = video_stats.get_stats()
        # Still only one resource
        assert_that(results, has_length(1))

        video_stat = results[0]
        assert_that(video_stat.ntiid, is_(single_resource_id))
        assert_that(video_stat.session_count, is_(session_count))
        assert_that(video_stat.view_event_count, is_(len(events)))
        assert_that(video_stat.video_duration,
                    contains_string(str(video_duration)))
        assert_that(video_stat.percentage_watched_completely,
                    contains_string(str(percentage_watched_completely)))
        assert_that(video_stat.watch_times.average_total_watch_time,
                    contains_string(avg_watch_time))
        assert_that(video_stat.watch_times.average_session_watch_time,
                    contains_string(avg_session_time))

        # Other fields we turn into strs
        drop_off = video_stat.falloff_rate
        assert_that(drop_off.drop25count, is_(0))
        assert_that(drop_off.drop25percentage, is_(0))
        assert_that(drop_off.drop50count, is_(3))
        assert_that(drop_off.drop50percentage, is_(38.0))
        assert_that(drop_off.drop75count, is_(1))
        assert_that(drop_off.drop75percentage, is_(13.0))
        assert_that(drop_off.drop100count, is_(4))
        assert_that(drop_off.drop100percentage, is_(50))

        # User stats
        video_stats = self.get_user_video_stats('Public1')
        results = video_stats.get_stats()
        assert_that(results, has_length(1))

        video_stat = results[0]
        assert_that(video_stat.ntiid, is_(single_resource_id))
        assert_that(video_stat.video_duration,
                    contains_string(str(video_duration)))
        assert_that(video_stat.session_count, is_(2))
        assert_that(video_stat.view_event_count, is_(4))
        assert_that(video_stat.watch_times.average_total_watch_time,
                    contains_string(':40'))
        assert_that(video_stat.watch_times.average_session_watch_time,
                    contains_string(':20'))

        video_stats = self.get_user_video_stats('Public2')
        results = video_stats.get_stats()
        assert_that(results, has_length(1))

        video_stats = self.get_user_video_stats('Public3')
        results = video_stats.get_stats()
        assert_that(results, has_length(0))
