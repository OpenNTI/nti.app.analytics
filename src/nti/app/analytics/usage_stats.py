#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Provide analytics stats on usage for a given context.

.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from heapq import nlargest

from collections import namedtuple
from collections import defaultdict

from zope import interface

from zope.cachedescriptors.property import Lazy

from nti.analytics.resource_views import get_video_views
from nti.analytics.resource_views import get_resource_views

from nti.app.contentlibrary.interfaces import IResourceUsageStats as IBookResourceUsageStats

from nti.app.products.courseware.interfaces import IVideoUsageStats
from nti.app.products.courseware.interfaces import IResourceUsageStats
from nti.app.products.courseware.interfaces import IUserVideoUsageStats
from nti.app.products.courseware.interfaces import IUserResourceUsageStats

from nti.dataserver.authorization import is_admin_or_content_admin_or_site_admin

from nti.dataserver.interfaces import IEnumerableEntityContainer

from nti.ntiids.ntiids import find_object_with_ntiid

from nti.property.property import alias

_VideoInfo = namedtuple('_VideoInfoData',
                        ('title',
                         'ntiid',
                         'session_count',
                         'view_event_count',
                         'watch_times',
                         'video_duration',
                         'percentage_watched_completely',
                         'number_watched_completely',
                         'falloff_rate'))

_ResourceInfo = namedtuple('_ResourceInfo',
                           ('title',
                            'ntiid',
                            'session_count',
                            'view_event_count',
                            'total_view_time',
                            'last_view_time',
                            'watch_times'))

_VideoDropOffRate = namedtuple('_VideoDropOffRate',
                               ('drop25count', 'drop25percentage',
                                'drop50count', 'drop50percentage',
                                'drop75count', 'drop75percentage',
                                'drop100count', 'drop100percentage'))

#: Used if we have undefined info (e.g. no video duration)
EMPTY_VIDEO_DROP_OFF = _VideoDropOffRate('-', '-', '-', '-',
                                         '-', '-', '-', '-')

_AverageWatchTimes = namedtuple('_AverageWatchTimes',
                                ('average_total_watch_time',
                                 'average_session_watch_time'))

ALL_USERS = u'AllUsers'

logger = __import__('logging').getLogger(__name__)


def _get_enrollment_scope_dict(course, instructors=set()):
    """
    Build a dict of scope_name to usernames.
    """
    # XXX We are not exposing these multiple scopes in many places,
    # including many reports and in TopCreators.
    # XXX This is confusing if we are nesting scopes.  Perhaps
    # it makes more sense to keep things in the Credit/NonCredit camps.
    # Seems like it would make sense to have an Everyone scope...
    # { Everyone: { Public : ( Open, Purchased ), ForCredit : ( FCD, FCND ) }}

    # XXX: Similar to other report data, we capture everyone in scope, which
    # includes all subinstances. This seems confusing to the end-user since
    # this isnt messaged.
    results = {}
    # Lumping purchased in with public.
    public_scope = course.SharingScopes.get('Public', None)
    purchased_scope = course.SharingScopes.get('Purchased', None)
    non_public_users = set()
    for scope_name in course.SharingScopes:
        scope = course.SharingScopes.get(scope_name, None)
        if scope is not None and scope not in (public_scope, purchased_scope):
            # If our scope is not 'public'-ish, store it separately.
            # All credit-type users should end up in ForCredit.
            scope_users = {
                x.lower() for x in IEnumerableEntityContainer(scope).iter_usernames()
            }
            scope_users = scope_users - instructors
            results[scope_name] = scope_users
            non_public_users = non_public_users.union(scope_users)

    all_users = {
        x.lower() for x in IEnumerableEntityContainer(public_scope).iter_usernames()
    }
    results['Public'] = all_users - non_public_users - instructors
    results[ALL_USERS] = all_users - instructors
    return results


class _AbstractUsageStats(object):
    """
    When fetching stats, we'll build or fetch the stats (run once). If
    building, we'll grab the relevant events and then run them through
    an accumulator, filtering out certain users and including others
    (optionally).
    """

    EXCLUDE_ADMINS = True

    #: The number of top resources to return in get_top_stats, by default.
    DEFAULT_TOP_COUNT = 6

    def __init__(self, context):
        self.context = context
        self.accum = ResourceEventAccumulator()
        self._stats = None

    def _get_included_users(self, *args, **kwargs):
        return None

    def _exclude_user(self, user):
        return  self.EXCLUDE_ADMINS \
            and is_admin_or_content_admin_or_site_admin(user)

    def _build_or_get_stats(self, *args, **kwargs):
        if self._stats == None:
            included_users = self._get_included_users(*args, **kwargs)
            self._stats = self._build_data_for_users(included_users)
        return self._stats

    def get_stats(self, *args, **kwargs):
        """
        Return stats.
        """
        return self._build_or_get_stats(*args, **kwargs)

    def get_top_stats(self, top_count=None, *args, **kwargs):
        """
        Return top usage stats for course users, optionally by scope.
        """
        stats = self._build_or_get_stats(*args, **kwargs)
        top_count = top_count or self.DEFAULT_TOP_COUNT
        # Safe as long as we're non-None.
        result = nlargest(top_count, stats, key=lambda vid: vid.session_count)
        return result

    def __get_title(self, obj):
        return getattr(obj, 'title', None) or getattr(obj, 'label', '')

    def _get_title(self, ntiid):
        result = None
        obj = find_object_with_ntiid(ntiid)
        if obj is not None:
            result = self.__get_title(obj)
            if not result:
                try:
                    # Content cards
                    target_ntiid = obj.path[-1].ntiid
                    obj = find_object_with_ntiid(target_ntiid)
                    result = self.__get_title(obj)
                except (AttributeError, IndexError):
                    pass
        return result

    def _get_watch_data(self, stats, user_count):
        """
        For the given stats and student count, return watch stats ready
        for display purposes.
        """
        average_watch_time = stats.total_view_time / user_count
        average_session_time = stats.total_view_time / stats.session_count

        watch_data = _AverageWatchTimes('%s:%02d' % divmod(int(average_watch_time), 60),
                                        '%s:%02d' % divmod(int(average_session_time), 60))
        return watch_data

    def build_results(self, accum, user_count):
        """
        Post-accumulation, build our result object set.
        """
        results = []

        for ntiid, stats in accum.ntiid_stats_map.items():
            data = self._build_resource_stats(ntiid, stats, user_count)
            if data is not None:
                results.append(data)

        results.sort(key=lambda x: x.title)
        return results

    def get_usernames_with_stats(self):
        self.get_stats()
        return tuple(self.accum.user_stats_map)

    def get_stats_for_user(self, user):
        self.get_stats()
        username = getattr(user, 'username', user)
        return self.accum.user_stats_map.get(username)

    def _build_data_for_users(self, included_users):
        """
        For the given set of usernames, build stats based on events and return.
        """
        for event in self.events:
            if     event is None \
                or event.user is None \
                or self._exclude_user(event.user) \
                or (    included_users is not None \
                    and event.user.username.lower() not in included_users):
                continue
            self.accum.accum(event)

        if included_users:
            user_count = len(included_users)
        else:
            user_count = len(tuple(self.accum.user_stats_map))
        result = self.build_results(self.accum, user_count)
        return result


class _AbstractCourseUsageStats(_AbstractUsageStats):

    #: A cache of scopes to result set stats.
    scope_result_set_map = None

    course = alias('context')

    def __init__(self, course):
        super(_AbstractCourseUsageStats, self).__init__(course)
        self.scope_result_set_map = {}

    def _get_included_users(self, scope_name):
        return self._get_user_base(scope_name)

    @Lazy
    def instructor_usernames(self):
        return {x.id.lower() for x in self.course.instructors}

    def _exclude_user(self, user):
        return user.username.lower() in self.instructor_usernames \
            or super(_AbstractCourseUsageStats, self)._exclude_user(user)

    @Lazy
    def enrollment_scope_dict(self):
        return _get_enrollment_scope_dict(self.course,
                                          self.instructor_usernames)

    def _get_scope(self, scope_name):
        result = ALL_USERS
        if scope_name and scope_name.lower() in ('public', 'open'):
            result = u'Public'
        elif scope_name:
            result = u'ForCredit'
        return result

    def _get_user_base(self, scope_name):
        user_base = self.enrollment_scope_dict[scope_name]
        return user_base

    def _build_or_get_stats(self, scope=None):
        scope_name = self._get_scope(scope)
        if scope_name not in self.scope_result_set_map:
            user_base = self._get_user_base(scope_name)
            result = self._build_data_for_users(user_base)
            self.scope_result_set_map[scope_name] = result
        else:
            result = self.scope_result_set_map.get(scope_name)
        return result


class BaseStats(object):

    def __init__(self):
        self.total_view_time = 0
        self.max_end_time = 0

    def incr(self, event):
        if event.Duration:
            self.total_view_time += event.Duration
        end_time = getattr(event, 'VideoEndTime', None)
        if end_time and end_time > self.max_end_time:
            self.max_end_time = end_time


class ResourceStats(object):
    """
    Resource specific stats that store per-session and per-user data.
    """

    def __init__(self):
        self.total_view_time = 0
        self.user_stats = defaultdict(BaseStats)
        self.session_stats = defaultdict(BaseStats)
        self.event_count = 0
        self.max_duration = None
        self.last_view_time = None

    @property
    def session_count(self):
        return len(self.session_stats)

    def incr(self, event):
        self.event_count += 1
        if event.Duration:
            self.total_view_time += event.Duration
        # Key on username
        user_stats = self.user_stats[event.user.username]
        user_stats.incr(event)
        session_stats = self.session_stats[event.SessionID]
        session_stats.incr(event)
        if self.max_duration is None:
            self.max_duration = getattr(event, 'MaxDuration', None)
        if self.last_view_time is None:
            self.last_view_time = event.timestamp
        elif event.timestamp and event.timestamp > self.last_view_time:
            self.last_view_time = event.timestamp


class ResourceEventAccumulator(object):
    """
    An event accumulator that stores events by ntiid -> stats.
    """

    def __init__(self):
        self.ntiid_stats_map = defaultdict(ResourceStats)
        self.user_stats_map = defaultdict(ResourceStats)

    def accum(self, event):
        resource_stats = self.ntiid_stats_map[event.ResourceId]
        resource_stats.incr(event)
        user_stats = self.user_stats_map[event.user.username]
        user_stats.incr(event)


@interface.implementer(IResourceUsageStats)
class CourseResourceUsageStats(_AbstractCourseUsageStats):
    """
    Usage stats that know how to build results for basic resource
    view stats.
    """

    @Lazy
    def events(self):
        return get_resource_views(root_context=self.course) or ()

    def _build_resource_stats(self, ntiid, stats, student_count):
        title = self._get_title(ntiid)
        if title is None:
            return

        watch_data = self._get_watch_data(stats, student_count)
        data = _ResourceInfo(title,
                             ntiid,
                             stats.session_count,
                             stats.event_count,
                             stats.total_view_time,
                             stats.last_view_time,
                             watch_data)
        return data


@interface.implementer(IBookResourceUsageStats)
class BookResourceUsageStats(_AbstractUsageStats):
    """
    Usage stats that know how to build results for basic resource view stats.
    """

    @Lazy
    def events(self):
        return get_resource_views(root_context=self.context) or ()

    def _build_resource_stats(self, ntiid, stats, student_count):
        title = self._get_title(ntiid)
        if title is None:
            return

        watch_data = self._get_watch_data(stats, student_count)
        data = _ResourceInfo(title,
                             ntiid,
                             stats.session_count,
                             stats.event_count,
                             stats.total_view_time,
                             stats.last_view_time,
                             watch_data)
        return data


@interface.implementer(IUserResourceUsageStats)
class UserBookResourceUsageStats(BookResourceUsageStats):
    """
    Usage stats that know how to build results for basic resource
    view stats for a course and user.
    """

    EXCLUDE_ADMINS = False

    def __init__(self, book, user):
        super(UserBookResourceUsageStats, self).__init__(book)
        self.user = user

    @Lazy
    def events(self):
        results = get_resource_views(root_context=self.context, user=self.user)
        return results or ()

    def get_stats(self):
        return self._build_data_for_users((self.user.username.lower(),))


@interface.implementer(IUserResourceUsageStats)
class UserCourseResourceUsageStats(CourseResourceUsageStats):
    """
    Usage stats that know how to build results for basic resource
    view stats for a course and user.
    """

    EXCLUDE_ADMINS = False

    def __init__(self, course, user):
        super(UserCourseResourceUsageStats, self).__init__(course)
        self.user = user

    @Lazy
    def events(self):
        results = get_resource_views(root_context=self.course, user=self.user)
        return results or ()

    def get_stats(self):
        return self._build_data_for_users((self.user.username.lower(),))


@interface.implementer(IVideoUsageStats)
class CourseVideoUsageStats(_AbstractCourseUsageStats):
    """
    Usage stats that know how to build results for video resource
    view stats.
    """

    #: The threshold at which videos are said to be completely watched.
    VIDEO_COMPLETED_THRESHOLD = 0.9

    @Lazy
    def events(self):
        return get_video_views(course=self.course) or ()

    def _build_drop_off_data(self, stats):
        """
        Using session stats, calculate where each user 'dropped' off while
        watching a video, bucketing into quartiles.
        """
        video_duration = stats.max_duration
        session_count = stats.session_count
        if not video_duration:
            return EMPTY_VIDEO_DROP_OFF

        first_quartile = video_duration * 0.25
        second_quartile = video_duration * 0.5
        third_quartile = video_duration * 0.75

        drop25count = drop50count = drop75count = drop100count = 0
        for session_stat in stats.session_stats.values():
            end_time = session_stat.max_end_time
            if end_time <= first_quartile:
                drop25count += 1
            elif end_time <= second_quartile:
                drop50count += 1
            elif end_time <= third_quartile:
                drop75count += 1
            else:
                drop100count += 1

        drop25percentage = round(drop25count / float(session_count) * 100)
        drop50percentage = round(drop50count / float(session_count) * 100)
        drop75percentage = round(drop75count / float(session_count) * 100)
        drop100percentage = round(drop100count / float(session_count) * 100)

        falloff_data = _VideoDropOffRate(drop25count, drop25percentage,
                                         drop50count, drop50percentage,
                                         drop75count, drop75percentage,
                                         drop100count, drop100percentage)

        return falloff_data

    def _build_resource_stats(self, ntiid, stats, student_count):
        title = self._get_title(ntiid)
        if title is None:
            return

        video_duration = stats.max_duration
        number_users_watched_completely = 0
        str_video_duration = ''

        if video_duration:
            str_video_duration = '%s:%02d' % divmod(int(video_duration), 60)

            for user_stat in stats.user_stats.values():
                # To completely watch a video, the user must accumulate up to a threshold and
                # watch up to a certain threshold (not just watch beginning
                # over and over).
                if      user_stat.total_view_time >= video_duration * self.VIDEO_COMPLETED_THRESHOLD \
                    and user_stat.max_end_time >= video_duration * self.VIDEO_COMPLETED_THRESHOLD:
                    number_users_watched_completely += 1

        perc_users_watched_completely = number_users_watched_completely / student_count
        str_perc_watched_completely = '%d%%' % int(perc_users_watched_completely * 100)

        watch_data = self._get_watch_data(stats, student_count)
        drop_off_data = self._build_drop_off_data(stats)
        data = _VideoInfo(title,
                          ntiid,
                          stats.session_count,
                          stats.event_count,
                          watch_data,
                          str_video_duration,
                          str_perc_watched_completely,
                          number_users_watched_completely,
                          drop_off_data)
        return data


@interface.implementer(IUserVideoUsageStats)
class UserCourseVideoUsageStats(CourseVideoUsageStats):
    """
    Usage stats that know how to build results for video resource
    view stats for a course and user.
    """

    def __init__(self, course, user):
        super(UserCourseVideoUsageStats, self).__init__(course)
        self.user = user

    @Lazy
    def events(self):
        results = get_video_views(course=self.course, user=self.user)
        return results or ()

    def get_stats(self):
        return self._build_data_for_users((self.user.username.lower(),))
