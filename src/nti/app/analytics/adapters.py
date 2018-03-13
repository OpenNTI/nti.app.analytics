#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Adapters for application-level events.

.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from datetime import datetime
from six import integer_types

from pyramid.threadlocal import get_current_request

from zope import component
from zope import interface

from zope.security.interfaces import IPrincipal

from nti.analytics import has_analytics

from nti.analytics.boards import get_topic_views
from nti.analytics.boards import get_topic_last_view

from nti.analytics.interfaces import IAnalyticsEvent
from nti.analytics.interfaces import IAnalyticsSessionIdProvider

from nti.analytics.resource_tags import get_note_views
from nti.analytics.resource_tags import get_note_last_view

from nti.analytics.stats.interfaces import IActivitySource
from nti.analytics.stats.interfaces import IActiveTimesStatsSource
from nti.analytics.stats.interfaces import IDailyActivityStatsSource

from nti.app.analytics.interfaces import IAnalyticsContextACLProvider

from nti.app.analytics.usage_stats import CourseVideoUsageStats
from nti.app.analytics.usage_stats import CourseResourceUsageStats
from nti.app.analytics.usage_stats import UserCourseVideoUsageStats
from nti.app.analytics.usage_stats import UserCourseResourceUsageStats

from nti.app.products.courseware.interfaces import ICourseInstanceEnrollment
from nti.app.products.courseware.interfaces import IViewStats
from nti.app.products.courseware.interfaces import IVideoUsageStats
from nti.app.products.courseware.interfaces import IResourceUsageStats

from nti.contenttypes.courses.interfaces import ICourseInstance

from nti.contenttypes.courses.utils import get_course_instructors

from nti.dataserver.authorization import ACT_READ

from nti.dataserver.authorization_acl import ace_allowing

from nti.dataserver.contenttypes.forums.interfaces import ITopic

from nti.dataserver.interfaces import INote
from nti.dataserver.interfaces import IUser

from nti.dataserver.users import User
from nti.app.analytics.utils import get_session_id_from_request

logger = __import__('logging').getLogger(__name__)


class _ViewStats(object):

    def __init__(self, view_count, new_reply_count_for_user=0):
        self.view_count = view_count
        self.new_reply_count_for_user = new_reply_count_for_user


def _get_stats(records, replies=None, user_last_viewed=None):
    count = len(records) if records else 0
    reply_count = len(replies) if replies else 0
    if user_last_viewed is None:
        result = _ViewStats(count, reply_count)
    else:
        new_reply_count_for_user = 0
        for reply in replies:
            reply_created_time = reply.createdTime
            if isinstance(reply_created_time, (integer_types, float)):
                reply_created_time = datetime.utcfromtimestamp(reply_created_time)
            if reply_created_time and reply_created_time > user_last_viewed:
                new_reply_count_for_user += 1
        result = _ViewStats(count, new_reply_count_for_user)
    return result


@interface.implementer(IViewStats)
@component.adapter(ITopic)
def _topic_view_stats(topic):
    result = None
    if has_analytics():
        records = get_topic_views(topic=topic)
        result = _get_stats(records)
    return result


@interface.implementer(IViewStats)
@component.adapter(ITopic, IUser)
def _topic_view_stats_for_user(topic, user):
    result = None
    if has_analytics():
        records = get_topic_views(topic=topic)
        replies = topic.values()
        user_last_view = get_topic_last_view(topic, user)
        result = _get_stats(records, replies, user_last_view)
    return result


@interface.implementer(IViewStats)
@component.adapter(INote)
def _note_view_stats(note):
    result = None
    if has_analytics():
        records = get_note_views(note=note)
        result = _get_stats(records)
    return result


@interface.implementer(IViewStats)
@component.adapter(INote, IUser)
def _note_view_stats_for_user(note, user):
    result = None
    if has_analytics():
        records = get_note_views(note=note)
        replies = note.referents
        user_last_view = get_note_last_view(note, user)
        result = _get_stats(records, replies, user_last_view)
    return result


@interface.implementer(IVideoUsageStats)
@component.adapter(ICourseInstance)
def _video_usage_stats(context):
    result = None
    if has_analytics():
        result = CourseVideoUsageStats(context)
    return result


@interface.implementer(IVideoUsageStats)
@component.adapter(ICourseInstance, IUser)
def _user_video_usage_stats(context, user):
    result = None
    if has_analytics():
        result = UserCourseVideoUsageStats(context, user)
    return result


@interface.implementer(IResourceUsageStats)
@component.adapter(ICourseInstance, IUser)
def _user_resource_usage_stats(context, user):
    result = None
    if has_analytics():
        result = UserCourseResourceUsageStats(context, user)
    return result


@interface.implementer(IResourceUsageStats)
@component.adapter(ICourseInstance)
def _resource_usage_stats(context):
    result = None
    if has_analytics():
        result = CourseResourceUsageStats(context)
    return result


def _unwrap_and_adapt_enrollment(enrollment, iface):
    course = enrollment.CourseInstance
    user = User.get_user(enrollment.Username)

    if not course or not user:
        return None
    return component.getMultiAdapter((user, course), iface)


@interface.implementer(IActiveTimesStatsSource)
@component.adapter(ICourseInstanceEnrollment)
def _active_times_for_enrollment(enrollment):
    return _unwrap_and_adapt_enrollment(enrollment, IActiveTimesStatsSource)


@interface.implementer(IDailyActivityStatsSource)
@component.adapter(ICourseInstanceEnrollment)
def _daily_activity_for_enrollment(enrollment):
    return _unwrap_and_adapt_enrollment(enrollment, IDailyActivityStatsSource)


@interface.implementer(IActivitySource)
@component.adapter(ICourseInstanceEnrollment)
def _activity_source_for_enrollment(enrollment):
    return _unwrap_and_adapt_enrollment(enrollment, IActivitySource)


@component.adapter(IAnalyticsEvent)
@interface.implementer(IAnalyticsSessionIdProvider)
class _AnalyticsSessionIdProvider(object):
    """
    For an analytics event, be able to determine the valid analytics session
    it is tied to.
    """

    def __init__(self, event):
        self.event = event

    def get_session_id(self):
        # Here is what we look for, in order:
        # 1. A session id attached to the incoming event (probably ipad only)
        # 2. A header on the request, (also ipad)
        # 3. A cookie, which should be from webapp, that we can also validate.
        given_session_id = getattr(self.event, 'SessionID', None)
        if given_session_id is not None:
            return given_session_id

        request = get_current_request()

        if     request is None \
            or not has_analytics():
            return None

        result = get_session_id_from_request(request)
        return result


@component.adapter(IUser)
@interface.implementer(IAnalyticsContextACLProvider)
class UserAceProvider(object):

    def __init__(self, user=None):
        self.user = user

    def aces(self):
        return [ace_allowing(self.user, ACT_READ, type(self))]


@component.adapter(ICourseInstanceEnrollment)
@interface.implementer(IAnalyticsContextACLProvider)
class EnrollmentAceProvider(object):

    def __init__(self, enrollment=None):
        self.enrollment = enrollment

    def aces(self):
        instructors = get_course_instructors(self.enrollment)
        aces = [ace_allowing(IPrincipal(self.enrollment.Username), ACT_READ, type(self))]
        for inst in instructors:
            aces.append(ace_allowing(IPrincipal(inst), ACT_READ, type(self)))
        return aces
