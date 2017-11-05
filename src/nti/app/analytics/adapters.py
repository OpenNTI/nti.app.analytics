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

from zope import component
from zope import interface

from nti.analytics import has_analytics

from nti.analytics.assessments import get_assignment_for_user
from nti.analytics.assessments import get_self_assessments_for_user_and_id

from nti.analytics.boards import get_topic_views
from nti.analytics.boards import get_topic_last_view

from nti.analytics.interfaces import IProgress

from nti.analytics.progress import DefaultProgress

from nti.analytics.resource_tags import get_note_views
from nti.analytics.resource_tags import get_note_last_view

from nti.app.analytics.usage_stats import CourseVideoUsageStats
from nti.app.analytics.usage_stats import CourseResourceUsageStats
from nti.app.analytics.usage_stats import UserCourseVideoUsageStats
from nti.app.analytics.usage_stats import UserCourseResourceUsageStats

from nti.app.products.courseware.interfaces import IViewStats
from nti.app.products.courseware.interfaces import IVideoUsageStats
from nti.app.products.courseware.interfaces import IResourceUsageStats

from nti.assessment.interfaces import IQAssignment
from nti.assessment.interfaces import IQuestionSet

from nti.contenttypes.courses.interfaces import ICourseInstance

from nti.dataserver.contenttypes.forums.interfaces import ITopic

from nti.dataserver.interfaces import INote
from nti.dataserver.interfaces import IUser

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IProgress)
@component.adapter(IUser, IQAssignment)
def _assignment_progress_for_user(user, assignment):
    """
    Given an assignment and a user, we
    attempt to determine the amount of progress the user
    has made on the assignment.  If we have nothing in which to
    gauge progress, we return None.
    """
    result = None
    # In local tests, about 100 objects are decorated in about 1s;
    # this is in UCOL with a lot of assignments but few assessments.
    assignment_id = getattr(assignment, 'ntiid', None)
    assignment_records = get_assignment_for_user(user, assignment_id)
    if assignment_records:
        # Simplistic implementation
        last_mod = max((x.timestamp for x in assignment_records))
        result = DefaultProgress(assignment_id, 1, 1, True,
                                 last_modified=last_mod)
    return result


@interface.implementer(IProgress)
@component.adapter(IUser, IQuestionSet)
def _assessment_progress_for_user(user, assessment):
    """
    Given a generic assessment and a user, we
    attempt to determine the amount of progress the user
    has made on the assignment.  If we have nothing in which to
    gauge progress, we return None.
    """
    # To properly check for assignment, we need the course to see
    # what the assignment ntiids are versus the possible self-assessment ids.
    # Maybe we're better off checking for assignment or self-assessment.
    # If we have a cache, the cost is trivial.
    # Or we only care about possible self-assessments here; if we have a record
    # great, else we do not return anything.
    assessment_id = getattr(assessment, 'ntiid', None)
    assessment_records = get_self_assessments_for_user_and_id(user, assessment_id)
    result = None
    if assessment_records:
        # Simplistic implementation
        last_mod = max((x.timestamp for x in assessment_records))
        result = DefaultProgress(assessment_id, 1, 1,
                                 True, last_modified=last_mod)
    return result


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
