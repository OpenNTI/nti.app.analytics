#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import not_none
from hamcrest import assert_that
from hamcrest import has_length
from hamcrest import contains_inanyorder

from nti.testing.time import time_monotonically_increases

import time
from datetime import datetime

import fudge

from zope import component

import zope.intid

from nti.app.products.courseware.interfaces import IViewStats

from nti.app.analytics.tests import NTIAnalyticsTestCase

from nti.analytics.database import get_analytics_db
from nti.analytics.database import boards as db_boards_view
from nti.analytics.database import resource_tags as db_tags_view

from nti.analytics.database.assessments import AssignmentsTaken
from nti.analytics.database.assessments import SelfAssessmentsTaken

from nti.analytics.database.root_context import _create_course

from nti.analytics.database.users import create_user

from nti.analytics.interfaces import IProgress

from nti.analytics.progress import get_assessment_progresses_for_course

from nti.analytics.tests import TestIdentifier

from nti.analytics_database.interfaces import IAnalyticsIntidIdentifier
from nti.analytics_database.interfaces import IAnalyticsNTIIDIdentifier
from nti.analytics_database.interfaces import IAnalyticsRootContextIdentifier

from nti.assessment.assignment import QAssignment

from nti.assessment.question import QQuestionSet

from nti.contenttypes.courses.courses import CourseInstance

from nti.dataserver.contenttypes.forums.forum import GeneralForum

from nti.dataserver.contenttypes.forums.post import GeneralForumComment

from nti.dataserver.contenttypes.forums.topic import GeneralTopic as Topic

from nti.dataserver.contenttypes.note import Note

from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.dataserver.users.users import User


def _create_topic_view(user_id, topic):
    time_length = 30
    event_time = time.time()
    db_boards_view.create_topic_view(user_id, None, event_time,
                                     1, None, topic, time_length)


def _create_note_view(user_id, note):
    event_time = time.time()
    db_tags_view.create_note_view(user_id, None, event_time,
                                  None, 1, note)


class _AbstractMockAnalyticTestClass(NTIAnalyticsTestCase):

    def setUp(self):
        self.analytics_db = get_analytics_db()
        gsm = component.getGlobalSiteManager()
        self.old_intid_util = gsm.getUtility(IAnalyticsIntidIdentifier)
        self.old_ntiid_util = gsm.getUtility(IAnalyticsNTIIDIdentifier)
        self.old_root_context_util = gsm.getUtility(IAnalyticsRootContextIdentifier)

        self.test_identifier = TestIdentifier()
        gsm.registerUtility(self.test_identifier,
                            IAnalyticsIntidIdentifier)
        gsm.registerUtility(self.test_identifier,
                            IAnalyticsNTIIDIdentifier)
        gsm.registerUtility(self.test_identifier,
                            IAnalyticsRootContextIdentifier)

    def tearDown(self):
        component.getGlobalSiteManager().unregisterUtility(self.test_identifier)
        component.getGlobalSiteManager().registerUtility(self.old_intid_util,
                                                         IAnalyticsIntidIdentifier)
        component.getGlobalSiteManager().registerUtility(self.old_ntiid_util,
                                                         IAnalyticsNTIIDIdentifier)
        component.getGlobalSiteManager().registerUtility(self.old_root_context_util,
                                                         IAnalyticsRootContextIdentifier)


class TestAnalyticAdapters(_AbstractMockAnalyticTestClass):

    def _get_assignment(self):
        new_assignment = QAssignment()
        new_assignment.ntiid = self.assignment_id = u'tag:nextthought.com,2015:ntiid1'
        return new_assignment

    def _get_self_assessment(self):
        assessment = QQuestionSet()
        assessment.ntiid = self.question_set_id = u'tag:nextthought.com,2015:question_set1'
        return assessment

    def _install_user(self):
        self.user = User.create_user(username='derpity', dataserver=self.ds)
        self.user_id = create_user(self.user).user_id
        return self.user

    def _install_course(self):
        course_id = 1
        new_course = CourseInstance()
        setattr(new_course, '_ds_intid', course_id)
        _create_course(self.analytics_db, new_course, course_id)
        return new_course

    def _install_assignment(self, db):
        new_object = AssignmentsTaken(user_id=self.user_id,
                                      session_id=2,
                                      timestamp=datetime.utcnow(),
                                      course_id=1,
                                      assignment_id=self.assignment_id,
                                      submission_id=2,
                                      time_length=10)
        db.session.add(new_object)
        db.session.flush()

    def _install_self_assessment(self, db, submit_id=1):
        new_object = SelfAssessmentsTaken(user_id=self.user_id,
                                          session_id=2,
                                          timestamp=datetime.utcnow(),
                                          course_id=1,
                                          assignment_id=self.question_set_id,
                                          submission_id=submit_id,
                                          time_length=10)
        db.session.add(new_object)
        db.session.flush()

    @WithMockDSTrans
    @fudge.patch('dm.zope.schema.schema.Object._validate')
    def test_progress_adapter(self, mock_validate):
        "Test progress for assessment adapters and courses."
        mock_validate.is_callable().returns(True)

        user = self._install_user()
        course = self._install_course()
        assignment = self._get_assignment()
        question_set = self._get_self_assessment()

        # No initial progress for assessments
        result = component.queryMultiAdapter((user, assignment), IProgress)
        assert_that(result, none())

        result = component.queryMultiAdapter((user, question_set), IProgress)
        assert_that(result, none())

        # Install assignment
        self._install_assignment(self.analytics_db)
        assignment_progress = component.queryMultiAdapter(
            (user, assignment), IProgress)
        assert_that(assignment_progress, not_none())
        assert_that(assignment_progress.HasProgress, is_(True))

        result = component.queryMultiAdapter((user, question_set), IProgress)
        assert_that(result, none())

        # Verify progress for course
        progressess = get_assessment_progresses_for_course(user, course)
        assert_that(progressess, has_length(1))
        assert_that(progressess[0], is_(assignment_progress))

        # Self-assessment
        self._install_self_assessment(self.analytics_db)
        result = component.queryMultiAdapter((user, assignment), IProgress)
        assert_that(result, not_none())
        assert_that(result.HasProgress, is_(True))

        assessment_progress = component.queryMultiAdapter(
            (user, question_set), IProgress)
        assert_that(assessment_progress, not_none())
        assert_that(assessment_progress.HasProgress, is_(True))

        # Verify progress course w/one of each
        # Assignment progress is unchanged.
        progressess = get_assessment_progresses_for_course(user, course)
        assert_that(progressess, has_length(2))
        assert_that(progressess,
                    contains_inanyorder(assessment_progress, assignment_progress))

        # Self-assessment; duped is ok
        self._install_self_assessment(self.analytics_db, submit_id=100)
        assessment_progress = component.queryMultiAdapter(
            (user, question_set), IProgress)
        assert_that(assessment_progress, not_none())
        assert_that(assessment_progress.HasProgress, is_(True))

        # Verify progress course w/one of each plus multi-assessments
        # The new self-assessment timestamp is in our progress.
        progressess = get_assessment_progresses_for_course(user, course)
        assert_that(progressess, has_length(2))
        assert_that(progressess,
                    contains_inanyorder(assessment_progress, assignment_progress))


class TestViewStatAdapters(_AbstractMockAnalyticTestClass):

    def _get_user(self):
        user = User.create_user(username=u'david_copperfield', 
							 	dataserver=self.ds)
        return user

    def _get_other_user(self):
        user = User.create_user(username=u'rod_stewart', 
							 	dataserver=self.ds)
        return user

    def _create_comment(self, user, parent):
        intids = component.getUtility(zope.intid.IIntIds)
        comment = GeneralForumComment()
        comment.creator = user
        comment.body = (u'test222',)
        comment.__parent__ = parent
        intids.register(comment)
        #parent['bleh'] = comment
        return comment

    def _create_topic(self, user):
        intids = component.getUtility(zope.intid.IIntIds)
        course = CourseInstance()
        course._ds_intid = 123456
        forum = GeneralForum()
        forum.creator = user
        forum.NTIID = u'tag:nextthought.com,2011-10:imaforum'
        forum.__parent__ = course
        intids.register(forum)

        intids = component.getUtility(zope.intid.IIntIds)
        topic = Topic()
        topic.creator = user
        topic.__parent__ = forum
        intids.register(topic)
        return topic

#     @WithMockDSTrans
#     @time_monotonically_increases
#     def test_topic_views(self):
#         user = self._get_user()
#         other_user = self._get_other_user()
#         topic = self._create_topic( user )
#
#         # Empty
#         result = IViewStats( topic )
#         assert_that( result.view_count, is_( 0 ))
#         result = component.queryMultiAdapter( (topic, user), IViewStats )
#         assert_that( result.view_count, is_( 0 ))
#         assert_that( result.new_reply_count_for_user, is_( 0 ))
#
#         # One view
#         _create_topic_view( user, topic )
#         result = IViewStats( topic )
#         assert_that( result.view_count, is_( 1 ))
#
#         topic_view_count = 5
#         for _ in range( topic_view_count ):
#             _create_topic_view( 1, topic )
#
#         topic_view_count += 1
#         result = IViewStats( topic )
#         assert_that( result.view_count, is_( topic_view_count ))
#         result = component.queryMultiAdapter( (topic, user), IViewStats )
#         assert_that( result.view_count, is_( topic_view_count ))
#         assert_that( result.new_reply_count_for_user, is_( 0 ))
#
#         # Replies without views
#         comment1 = self._create_comment( other_user, parent=topic )
#
#         result = IViewStats( topic )
#         assert_that( result.view_count, is_( topic_view_count ))
#         result = component.queryMultiAdapter( (topic, user), IViewStats )
#         assert_that( result.view_count, is_( topic_view_count ))
#         assert_that( result.new_reply_count_for_user, is_( 1 ))
#
#         comment2 = self._create_comment( other_user, parent=comment1 )
#         self._create_comment( other_user, parent=comment2 )
#
#         result = IViewStats( topic )
#         assert_that( result.view_count, is_( topic_view_count ))
#         result = component.queryMultiAdapter( (topic, user), IViewStats )
#         assert_that( result.view_count, is_( topic_view_count ))
#         assert_that( result.new_reply_count_for_user, is_( 3 ))
#
#         # Now a view resets everything
#         _create_topic_view( user, topic )
#         result = IViewStats( topic )
#         assert_that( result.view_count, is_( topic_view_count + 1 ))
#         result = component.queryMultiAdapter( (topic, user), IViewStats )
#         assert_that( result.view_count, is_( topic_view_count + 1 ))
#         assert_that( result.new_reply_count_for_user, is_( 0 ))

    def _create_note(self, user, parent_note=None):
        note = Note()
        note.body = (u'test',)
        note.creator = user
        note.containerId = u'tag:nti:foo'
        if parent_note is not None:
            note.inReplyTo = parent_note
        user.addContainedObject(note)
        return note

    @WithMockDSTrans
    @time_monotonically_increases
    @fudge.patch('nti.analytics.database.resource_tags.get_root_context')
    def test_note_views(self, mock_root_context):
        mock_root_context.is_callable().returns(1)
        user = self._get_user()
        other_user = self._get_other_user()
        note = self._create_note(user)

        # Empty
        result = IViewStats(note)
        assert_that(result.view_count, is_(0))
        result = component.queryMultiAdapter((note, user), IViewStats)
        assert_that(result.view_count, is_(0))
        assert_that(result.new_reply_count_for_user, is_(0))

        # Single
        _create_note_view(other_user, note)
        result = IViewStats(note)
        assert_that(result.view_count, is_(1))
        result = component.queryMultiAdapter((note, user), IViewStats)
        assert_that(result.view_count, is_(1))
        assert_that(result.new_reply_count_for_user, is_(0))

        # Many views
        note_view_count = 5
        for _ in range(note_view_count):
            _create_note_view(other_user, note)

        note_view_count += 1
        result = IViewStats(note)
        assert_that(result.view_count, is_(note_view_count))
        result = component.queryMultiAdapter((note, user), IViewStats)
        assert_that(result.view_count, is_(note_view_count))
        assert_that(result.new_reply_count_for_user, is_(0))

        # Replies without views
        note2 = self._create_note(other_user, parent_note=note)

        result = IViewStats(note)
        assert_that(result.view_count, is_(note_view_count))
        result = component.queryMultiAdapter((note, user), IViewStats)
        assert_that(result.view_count, is_(note_view_count))
        assert_that(result.new_reply_count_for_user, is_(1))

        note3 = self._create_note(other_user, parent_note=note2)
        self._create_note(other_user, parent_note=note3)

        result = IViewStats(note)
        assert_that(result.view_count, is_(note_view_count))
        result = component.queryMultiAdapter((note, user), IViewStats)
        assert_that(result.view_count, is_(note_view_count))
        assert_that(result.new_reply_count_for_user, is_(3))

        # Now a view resets everything
        _create_note_view(user, note)
        result = IViewStats(note)
        assert_that(result.view_count, is_(note_view_count + 1))
        result = component.queryMultiAdapter((note, user), IViewStats)
        assert_that(result.view_count, is_(note_view_count + 1))
        assert_that(result.new_reply_count_for_user, is_(0))
