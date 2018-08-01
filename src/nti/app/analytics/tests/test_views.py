#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import assert_that
from hamcrest import contains
from hamcrest import contains_inanyorder
from hamcrest import ends_with
from hamcrest import is_
from hamcrest import none
from hamcrest import has_key
from hamcrest import has_item
from hamcrest import not_none
from hamcrest import has_entry
from hamcrest import has_length
from hamcrest import has_entries
from hamcrest import contains_string
from hamcrest import is_not as does_not
from hamcrest import has_key as have_key
from hamcrest import greater_than

import fudge

from nti.testing.time import time_monotonically_increases

import six
import time
import calendar

from datetime import datetime
from datetime import timedelta

from webob.datetime_utils import serialize_date

from zope import component
from zope import interface

from nti.analytics_database.interfaces import IAnalyticsDB

from nti.analytics.common import timestamp_type

from nti.analytics.interfaces import DEFAULT_ANALYTICS_BATCH_SIZE
from nti.analytics.interfaces import DEFAULT_ANALYTICS_FREQUENCY

from nti.analytics.database.assessments import AssignmentViews
from nti.analytics.database.assessments import SelfAssessmentViews

from nti.analytics.database.blogs import BlogsViewed

from nti.analytics.database.boards import TopicsViewed

from nti.analytics.database.database import AnalyticsDB

from nti.analytics.database.enrollments import CourseCatalogViews

from nti.analytics.database.locations import Location
from nti.analytics.database.locations import IpGeoLocation

from nti.analytics.database.resource_tags import NotesViewed

from nti.analytics.database.resource_views import VideoEvents
from nti.analytics.database.resource_views import ResourceViews
from nti.analytics.database.resource_views import VideoPlaySpeedEvents
from nti.analytics.database.resource_views import create_course_resource_view
from nti.analytics.database.resource_views import create_video_event

from nti.analytics.database.root_context import get_root_context_id

from nti.analytics.database.sessions import Sessions

from nti.analytics.database.users import create_user

from nti.analytics.model import BlogViewEvent
from nti.analytics.model import NoteViewEvent
from nti.analytics.model import ResourceEvent
from nti.analytics.model import SkipVideoEvent
from nti.analytics.model import TopicViewEvent
from nti.analytics.model import WatchVideoEvent
from nti.analytics.model import AnalyticsSession
from nti.analytics.model import AnalyticsSessions
from nti.analytics.model import AssignmentViewEvent
from nti.analytics.model import BatchResourceEvents
from nti.analytics.model import CourseCatalogViewEvent
from nti.analytics.model import SelfAssessmentViewEvent
from nti.analytics.model import VideoPlaySpeedChangeEvent

from nti.analytics.sessions import _add_session
from nti.analytics.sessions import get_current_session_id

from nti.analytics.tests import TestIdentifier

from nti.analytics_database.interfaces import IAnalyticsIntidIdentifier
from nti.analytics_database.interfaces import IAnalyticsNTIIDIdentifier
from nti.analytics_database.interfaces import IAnalyticsRootContextIdentifier

from nti.app.analytics import SYNC_PARAMS
from nti.app.analytics import ACTIVE_USERS
from nti.app.analytics import ACTIVE_TIMES_SUMMARY
from nti.app.analytics import ACTIVITY_SUMMARY_BY_DATE
from nti.app.analytics import ANALYTICS_SESSION_HEADER

from nti.app.products.courseware.tests import LegacyInstructedCourseApplicationTestLayer

from nti.app.analytics.utils import get_session_id_from_request

from nti.app.analytics.views import GEO_LOCATION_VIEW
from nti.app.analytics.views import UserLocationJsonView

from nti.app.assessment.history import UsersCourseAssignmentHistoryItem

from nti.app.assessment.interfaces import IUsersCourseAssignmentHistory

from nti.app.products.courseware.workspaces import enrollment_from_record

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.app.testing.webtest import TestApp

from nti.app.contentlibrary.tests import PersistentApplicationTestLayer

from nti.assessment.assignment import QAssignment
from nti.assessment.assignment import QAssignmentSubmissionPendingAssessment

from nti.assessment.submission import AssignmentSubmission

from nti.contenttypes.courses.courses import CourseInstance
from nti.contenttypes.courses.courses import ContentCourseSubInstance

from nti.contenttypes.courses.interfaces import ICourseInstance
from nti.contenttypes.courses.interfaces import ICourseEnrollmentManager

from nti.contenttypes.courses.sharing import CourseInstanceSharingScope

from nti.dataserver.interfaces import ILinkExternalHrefOnly

from nti.dataserver.tests import mock_dataserver

from nti.dataserver.users.users import User

from nti.externalization import internalization

from nti.externalization.externalization import toExternalObject

from nti.ntiids.ntiids import find_object_with_ntiid
from nti.ntiids.oids import to_external_ntiid_oid

from nti.links.externalization import render_link
from nti.links.links import Link

timestamp = calendar.timegm(datetime.utcnow().timetuple())

user = u'sjohnson@nextthought.com'
course = u'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.course_info'
context_path = [u'DASHBOARD', u'ntiid:tag_blah']
resource_id = u'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.subsec:BOOK_Three_PART_11'
time_length = 30
video_start_time = 13
video_end_time = 39
with_transcript = True

blog_id = u'1111'
note_id = u'with_container'
topic_id = u'with_parent'

blog_event = BlogViewEvent(user=user,
                           timestamp=timestamp,
                           blog_id=blog_id,
                           Duration=time_length)

note_event = NoteViewEvent(user=user,
                           timestamp=timestamp,
                           RootContextID=course,
                           note_id=note_id,
                           Duration=time_length)

topic_event = TopicViewEvent(user=user,
                             timestamp=timestamp,
                             RootContextID=course,
                             topic_id=topic_id,
                             Duration=time_length)

course_catalog_event = CourseCatalogViewEvent(user=user,
                                              timestamp=timestamp,
                                              RootContextID=course,
                                              Duration=time_length)

video_event = SkipVideoEvent(user=user,
                             timestamp=timestamp,
                             RootContextID=course,
                             context_path=context_path,
                             ResourceId=resource_id,
                             Duration=time_length,
                             video_start_time=video_start_time,
                             video_end_time=video_end_time,
                             with_transcript=with_transcript)

# Essentially a video start event
watch_video_event = WatchVideoEvent(user=user,
                                    timestamp=timestamp,
                                    RootContextID=course,
                                    context_path=context_path,
                                    ResourceId=resource_id,
                                    Duration=None,
                                    video_start_time=video_start_time,
                                    video_end_time=None,
                                    with_transcript=with_transcript)

resource_kwargs = {'user': user,
                   'timestamp': timestamp,
                   'RootContextID': course,
                   'context_path': context_path,
                   'ResourceId': resource_id,
                   'Duration': time_length}

resource_event = ResourceEvent(**resource_kwargs)

self_assess_kwargs = dict(**resource_kwargs)
self_assess_kwargs['ContentId'] = self_assess_kwargs.pop('ResourceId')
self_assess_kwargs['ResourceId'] = question_set_id = u'tag:nextthought.com,2011-10:OU-NAQ-CLC3403_LawAndJustice.naq.set.qset:QUIZ1_aristotle'
self_assessment_event = SelfAssessmentViewEvent(**self_assess_kwargs)

assignment_kwargs = dict(**resource_kwargs)
assignment_kwargs['ResourceId'] = assignment_id = u'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.sec:QUIZ_01.01'
assignment_event = AssignmentViewEvent(**assignment_kwargs)

play_speed_event = VideoPlaySpeedChangeEvent(user=user,
                                             timestamp=timestamp,
                                             RootContextID=course,
                                             ResourceId=resource_id,
                                             VideoTime=video_start_time,
                                             OldPlaySpeed=2,
                                             NewPlaySpeed=8)


def _internalize(ext):
    factory = internalization.find_factory_for(ext)
    _object = factory()
    internalization.update_from_external_object(_object, ext)
    return _object


class _AbstractTestViews(ApplicationLayerTest):

    layer = LegacyInstructedCourseApplicationTestLayer

    def setUp(self):
        self.db = AnalyticsDB(
            dburi='sqlite://', testmode=True, defaultSQLite=True)
        component.getGlobalSiteManager().registerUtility(self.db,
                                                         IAnalyticsDB)
        self.session = self.db.session

        gsm = component.getGlobalSiteManager()
        self.old_intid_util = gsm.getUtility(IAnalyticsIntidIdentifier)
        self.old_ntiid_util = gsm.getUtility(IAnalyticsNTIIDIdentifier)
        self.old_root_context_util = gsm.getUtility(
            IAnalyticsRootContextIdentifier)

        self.test_identifier = TestIdentifier()
        gsm.registerUtility(self.test_identifier,
                            IAnalyticsIntidIdentifier)
        gsm.registerUtility(self.test_identifier,
                            IAnalyticsNTIIDIdentifier)
        gsm.registerUtility(self.test_identifier,
                            IAnalyticsRootContextIdentifier)

    def tearDown(self):
        component.getGlobalSiteManager().unregisterUtility(self.db,
                                                           provided=IAnalyticsDB)
        self.session.close()
        component.getGlobalSiteManager().unregisterUtility(self.test_identifier)
        component.getGlobalSiteManager().registerUtility(self.old_intid_util,
                                                         IAnalyticsIntidIdentifier)
        component.getGlobalSiteManager().registerUtility(self.old_ntiid_util,
                                                         IAnalyticsNTIIDIdentifier)
        component.getGlobalSiteManager().registerUtility(self.old_root_context_util,
                                                         IAnalyticsRootContextIdentifier)


class TestBatchEvents(_AbstractTestViews):

    default_origin = 'http://janux.ou.edu'

    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    @fudge.patch('nti.analytics.resource_views.find_object_with_ntiid')
    @fudge.patch('nti.analytics.resource_views._get_root_context')
    @fudge.patch('nti.analytics.resource_views._get_course')
    @fudge.patch('nti.analytics.database.blogs._get_blog_id')
    @fudge.patch('nti.analytics.database.resource_tags._get_note_id')
    @fudge.patch('nti.analytics.database.boards._get_forum_id_from_forum')
    @fudge.patch('nti.analytics.database.boards._get_topic_id_from_topic')
    def test_batch_event(self, mock_get_object, mock_root_context, mock_get_course,
                         mock_get_blog, mock_get_note, mock_get_forum, mock_get_topic):
        mock_parent = mock_get_object.is_callable().returns_fake()
        mock_parent.has_attr(__parent__=201)
        mock_parent.has_attr(containerId=333)

        course = CourseInstance()
        mock_root_context.is_callable().returns(course)
        mock_get_course.is_callable().returns(course)

        mock_get_blog.is_callable().returns(1)
        mock_get_note.is_callable().returns(2)
        mock_get_forum.is_callable().returns(3)
        mock_get_topic.is_callable().returns(4)

        # Event specified session id
        course_catalog_session_id = 11111
        course_catalog_event.SessionID = course_catalog_session_id

        io = BatchResourceEvents(events=[video_event, resource_event, course_catalog_event,
                                         blog_event, note_event, topic_event, play_speed_event,
                                         assignment_event, self_assessment_event])

        ext_obj = toExternalObject(io)

        # Add a session header
        session_id = 9999
        headers = {ANALYTICS_SESSION_HEADER: str(session_id)}
        batch_url = '/dataserver2/analytics/batch_events'

        # Must be authenticated
        TestApp(self.app).post_json(batch_url, ext_obj, status=401)

        # Upload our events
        self.testapp.post_json(batch_url,
                               ext_obj,
                               headers=headers)

        results = self.session.query(SelfAssessmentViews).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(AssignmentViews).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(VideoEvents).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(CourseCatalogViews).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(course_catalog_session_id))

        results = self.session.query(ResourceViews).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(BlogsViewed).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(NotesViewed).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(TopicsViewed).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(VideoPlaySpeedEvents).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        # We should be able to send the same events without error or duplicates
        # in database.
        self.testapp.post_json(batch_url,
                               ext_obj,
                               headers=headers,
                               status=200)

        results = self.session.query(SelfAssessmentViews).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(AssignmentViews).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(VideoEvents).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(CourseCatalogViews).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(course_catalog_session_id))

        results = self.session.query(ResourceViews).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(BlogsViewed).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(NotesViewed).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        results = self.session.query(TopicsViewed).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, is_(session_id))

        # Send a video start event successfully
        io = BatchResourceEvents(events=[watch_video_event])
        ext_obj = toExternalObject(io)
        session_id = 9999
        headers = {ANALYTICS_SESSION_HEADER: str(session_id)}

        self.testapp.post_json(batch_url,
                               ext_obj,
                               headers=headers,
                               status=200)

        results = self.session.query(VideoEvents).all()
        assert_that(results, has_length(2))

        future = time.time() + 1000000
        result = self.testapp.get(batch_url+'?notAfter='+str(future), status=200).json_body
        assert_that(result, has_entry('Items', has_length(1)))

    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    @fudge.patch('nti.analytics.resource_views.find_object_with_ntiid')
    @fudge.patch('nti.analytics.resource_views._get_root_context')
    @fudge.patch('nti.analytics.resource_views._get_course')
    def test_malformed_event(self, mock_get_object, mock_find_object, mock_get_course):
        mock_parent = mock_get_object.is_callable().returns_fake()
        mock_parent.has_attr(__parent__=201)
        mock_parent.has_attr(containerId=333)

        course = CourseInstance()
        mock_find_object.is_callable().returns(course)
        mock_get_course.is_callable().returns(course)

        io = BatchResourceEvents(
            events=[video_event, resource_event, course_catalog_event])

        ext_obj = toExternalObject(io)

        # Make a malformed event; validate resource_id field.
        events = ext_obj.get('events')
        new_events = []
        for event in events:
            if event.get('MimeType') == ResourceEvent.mime_type:
                event.pop('ResourceId')
            elif 'ResourceId' in event:
                event['resource_id'] = event.pop('ResourceId')
            new_events.append(event)
        ext_obj['events'] = new_events

        # Upload our events
        batch_url = '/dataserver2/analytics/batch_events'
        self.testapp.post_json(batch_url,
                               ext_obj,
                               status=200)

        results = self.session.query(VideoEvents).all()
        assert_that(results, has_length(1))

        results = self.session.query(CourseCatalogViews).all()
        assert_that(results, has_length(1))

        # We insert all but the single malformed event
        results = self.session.query(ResourceViews).all()
        assert_that(results, has_length(0))

    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    def test_batch_params(self):
        batch_url = '/dataserver2/analytics/@@' + SYNC_PARAMS
        result = self.testapp.get(batch_url, status=200)
        result = result.json_body
        assert_that(result,
                    has_entries('RecommendedAnalyticsSyncInterval', DEFAULT_ANALYTICS_FREQUENCY,
                                'RecommendedBatchEventsSendFrequency', DEFAULT_ANALYTICS_FREQUENCY,
                                'RecommendedBatchEventsSize', DEFAULT_ANALYTICS_BATCH_SIZE,
                                'RecommendedBatchSessionsSendFrequency', DEFAULT_ANALYTICS_FREQUENCY,
                                'RecommendedBatchSessionsSize', DEFAULT_ANALYTICS_BATCH_SIZE))


class TestAnalyticsSession(_AbstractTestViews):

    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    def test_session(self):
        results = self.session.query(Sessions).all()
        assert_that(results, has_length(0))

        # New session
        session_url = '/dataserver2/analytics/sessions/@@analytics_session'
        res = self.testapp.post_json(session_url)

        assert_that(res.headers['Set-Cookie'], is_('nti.da_session=1; Path=/'))

        first_session_id = 1
        results = self.session.query(Sessions).all()
        assert_that(results, has_length(1))
        assert_that(results[0].session_id, first_session_id)
        assert_that(results[0].end_time, none())

        with mock_dataserver.mock_db_trans(self.ds):
            user = User.get_user(self.extra_environ_default_user)
            current_session_id = get_current_session_id(user)
            assert_that(current_session_id, none())

        # New session #2
        res = self.testapp.post_json(session_url)
        assert_that(res.headers['Set-Cookie'], is_('nti.da_session=2; Path=/'))

        # Our request was with the old cookie_id
        cookie_id = get_session_id_from_request(res.request)
        assert_that(cookie_id, is_(first_session_id))

        results = self.session.query(Sessions).all()
        assert_that(results, has_length(2))
        # This last call implicitly ends the previous session.
        first_session = self.session.query(Sessions).filter(
                                           Sessions.session_id == first_session_id).one()
        assert_that(first_session.end_time, not_none())

        with mock_dataserver.mock_db_trans(self.ds):
            user = User.get_user(self.extra_environ_default_user)
            current_session_id = get_current_session_id(user)
            assert_that(current_session_id, none())

        # Batch information
        io = BatchResourceEvents(events=[video_event, resource_event,
                                         course_catalog_event])
        batch_events = toExternalObject(io)

        # End our session
        end_session_url = '/dataserver2/analytics/sessions/@@end_analytics_session'

        timestamp = timestamp_type(1)
        res = self.testapp.post_json(end_session_url,
                                     {'session_id': 2,
                                      'timestamp': 1,
                                      'batch_events': batch_events})

        # Request with second session id
        assert_that(get_session_id_from_request(res.request), is_(2))
        # Response cookie is deleted
        assert_that(res.headers['Set-Cookie'],
                    contains_string('nti.da_session=; Max-Age=0;'))

        session_record = self.session.query(Sessions).filter(
                                            Sessions.session_id == 2).first()
        assert_that(session_record, not_none())
        assert_that(session_record.end_time, is_(timestamp))

        with mock_dataserver.mock_db_trans(self.ds):
            user = User.get_user(self.extra_environ_default_user)
            current_session_id = get_current_session_id(user)
            assert_that(current_session_id, none())

    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    def test_sessions(self):
        results = self.session.query(Sessions).all()
        assert_that(results, has_length(0))

        # No end time
        session = AnalyticsSession(SessionStartTime=timestamp)
        sessions = [session, session, session]

        session_count = len(sessions)
        io = AnalyticsSessions(sessions=sessions)
        ext_obj = toExternalObject(io)

        # Send our sessions over
        session_url = '/dataserver2/analytics/sessions'
        result = self.testapp.post_json(session_url,
                                        ext_obj,
                                        status=200)

        new_sessions = [_internalize(x) for x in result.json_body]
        session_ids = [x.SessionID for x in new_sessions]
        assert_that(new_sessions, has_length(session_count))
        assert_that(session_ids, contains_inanyorder(1, 2, 3))

        results = self.session.query(Sessions).all()
        assert_that(results, has_length(3))

        # This is header driven.
        current_session_id = get_current_session_id(user)
        assert_that(current_session_id, none())

        # Now update with an endtime
        session = new_sessions[0]
        session_id = session.SessionID

        db_session = self.session.query(Sessions) \
                                 .filter(Sessions.session_id == session_id).one()
        assert_that(db_session, not_none())
        assert_that(db_session.end_time, none())

        end_time = timestamp + 1
        session.SessionEndTime = end_time
        sessions = [session]
        io = AnalyticsSessions(sessions=sessions)
        ext_obj = toExternalObject(io)

        session_url = '/dataserver2/analytics/sessions'
        result = self.testapp.post_json(session_url,
                                        ext_obj,
                                        status=200)

        new_sessions = [_internalize(x) for x in result.json_body]
        session_ids = [x.SessionID for x in new_sessions]
        assert_that(session_ids, has_length(1))
        assert_that(session_ids[0], is_(session_id))

        db_session = self.session.query(Sessions) \
                                 .filter(Sessions.session_id == session_id).one()
        assert_that(db_session, not_none())
        assert_that(db_session.end_time, not_none())

    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    def test_update_session_with_invalid(self):
        end_time = timestamp + 1

        new_session = AnalyticsSession(SessionStartTime=timestamp)
        session_with_made_up_id = AnalyticsSession(SessionID=99999, SessionStartTime=timestamp,
                                                   SessionEndTime=end_time)
        sessions = [new_session, session_with_made_up_id]

        session_count = len(sessions)
        io = AnalyticsSessions(sessions=sessions)
        ext_obj = toExternalObject(io)

        # Send our sessions over
        session_url = '/dataserver2/analytics/sessions'
        result = self.testapp.post_json(session_url,
                                        ext_obj,
                                        status=200)

        results = result.json_body
        assert_that(results, has_length(session_count))

        # First session is valid
        valid_session = _internalize(results[0])
        assert_that(valid_session, not_none())
        assert_that(valid_session.SessionID, is_(1))

        # Next is an error
        key = 'Error'
        assert_that(results[1], has_key(key))


class TestProgressView(_AbstractTestViews):

    def _create_course(self):
        content_unit = find_object_with_ntiid(course)
        course_obj = self.course = ICourseInstance(content_unit)
        get_root_context_id(self.db, course_obj, create=True)

    def _create_video_event(self, user, resource_val, max_time_length=None, video_end_time=None):
        test_session_id = 1
        time_length = 30
        video_event_type = 'WATCH'
        video_start_time = 30
        video_end_time = video_end_time
        with_transcript = True
        event_time = time.time()
        context_path = [u'Blah', u'Bleh']
        create_video_event(user,
                           test_session_id, event_time,
                           self.course, context_path,
                           resource_val, time_length, max_time_length,
                           video_event_type, video_start_time,
                           video_end_time,  with_transcript, None)

    def _create_resource_view(self, user, resource_val):
        test_session_id = 1
        time_length = 30
        event_time = time.time()
        context_path = [u'Blah', u'Bleh']
        create_course_resource_view(user,
                                    test_session_id, event_time,
                                    self.course, context_path,
                                    resource_val, time_length)

    def _get_assignment(self):
        new_assignment = QAssignment()
        new_assignment.ntiid = self.assignment_id = u'tag:nextthought.com,2015:ntiid1'
        return new_assignment

    def _install_user(self, user_id):
        with mock_dataserver.mock_db_trans(self.ds):
            self.user = User.get_user(user_id)
            self.user_id = create_user(self.user).user_id
            return self.user

    def _do_get_url(self, url, status=200, response=None):
        """
        Gets the url, using the given response (if available)
        as last modified.
        """
        if response and response.last_modified:
            response = self.testapp.get(url,
                                        headers={'If-Modified-Since':
                                                 serialize_date(response.last_modified)},
                                        status=status)
        else:
            response = self.testapp.get(url, status=status)
        return response

    def _get_progress(self, status=200, response=None):
        outline_ntiid = "tag:nextthought.com,2011-10:OU-NTICourseOutlineNode-CLC3403_LawAndJustice.course_info.2.1"
        # Why does this outline navigation path no longer work?
        #progress_url = '/dataserver2/users/CLC3403.ou.nextthought.com/LegacyCourses/CLC3403/Outline/2/1/Progress'
        progress_url = "/dataserver2/Objects/%s/Progress" % outline_ntiid
        return self._do_get_url(progress_url, status, response)

    def _get_video_progress(self, status=200, response=None):
        progress_url = '/dataserver2/users/CLC3403.ou.nextthought.com/LegacyCourses/CLC3403/VideoProgress'
        return self._do_get_url(progress_url, status, response)

    def _setup_mocks(self, mock_adapter, mock_find_object, mock_validate, mock_no_submit, assignment_id):
        mock_validate.is_callable().returns(True)
        mock_adapter.is_callable().returns(object())

        def _get_assignment(key):
            "Get our assignment, or fallback to ntiids lookup."
            if key == assignment_id:
                assignment_object = self._get_assignment()
                assignment_object.ntiid = assignment_id
                return assignment_object
            return find_object_with_ntiid(key)

        assignment_object = self._get_assignment()
        assignment_object.ntiid = assignment_id
        mock_find_object.is_callable().calls(_get_assignment)
        mock_no_submit.is_callable().returns(False)

    @time_monotonically_increases
    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    @fudge.patch('nti.app.products.courseware.adapters._content_unit_to_course',
                 'nti.app.contenttypes.presentation.views.completion_views.find_object_with_ntiid',
                 'dm.zope.schema.schema.Object._validate',
                 'nti.app.products.gradebook.completion._is_assignment_no_submit')
    def test_progress(self, mock_adapter, mock_find_object, mock_validate, mock_no_submit):
        video1 = u'tag:nextthought.com,2011-10:OU-NTIVideo-CLC3403_LawAndJustice.ntivideo.video_10.03'
        video2 = u'tag:nextthought.com,2011-10:OU-NTIVideo-CLC3403_LawAndJustice.ntivideo.video_10.02'
        resource1 = u'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.lec:10_LESSON'
        assignment1 = u'tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.sec:QUIZ_10.01'

        self._setup_mocks(mock_adapter, mock_find_object,
                          mock_validate, mock_no_submit, assignment1)

        # Empty progress/empty video progress
        response = self._get_progress()
        result = response.json_body['Items']
        assert_that(result, has_length(0))

        video_response = self._get_video_progress()
        result = video_response.json_body['Items']
        assert_that(result, has_length(0))

        user_id = 'sjohnson@nextthought.com'
        user = self._install_user(user_id)

        # Now a video event
        max_progress = 120
        with mock_dataserver.mock_db_trans(self.ds):
            user = User.get_user(user_id)
            self._create_course()
            self._create_video_event(user=user,
                                     resource_val=video1,
                                     max_time_length=max_progress,
                                     video_end_time=30)

        response = self._get_progress(response=response)
        result = response.json_body['Items']
        assert_that(result, has_length(1))
        assert_that(result, contains(video1))

        video_progress = result.get(video1)
        assert_that(video_progress,
                    has_entry('MaxPossibleProgress', max_progress))
        assert_that(video_progress, has_entry('AbsoluteProgress', 30))
        assert_that(video_progress, has_entry('HasProgress', True))
        assert_that(video_progress, has_entry('MostRecentEndTime', 30))

        # Video progress for course
        video_response = self._get_video_progress()
        result = video_response.json_body['Items']
        assert_that(result, has_length(1))
        assert_that(result, contains(video1))

        # Same video event
        with mock_dataserver.mock_db_trans(self.ds):
            user = User.get_user(user_id)
            self._create_video_event(user=user, resource_val=video1)
        response = self._get_progress(response=response)

        result = response.json_body['Items']
        assert_that(result, has_length(1))
        assert_that(result, has_key(video1))

        video_progress = result.get(video1)
        assert_that(video_progress,
                    has_entry('MaxPossibleProgress', max_progress))
        assert_that(video_progress, has_entry('AbsoluteProgress', 60))
        assert_that(video_progress, has_entry('HasProgress', True))
        assert_that(video_progress, has_entry('MostRecentEndTime', None))

        video_response = self._get_video_progress()
        result = video_response.json_body['Items']
        assert_that(result, has_length(1))
        assert_that(result, contains(video1))

        # New video doesn't affect old video
        with mock_dataserver.mock_db_trans(self.ds):
            user = User.get_user(user_id)
            self._create_video_event(user=user, resource_val=video2)
        response = self._get_progress(response=response)

        result = response.json_body['Items']
        assert_that(result, has_length(2))
        assert_that(result, contains_inanyorder(video1, video2))

        video_progress = result.get(video1)
        assert_that(video_progress,
                    has_entry('MaxPossibleProgress', max_progress))
        assert_that(video_progress, has_entry('AbsoluteProgress', 60))
        assert_that(video_progress, has_entry('HasProgress', True))
        assert_that(video_progress, has_entry('MostRecentEndTime', None))

        video_response = self._get_video_progress()
        result = video_response.json_body['Items']
        assert_that(result, has_length(2))
        assert_that(result, contains_inanyorder(video1, video2))

        # Now a resource view
        with mock_dataserver.mock_db_trans(self.ds):
            user = User.get_user(user_id)
            self._create_resource_view(user=user, resource_val=resource1)
        response = self._get_progress(response=response)

        result = response.json_body['Items']
        assert_that(result, has_length(3))
        assert_that(result, contains_inanyorder(video1, video2, resource1))

        resource_progress = result.get(resource1)
        assert_that(resource_progress, has_entry('MaxPossibleProgress', 2))
        assert_that(resource_progress, has_entry('AbsoluteProgress', 1))
        assert_that(resource_progress, has_entry('HasProgress', True))

        # Now a 304
        self._get_progress(response=response, status=304)

        # Now an assignment
        # Completion is based on a grade for a no_submit (assignment without
        # questions, which is what we have here). We want to treat this as a
        # submission assignment, complete upon submission.
        with mock_dataserver.mock_db_trans(self.ds):
            content = find_object_with_ntiid(course)
            course_obj = ICourseInstance(content)
            user = User.get_user(user_id)
            history = component.queryMultiAdapter((course_obj, user),
                                                  IUsersCourseAssignmentHistory)
            submission = AssignmentSubmission(assignmentId=assignment1, parts=())
            pending = QAssignmentSubmissionPendingAssessment(assignmentId=assignment1,
                                                             parts=())
            item = UsersCourseAssignmentHistoryItem(Submission=submission,
                                                    pendingAssessment=pending)
            history._setitemf(assignment1, item)

        response = self._get_progress(response=response)

        result = response.json_body['Items']
        assert_that(result, has_length(4))
        assert_that(result,
                    contains_inanyorder(video1, video2, resource1, assignment1))

        resource_progress = result.get(assignment1)
        assert_that(resource_progress, has_entry('MaxPossibleProgress', none()))
        assert_that(resource_progress, has_entry('AbsoluteProgress', none()))
        assert_that(resource_progress, has_entry('HasProgress', True))

        # Now a 304 again
        self._get_progress(response=response, status=304)


def _tx_string(s):
    if s and isinstance(s, six.text_type):
        s = s.encode('utf-8')
    return s


class TestUserLocationView(_AbstractTestViews):

    default_origin = 'http://janux.ou.edu'

    def setUp(self):
        super(TestUserLocationView, self).setUp()
        self.params = {}

    def _store_locations(self, *locations):
        with mock_dataserver.mock_db_trans(self.ds):
            for location in locations:
                self.db.session.add(location)

    def set_up_test_locations(self):
        # Create test locations; validate unicode.
        location1 = Location(latitude='10.0000',
                             longitude='10.0000',
                             city=u'Zürich åß∂∆˚≈ç√ñ≤œ∑ø',
                             state='',
                             country='Switzerland')

        # Native spelling of Shanghai
        location2 = Location(latitude='11.0000',
                             longitude='11.0000',
                             city=u'\u4e0a\u6d77\u5e02',
                             state='',
                             country='China')

        location3 = Location(latitude='12.0000',
                             longitude='12.0000',
                             city='Running out of city names',
                             state='Oklahoma',
                             country='United States of America')

        self._store_locations(location1, location2, location3)
        location_results = self.db.session.query(Location).all()
        assert_that(location_results, has_length(3))

    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    @fudge.patch('nti.analytics.database.locations._get_enrolled_user_ids')
    def test_location_json(self, mock_get_enrollment_list):

        # No one is enrolled in the course yet
        mock_get_enrollment_list.is_callable().returns([])

        # Initialize location view and fake course
        course = ContentCourseSubInstance()
        course.SharingScopes['Public'] = CourseInstanceSharingScope('Public')
        # TODO: Self? Shouldn't we just use web query?
        location_view = UserLocationJsonView(self)
        location_view.context = course

        # Starting out, no results should be returned
        result = location_view()
        assert_that(result, has_length(0))

        self.set_up_test_locations()

        # There should still be nothing returned, since no users are currently
        # enrolled
        result = location_view()
        assert_that(result, has_length(0))

        # Add an IP address for a user enrolled in the course
        ip_address_1 = IpGeoLocation(user_id=1,
                                     ip_addr='1.1.1.1',
                                     country_code='US',
                                     location_id=1)
        self._store_locations(ip_address_1)

        # Now let user 1 be enrolled in the course
        mock_get_enrollment_list.is_callable().returns([1])

        # We should get one result now
        result = location_view()
        assert_that(result, has_length(1))
        assert_that(result[0], has_entry('number_of_students', 1))
        assert_that(result[0], has_entry('latitude', 10.0))
        assert_that(result[0], has_entry('longitude', 10.0))

        # Add another IP address for the same user
        ip_address_2 = IpGeoLocation(user_id=1,
                                     ip_addr='1.1.1.2',
                                     country_code='US',
                                     location_id=2)
        self._store_locations(ip_address_2)

        # We should get back two locations with 1 user in each
        result = location_view()
        assert_that(result, has_length(2))
        assert_that(result, has_item(has_entries('latitude', 10.0,
                                                 'longitude', 10.0,
                                                 'number_of_students', 1)))
        assert_that(result, has_item(has_entries('latitude', 11.0,
                                                 'longitude', 11.0,
                                                 'number_of_students', 1)))

        # Add another user in the first location
        ip_address_3 = IpGeoLocation(user_id=2,
                                     ip_addr='1.1.1.3',
                                     country_code='US',
                                     location_id=1)
        self._store_locations(ip_address_3)
        mock_get_enrollment_list.is_callable().returns([1, 2])

        # Now we get back 2 locations, 1 of which has two users
        result = location_view()
        assert_that(result, has_length(2))
        assert_that(result, has_item(has_entries('latitude', 10.0,
                                                 'longitude', 10.0,
                                                 'number_of_students', 2)))
        assert_that(result, has_item(has_entries('latitude', 11.0,
                                                 'longitude', 11.0,
                                                 'number_of_students', 1)))

        # The second user has another ip address in a location not shared by
        # the first
        ip_address_4 = IpGeoLocation(user_id=2,
                                     ip_addr='1.1.1.4',
                                     country_code='US',
                                     location_id=3)
        self._store_locations(ip_address_4)

        # Now we get back 3 locations, one of which has two users.
        # The other two locations should only have one user each.
        result = location_view()
        assert_that(result, has_length(3))
        assert_that(result, has_item(has_entries('latitude', 10.0,
                                                 'longitude', 10.0,
                                                 'number_of_students', 2)))
        assert_that(result, has_item(has_entries('latitude', 11.0,
                                                 'longitude', 11.0,
                                                 'number_of_students', 1)))
        assert_that(result, has_item(has_entries('latitude', 12.0,
                                                 'longitude', 12.0,
                                                 'number_of_students', 1)))

    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    @fudge.patch('nti.analytics.database.locations._get_enrolled_user_ids')
    def test_location_html(self, mock_get_enrollment_list):

        location_link_path = '/dataserver2/users/CLC3403.ou.nextthought.com/LegacyCourses/CLC3403/%s' % GEO_LOCATION_VIEW

        # No one is enrolled in the course yet
        mock_get_enrollment_list.is_callable().returns([])

        # Initialize location view and fake course
        course = ContentCourseSubInstance()
        course.SharingScopes['Public'] = CourseInstanceSharingScope('Public')
        location_view = UserLocationJsonView(self)
        location_view.context = course
        self.set_up_test_locations()
        instructor_environ = self._make_extra_environ(user='harp4162')

        def fetch_html(status=200):
            return self.testapp.get(location_link_path, extra_environ=instructor_environ,
                                    status=status, headers={'accept': str('text/html')})

        # With no students in the course, we expect a 422 to be returned.
        # Anything else, and this will throw an exception.
        fetch_html(status=422)

        # Add an IP address for a user enrolled in the course
        ip_address_1 = IpGeoLocation(user_id=1,
                                     ip_addr='1.1.1.1',
                                     country_code='US',
                                     location_id=1)
        self._store_locations(ip_address_1)

        # Now let user 1 be enrolled in the course
        mock_get_enrollment_list.is_callable().returns([1])

        result = fetch_html()
        # Check the result against json from the other view,
        # which is tested above. We have to do some encoding
        # stuff to be able to find the string inside of the HTML response.
        location_json_result = location_view()[0]
        json_result = [location_json_result['latitude'],
                       location_json_result['longitude'],
                       _tx_string(location_json_result['label'])]

        # The html output should contain the same location data as in the json
        # result.
        assert_that(str(result.html), contains_string(str(json_result)))

        ip_address_2 = IpGeoLocation(user_id=1,
                                     ip_addr='1.1.1.2',
                                     country_code='US',
                                     location_id=2)
        self._store_locations(ip_address_2)

        # We should get back two locations with 1 user in each
        result = fetch_html()
        location_json_result = location_view()
        json_result = [[str('Lat'), str('Long'), str('Label')]]
        for view in location_json_result:
            json_result.append([view['latitude'],
                                view['longitude'],
                                _tx_string(view['label'])])
        assert_that(str(result.html), contains_string(str(json_result)))

        # Add another user in the first location
        ip_address_3 = IpGeoLocation(user_id=2,
                                     ip_addr='1.1.1.3',
                                     country_code='US',
                                     location_id=1)
        self._store_locations(ip_address_3)
        mock_get_enrollment_list.is_callable().returns([1, 2])

        # Now we get back 2 locations, 1 of which has two users
        result = fetch_html()

        location_json_result = location_view()
        json_result = [[str('Lat'), str('Long'), str('Label')]]
        for view in location_json_result:
            json_result.append([view['latitude'],
                                view['longitude'],
                                _tx_string(view['label'])])
        assert_that(str(result.html), contains_string(str(json_result)))

        # The second user has another ip address in a location not shared by
        # the first
        ip_address_4 = IpGeoLocation(user_id=2,
                                     ip_addr='1.1.1.4',
                                     country_code='US',
                                     location_id=3)
        self._store_locations(ip_address_4)

        # Now we get back 3 locations, one of which has two users.
        # The other two locations should only have one user each.
        result = fetch_html()
        location_json_result = location_view()
        json_result = [[str('Lat'), str('Long'), str('Label')]]
        for view in location_json_result:
            json_result.append([view['latitude'],
                                view['longitude'],
                                _tx_string(view['label'])])
        assert_that(str(result.html), contains_string(str(json_result)))

    @WithSharedApplicationMockDS(users=True, testapp=True, default_authenticate=True)
    @fudge.patch('nti.analytics.database.locations._get_enrolled_user_ids')
    def test_location_csv(self, mock_get_enrollment_list):

        location_link_path = '/dataserver2/users/CLC3403.ou.nextthought.com/LegacyCourses/CLC3403/%s' % GEO_LOCATION_VIEW

        # No one is enrolled in the course yet
        mock_get_enrollment_list.is_callable().returns([])

        # Initialize location view and fake course
        course = ContentCourseSubInstance()
        course.SharingScopes['Public'] = CourseInstanceSharingScope('Public')
        json_view = UserLocationJsonView(self)
        json_view.context = course
        self.set_up_test_locations()
        instructor_environ = self._make_extra_environ(user='harp4162')

        def fetch_csv(status=200):
            return self.testapp.get(location_link_path, extra_environ=instructor_environ,
                                    status=status, headers={'accept': str('text/csv')})

        # With no students in the course, we expect a 422 to be returned.
        # Anything else, and this will throw an exception.
        fetch_csv(status=422)

        # Add an IP address for a user enrolled in the course
        ip_address_1 = IpGeoLocation(user_id=1,
                                     ip_addr='1.1.1.1',
                                     country_code='US',
                                     location_id=1)
        self._store_locations(ip_address_1)

        # Now let user 1 be enrolled in the course
        mock_get_enrollment_list.is_callable().returns([1])

        def convert_to_utf8(data):
            for key, value in list(data.items()):  # mutating
                data[key] = _tx_string(value)
            return data

        fieldnames = ['number_of_students', 'city', 'state',
                      'country', 'latitude', 'longitude']

        def get_csv_string(data):
            predicted_result = []
            for field in fieldnames[:-1]:
                value = str(data[field])
                predicted_result.extend((value, ','))
            predicted_result.append(str(data[fieldnames[-1]]))
            return ''.join(predicted_result)

        result = fetch_csv()
        predicted_result = get_csv_string(convert_to_utf8(json_view.get_data(self)[0]))
        assert_that(result.body, contains_string(predicted_result))

        # add a second user in the same location
        ip_address_2 = IpGeoLocation(user_id=1,
                                     ip_addr='1.1.1.2',
                                     country_code='US',
                                     location_id=2)
        self._store_locations(ip_address_2)

        # Same thing, except we have two users in the same location.
        result = fetch_csv()
        predicted_result = get_csv_string(convert_to_utf8(json_view.get_data(self)[0]))
        assert_that(result.body, contains_string(predicted_result))

        # Add another user in the first location
        ip_address_3 = IpGeoLocation(user_id=2,
                                     ip_addr='1.1.1.3',
                                     country_code='US',
                                     location_id=1)
        self._store_locations(ip_address_3)
        mock_get_enrollment_list.is_callable().returns([1, 2])

        # Now we get back 2 locations, 1 of which has two users
        result = fetch_csv()
        json_data = json_view.get_data(self)
        first_location = get_csv_string(convert_to_utf8(json_data[0]))
        assert_that(result.body, contains_string(first_location))
        second_location = get_csv_string(convert_to_utf8(json_data[1]))
        assert_that(result.body, contains_string(second_location))

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_most_recent_session(self):
        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            self._create_user(u'new_user1',
                              external_value={'realname': u'Billy Bob',
                                              'email': u'foo@bar.com'})
            user2 = self._create_user(u'new_user2',
                                      external_value={'realname': u'Billy Rob',
                                                      'email': u'foo@bar.com'})

        ip_address_1 = IpGeoLocation(user_id=2,
                                     ip_addr='1.1.1.1',
                                     country_code='US',
                                     location_id=1)
        self._store_locations(ip_address_1)
        self.set_up_test_locations()

        href = '/dataserver2/ResolveUser/new_user2'
        res = self.testapp.get(href)
        res = res.json_body
        user = res['Items'][0]
        assert_that(user, not_none())

        # As an admin fetching we should see the property, but we have no
        # sessions currently
        assert_that(user, has_entry('MostRecentSession', none()))
        # We also should have a HistoricalSessions link
        self.require_link_href_with_rel(user, 'HistoricalSessions')

        # As ourselves we can see the property
        res = self.testapp.get(href, status=200,
                               extra_environ=self._make_extra_environ(username='new_user2'))
        res = res.json_body

        user = res['Items'][0]
        assert_that(user, not_none())
        assert_that(user, has_entry('MostRecentSession', none()))

        # But as another user we cannot
        res = self.testapp.get(href, status=200,
                               extra_environ=self._make_extra_environ(username='new_user1'))
        res = res.json_body

        user = res['Items'][0]
        assert_that(user, not_none())
        assert_that(user, does_not(have_key('MostRecentSession')))

        # Now simulate a couple of sessions from new_user2
        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            start = datetime.utcnow() - timedelta(days=1)
            start2 = start + timedelta(seconds=30)
            duration = 3600
            end = start2 + timedelta(seconds=duration)

            # Test start = end
            _add_session(user2.username, '', '1.1.1.1',
                         start_time=start, end_time=start)
            # 30 seconds later, a longer session
            _add_session(user2.username, '', '1.1.1.1',
                          start_time=start2, end_time=end)
            # and a session a long time ago
            longago = start - timedelta(days=60)
            sesh3 = _add_session(user2.username, '', '1.1.1.1',
                                 start_time=longago, end_time=longago + timedelta(hours=1))

        res = self.testapp.get(href, status=200,
                               extra_environ=self._make_extra_environ(username='new_user2'))
        res = res.json_body

        user = res['Items'][0]
        assert_that(user, not_none())
        # We now proxy the most recent session (by session id) as the most
        # recent session
        assert_that(user, has_entry('MostRecentSession',
                                    has_entries('SessionID', sesh3.SessionID,
                                                'Username', 'new_user2',
                                                'UserAgent', not_none(),
                                                'GeographicalLocation', not_none())))

        notBefore = time.mktime((start - timedelta(days=30)).timetuple())
        notAfter = time.mktime(start.timetuple())

        href = self.require_link_href_with_rel(user, 'HistoricalSessions')
        res = self.testapp.get(href,
                               {'notBefore': notBefore,
                               'notAfter': notAfter})
        res = res.json_body
        assert_that(res['Items'], has_length(2))

        # This can also be fetched by yourself
        self.testapp.get(href, status=200,
                         extra_environ=self._make_extra_environ(username='new_user2'))

        # But not by others
        self.testapp.get(href, status=403,
                         extra_environ=self._make_extra_environ(username='new_user1'))

        # We can also fetch with notAfter and a limit
        href = self.require_link_href_with_rel(user, 'HistoricalSessions')
        res = self.testapp.get(href,
                               {'limit': 2,
                               'notAfter': notAfter})
        res = res.json_body
        assert_that(res['Items'], has_length(2))


class TestAnalyticsContexts(_AbstractTestViews):

    default_origin = 'http://platform.ou.edu'

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_user_context(self):
        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            self._create_user(u'new_user1',
                              external_value={'realname': u'Billy Bob',
                                              'email': u'foo@bar.com'})
            self._create_user(u'new_user2',
                              external_value={'realname': u'Billy Rob',
                                              'email': u'foo@bar.com'})

        href = '/dataserver2/ResolveUser/new_user2'
        res = self.testapp.get(href)
        res = res.json_body
        user = res['Items'][0]
        href = self.require_link_href_with_rel(user, 'analytics')

        assert_that(href, ends_with('analytics'))

        # It is fetchable
        analytics_workspace = self.testapp.get(href)
        analytics_workspace = analytics_workspace.json_body
        assert_that(analytics_workspace, not_none())

        assert_that(analytics_workspace, has_entry('href', href))

        # We have two collections that are also routed beneath href
        assert_that(analytics_workspace,
                    has_entry('Items',
                              contains_inanyorder(has_entry('href', href+'/batch_events'),
                                                  has_entry('href', href+'/sessions'))))

        # Find one of the links we expect and make sure that the rendered link
        # is correctly traversable
        activity_by_date_summary = self.require_link_href_with_rel(analytics_workspace,
                                                                   ACTIVITY_SUMMARY_BY_DATE)
        self.testapp.get(activity_by_date_summary)


    def _create_course(self):
        content_unit = find_object_with_ntiid(course)
        course_obj = self.course = ICourseInstance(content_unit)
        get_root_context_id(self.db, course_obj, create=True)

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_course_context(self):
        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            self._create_course()
            ntiid = to_external_ntiid_oid(self.course)

        href = '/dataserver2/Objects/'+ntiid
        res = self.testapp.get(href)
        res = res.json_body
        href = self.require_link_href_with_rel(res, 'analytics')

        assert_that(href, ends_with('analytics'))

        # It is fetchable
        analytics_workspace = self.testapp.get(href)
        analytics_workspace = analytics_workspace.json_body
        assert_that(analytics_workspace, not_none())

        assert_that(analytics_workspace, has_entry('href', href))

        # We have two collections that are also routed beneath href
        assert_that(analytics_workspace,
                    has_entry('Items',
                              contains_inanyorder(has_entry('href', href+'/batch_events'),
                                                  has_entry('href', href+'/sessions'))))

        # Find one of the links we expect and make sure that the rendered link
        # is correctly traversable
        activity_by_date_summary = self.require_link_href_with_rel(analytics_workspace,
                                                                   ACTIVITY_SUMMARY_BY_DATE)
        self.testapp.get(activity_by_date_summary)

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_enrollment_record_context(self):
        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            self._create_course()

            user2 = self._create_user(u'new_user2',
                                      external_value={'realname': u'Billy Rob',
                                                      'email': u'foo@bar.com'})

            enrollment_manager = ICourseEnrollmentManager(self.course)
            er = enrollment_manager.enroll(user2)
            er = enrollment_from_record(None, er)
            er_oid = er.ntiid
            link = Link(er, elements=('analytics',))
            interface.alsoProvides(link, ILinkExternalHrefOnly)
            href = render_link(link)

        assert_that(href, ends_with('analytics'))

        # It is fetchable
        analytics_workspace = self.testapp.get(href)
        analytics_workspace = analytics_workspace.json_body
        assert_that(analytics_workspace, not_none())

        assert_that(analytics_workspace, has_entry('href', ends_with('analytics')))

        # We have two collections that are also routed beneath href
        assert_that(analytics_workspace,
                    has_entry('Items',
                              contains_inanyorder(has_entry('href', ends_with('/batch_events')),
                                                  has_entry('href', ends_with('/sessions')))))

        # Find one of the links we expect and make sure that the rendered link
        # is correctly traversable
        activity_by_date_summary = self.require_link_href_with_rel(analytics_workspace,
                                                                   ACTIVITY_SUMMARY_BY_DATE)
        self.testapp.get(activity_by_date_summary)

        oid_object = self.testapp.get('/dataserver2/Objects/'+er_oid)
        oid_object = oid_object.json_body
        # we also have an analytics link if we fetch the enrollment record by oid
        from_oid_href = self.require_link_href_with_rel(oid_object, 'analytics')
        assert_that(from_oid_href, is_(href))


class TestUserAnalyticsWorkspace(ApplicationLayerTest):

    default_origin = 'http://platform.ou.edu'

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_analytics_workspace_link(self):
        with mock_dataserver.mock_db_trans(self.ds, site_name='platform.ou.edu'):
            self._create_user(u'new_user1',
                              external_value={'realname': u'Billy Bob',
                                              'email': u'foo@bar.com'})
            self._create_user(u'new_user2',
                              external_value={'realname': u'Billy Rob',
                                              'email': u'foo@bar.com'})

        href = '/dataserver2/ResolveUser/new_user2'
        res = self.testapp.get(href)
        res = res.json_body

        user = res['Items'][0]
        assert_that(user, not_none())

        # As an admin we should have a analytics link
        self.require_link_href_with_rel(user, 'analytics')

    @WithSharedApplicationMockDS(testapp=True, users=True)
    def test_stats_caching(self):

        workspace_href = '/dataserver2/analytics'
        workspace = self.testapp.get(workspace_href)
        workspace = workspace.json_body

        href = self.require_link_href_with_rel(workspace,
                                               ACTIVITY_SUMMARY_BY_DATE)
        resp = self.testapp.get(href)

        assert_that(resp.cache_control.max_age, greater_than(0))
        assert_that(resp.cache_control.must_revalidate, is_(False))

        href = self.require_link_href_with_rel(workspace, ACTIVE_TIMES_SUMMARY)
        resp = self.testapp.get(href)

        assert_that(resp.cache_control.max_age, greater_than(0))
        assert_that(resp.cache_control.must_revalidate, is_(False))


class TestBookViews(ApplicationLayerTest):

    layer = PersistentApplicationTestLayer

    default_origin = 'http://janux.ou.edu'

    bundle_ntiid = 'tag:nextthought.com,2011-10:NTI-Bundle-VisibleBundle'
    packageA = 'tag:nextthought.com,2011-10:NTI-HTML-PackageA'
    packageB = "tag:nextthought.com,2011-10:NTI-HTML-PackageB"

    def create_book_event(self, package_ntiid, time_length, username, environ, timestamp=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        timestamp = calendar.timegm(timestamp.timetuple())
        resource_event = ResourceEvent(user=username,
                                       timestamp=timestamp,
                                       RootContextID=self.bundle_ntiid,
                                       context_path=(package_ntiid,),
                                       ResourceId=package_ntiid,
                                       Duration=time_length)

        events = BatchResourceEvents(events=(resource_event,))
        ext_obj = toExternalObject(events)

        # Add a session header
        session_id = 9999
        headers = {ANALYTICS_SESSION_HEADER: str(session_id)}

        # Upload our events
        batch_url = '/dataserver2/analytics/batch_events'
        self.testapp.post_json(batch_url,
                               ext_obj,
                               headers=headers,
                               extra_environ=environ)


    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_book_views(self):
        with mock_dataserver.mock_db_trans(self.ds):
            self._create_user(username='test_book_view1')
            self._create_user(username='test_book_view2')
        user1 = self._make_extra_environ(user='test_book_view1')
        user2 = self._make_extra_environ(user='test_book_view2')

        href = '/dataserver2/Objects/%s' % self.bundle_ntiid
        res = self.testapp.get(href)
        res = res.json_body
        href = self.require_link_href_with_rel(res, 'analytics')
        assert_that(href, ends_with('analytics'))

        # It is fetchable
        analytics_workspace = self.testapp.get(href)
        analytics_workspace = analytics_workspace.json_body
        assert_that(analytics_workspace, not_none())

        assert_that(analytics_workspace, has_entry('href', href))

        # We have two collections that are also routed beneath href
        assert_that(analytics_workspace,
                    has_entry('Items',
                              contains_inanyorder(has_entry('href', href+'/batch_events'),
                                                  has_entry('href', href+'/sessions'))))

        # Find one of the links we expect and make sure that the rendered link
        # is correctly traversable
        activity_by_date_summary = self.require_link_href_with_rel(analytics_workspace,
                                                                   ACTIVITY_SUMMARY_BY_DATE)
        active_times_href = self.require_link_href_with_rel(analytics_workspace,
                                                            ACTIVE_TIMES_SUMMARY)
        active_users_href = self.require_link_href_with_rel(analytics_workspace,
                                                            ACTIVE_USERS)

        self.testapp.get(active_times_href)

        res = self.testapp.get(active_users_href)
        res = res.json_body
        assert_that(res['ItemCount'], is_(0))

        res = self.testapp.get(activity_by_date_summary)
        res = res.json_body
        assert_that(res['Dates'], has_length(0))

        event1_time = datetime.utcnow()
        self.create_book_event(self.packageA, 30, 'test_book_view1', user1)
        event2_time = datetime.utcnow() - timedelta(days=1)
        self.create_book_event(self.packageB, 60, 'test_book_view2', user2,
                               timestamp=event2_time)
        self.create_book_event(self.packageB, 90, 'test_book_view1', user1,
                               timestamp=event2_time)

        res = self.testapp.get(activity_by_date_summary)
        res = res.json_body
        assert_that(res, has_entry('Dates',
                                   has_entries(str(event2_time.date()), 2,
                                               str(event1_time.date()), 1)))
        self.testapp.get(active_times_href)

        res = self.testapp.get(active_users_href)
        res = res.json_body
        assert_that(res['ItemCount'], is_(2))
        items = res['Items']
        assert_that(items, has_length(2))
        usernames = [x[u'Username'] for x in items]
        assert_that(usernames, contains_inanyorder('test_book_view1',
                                                   'test_book_view2'))
