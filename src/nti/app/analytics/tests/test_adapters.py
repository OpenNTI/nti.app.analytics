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

import time

import fudge

from zope import component

import zope.intid

from nti.app.products.courseware.interfaces import IViewStats

from nti.app.analytics.tests import NTIAnalyticsTestCase

from nti.analytics.database import get_analytics_db
from nti.analytics.database import boards as db_boards_view
from nti.analytics.database import lti as db_lti_views
from nti.analytics.database import resource_tags as db_tags_view
from nti.analytics.database import resource_views as db_resource_views

from nti.analytics.sessions import get_nti_session_id

from nti.analytics.tests import TestIdentifier

from nti.analytics_database.interfaces import IAnalyticsIntidIdentifier
from nti.analytics_database.interfaces import IAnalyticsNTIIDIdentifier
from nti.analytics_database.interfaces import IAnalyticsRootContextIdentifier

from nti.app.analytics.completion import content_progress
from nti.app.analytics.completion import lti_external_tool_asset_progress

from nti.app.products.courseware_ims.lti import LTIExternalToolAsset

from nti.contentlibrary.contentunit import ContentUnit

from nti.contenttypes.courses.courses import CourseInstance

from nti.dataserver.contenttypes.forums.forum import GeneralForum

from nti.dataserver.contenttypes.forums.post import GeneralForumComment

from nti.dataserver.contenttypes.forums.topic import GeneralTopic as Topic

from nti.dataserver.contenttypes.note import Note

from nti.dataserver.tests import mock_dataserver

from nti.dataserver.tests.mock_dataserver import WithMockDSTrans

from nti.dataserver.users.users import User

from nti.testing.time import time_monotonically_increases


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


class TestPagedProgress( NTIAnalyticsTestCase ):

    def _create_resource_view(self, user, resource_val, course):
        time_length = 30
        event_time = time.time()
        db_resource_views.create_course_resource_view( user,
                                                       None, event_time,
                                                       course, None,
                                                       resource_val, time_length )

    @WithMockDSTrans
    @fudge.patch( 'nti.ntiids.ntiids.find_object_with_ntiid' )
    def test_paged_progress(self, mock_find_object):
        user = User.create_user( username='new_user1', dataserver=self.ds )
        course = CourseInstance()

        container = ContentUnit()
        container.NTIID = container.ntiid = u'tag:nextthought.com,2011:bleh'
        mock_find_object.is_callable().returns( container )

        # No children
        result = content_progress(user, container, course)
        assert_that( result, none() )

        # One child with no views
        child1 = ContentUnit()
        child1.ntiid = child_ntiid = u'tag:nextthought.com,2011:bleh.page_1'
        container.children = children = (child1,)
        # Max progress is different, currently.  Since the container
        # counts towards progress.  This may change.
        max_progress = len( children ) + 1

        mock_find_object.is_callable().returns( container )
        result = content_progress(user, container, course)
        assert_that( result, none() )

        # Child with view
        self._create_resource_view( user, child_ntiid, course )

        mock_find_object.is_callable().returns( container )
        result = content_progress(user, container, course)
        assert_that( result, not_none() )
        assert_that( result.AbsoluteProgress, is_( 1 ))
        assert_that( result.MaxPossibleProgress, is_( max_progress ))

        # Multiple children
        child2 = ContentUnit()
        child3 = ContentUnit()
        child2.ntiid = child_ntiid2 = u'tag:nextthought.com,2011:bleh.page_2'
        child3.ntiid = u'tag:nextthought.com,2011:bleh.page_3'
        container.children = children = ( child1, child2, child3 )
        max_progress = len( children ) + 1

        mock_find_object.is_callable().returns( container )
        result = content_progress(user, container, course)
        assert_that( result, not_none() )
        assert_that( result.AbsoluteProgress, is_( 1 ))
        assert_that( result.MaxPossibleProgress, is_( max_progress ))

        # Original child again
        self._create_resource_view( user, child_ntiid, course )

        mock_find_object.is_callable().returns( container )
        result = content_progress(user, container, course)
        assert_that( result, not_none() )
        assert_that( result.AbsoluteProgress, is_( 1 ))
        assert_that( result.MaxPossibleProgress, is_( max_progress ))

        # Different child
        self._create_resource_view( user, child_ntiid2, course )

        mock_find_object.is_callable().returns( container )
        result = content_progress(user, container, course)
        assert_that( result, not_none() )
        assert_that( result.AbsoluteProgress, is_( 2 ))
        assert_that( result.MaxPossibleProgress, is_( max_progress ))
        assert_that( result.HasProgress, is_( True ))


class TestLTIProgress(NTIAnalyticsTestCase):

    @WithMockDSTrans
    @fudge.patch('nti.ntiids.ntiids.find_object_with_ntiid')
    def test_lti_progress(self, mock_find_object):

        user = User.create_user(username='test_user', dataserver=self.ds)
        course = CourseInstance()
        connection = mock_dataserver.current_transaction
        connection.add(course)
        asset = LTIExternalToolAsset()
        asset.ntiid = 'fake_ntiid'
        mock_find_object.is_callable().returns(asset)

        result = lti_external_tool_asset_progress(user, asset, course)
        assert_that(result, is_(0))

        db_lti_views.create_launch_record(user, course, asset, get_nti_session_id(), [course.ntiid], time.time())
        result = lti_external_tool_asset_progress(user, asset, course)
        assert_that(result, is_(1))