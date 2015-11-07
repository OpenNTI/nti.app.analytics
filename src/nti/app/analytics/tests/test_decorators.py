#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import contains
from hamcrest import not_none
from hamcrest import has_entry
from hamcrest import assert_that
from hamcrest import has_property

from datetime import datetime

from nti.contenttypes.courses.courses import CourseInstance
from nti.contenttypes.courses.outlines import CourseOutlineNode
from nti.contenttypes.courses.outlines import CourseOutlineContentNode

from nti.app.analytics.decorators import _CourseVideoProgressLinkDecorator
from nti.app.analytics.decorators import _CourseOutlineNodeProgressLinkDecorator

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.analytics.tests import NTIAnalyticsApplicationTestLayer

class TestDecorators(ApplicationLayerTest):

	layer = NTIAnalyticsApplicationTestLayer

	def test_node_decorator(self):
		inst = CourseInstance()
		outline = inst.Outline
		node = CourseOutlineNode()
		outline.append(node)
		node2 = CourseOutlineContentNode(ContentNTIID='tag:nextthought.com,2011-10:OU-HTML-CLC3403_LawAndJustice.lec:01_LESSON',
										 AvailableBeginning=datetime.now())
		result = {}
		decorator = _CourseOutlineNodeProgressLinkDecorator(object(), None)
		decorator._do_decorate_external(node2, result)

		assert_that(result, not_none())
		assert_that(result, has_entry('Links',
								contains(has_property('rel', 'Progress'))))

	def test_course_decorator(self):
		inst = CourseInstance()
		result = {}
		decorator = _CourseVideoProgressLinkDecorator(object(), None)
		decorator._do_decorate_external(inst, result)

		assert_that(result, not_none())
		assert_that(result, has_entry('Links',
									  contains(has_property('rel', 'VideoProgress'))))

