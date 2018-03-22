#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import contains
from hamcrest import not_none
from hamcrest import has_entry
from hamcrest import assert_that
from hamcrest import has_property

from nti.analytics.tests import NTIAnalyticsApplicationTestLayer

from nti.app.analytics.decorators import _CourseVideoProgressLinkDecorator

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.contenttypes.courses.courses import CourseInstance


class TestDecorators(ApplicationLayerTest):

    layer = NTIAnalyticsApplicationTestLayer

    def test_course_decorator(self):
        inst = CourseInstance()
        result = {}
        decorator = _CourseVideoProgressLinkDecorator(object(), None)
        decorator._do_decorate_external(inst, result)

        assert_that(result, not_none())
        assert_that(result, has_entry('Links',
                                      contains(has_property('rel', 'VideoProgress'))))
