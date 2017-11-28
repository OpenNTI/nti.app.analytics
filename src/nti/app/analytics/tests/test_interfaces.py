#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import assert_that

import unittest

from zope.dottedname import resolve as dottedname

from nti.analytics.tests import NTIAnalyticsTestCase

from nti.app.products.courseware.workspaces import enrollment_from_record

from nti.contenttypes.courses.courses import CourseInstance
from nti.contenttypes.courses.enrollment import DefaultCourseInstanceEnrollmentRecord

from nti.dataserver.users import User

from nti.testing.matchers import verifiably_provides

from ..interfaces import IAnalyticsContext


class TestInterfaces(unittest.TestCase):

    def test_import_interfaces(self):
        dottedname.resolve('nti.app.analytics.interfaces')


class TestAnalyticsContext( NTIAnalyticsTestCase ):
    """
    Tests that things that should be analytics context aware, are.
    """

    def test_user_is_context(self):
        user = User('testuser')
        assert_that(user, verifiably_provides(IAnalyticsContext))

    def test_course_is_context(self):
        course = CourseInstance()
        assert_that(course, verifiably_provides(IAnalyticsContext))

    def test_enrollment_record_is_context(self):
        enrollment = enrollment_from_record(None, DefaultCourseInstanceEnrollmentRecord())
        assert_that(enrollment, verifiably_provides(IAnalyticsContext))

