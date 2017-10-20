#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

import os
import shutil
import tempfile
import unittest

import ZODB

import zope.testing.cleanup

from zope import component

from zope.component.interfaces import IComponents

from nti.dataserver.users.users import User

from nti.dataserver.tests.mock_dataserver import WithMockDS
from nti.dataserver.tests.mock_dataserver import mock_db_trans
from nti.dataserver.tests.mock_dataserver import DSInjectorMixin

from nti.analytics.database.tests import MockParent

from nti.contenttypes.courses.interfaces import ICourseCatalog
from nti.contentlibrary.interfaces import IContentPackageLibrary

from nti.testing.layers import find_test
from nti.testing.layers import GCLayerMixin
from nti.testing.layers import ZopeComponentLayer
from nti.testing.layers import ConfiguringLayerMixin

from nti.app.testing.application_webtest import ApplicationTestLayer


def publish_ou_course_entries():
    lib = component.getUtility(IContentPackageLibrary)
    try:
        del lib.contentPackages
    except AttributeError:
        pass
    lib.syncContentPackages()


def _do_then_enumerate_library(do, sync_libs=False):

    database = ZODB.DB(ApplicationTestLayer._storage_base,
                       database_name='Users')

    @WithMockDS(database=database)
    def _create():
        with mock_db_trans():
            do()
            publish_ou_course_entries()
            if sync_libs:
                from nti.app.contentlibrary.admin_views import _SyncAllLibrariesView
                _SyncAllLibrariesView(None)()

    _create()


class LegacyInstructedCourseApplicationTestLayer(ApplicationTestLayer):

    _library_path = 'Library'

    @staticmethod
    def _setup_library(cls, *args, **kwargs):
        from nti.contentlibrary.filesystem import CachedNotifyingStaticFilesystemLibrary as Library
        lib = Library(
            paths=(
                os.path.join(
                    os.path.dirname(__file__),
                    cls._library_path,
                    'IntroWater'),
                os.path.join(
                    os.path.dirname(__file__),
                    cls._library_path,
                    'CLC3403_LawAndJustice')))
        return lib

    @classmethod
    def setUp(cls):
        # Must implement!
        cls.__old_library = component.getUtility(IContentPackageLibrary)
        component.provideUtility(cls._setup_library(cls),
                                 IContentPackageLibrary)

        _do_then_enumerate_library(lambda: User.create_user(username=u'harp4162', 
                                                            password=u'temp001'))

    @classmethod
    def tearDown(cls):
        # Must implement!
        # Clean up any side effects of these content packages being
        # registered
        def cleanup():
            del component.getUtility(IContentPackageLibrary).contentPackages
            try:
                del cls.__old_library.contentPackages
            except AttributeError:
                pass
            component.provideUtility(cls.__old_library, IContentPackageLibrary)
            User.delete_user('harp4162')
            component.getGlobalSiteManager().getUtility(ICourseCatalog).clear()
            
            components = component.getUtility(IComponents, name='platform.ou.edu')
            components.getUtility(ICourseCatalog).clear()

        _do_then_enumerate_library(cleanup)
        del cls.__old_library


class RestrictedInstructedCourseApplicationTestLayer(ApplicationTestLayer):

    _library_path = 'RestrictedLibrary'

    @classmethod
    def setUp(cls):
        # Must implement!
        cls.__old_library = component.getUtility(IContentPackageLibrary)
        component.provideUtility(LegacyInstructedCourseApplicationTestLayer._setup_library(cls), 
                                 IContentPackageLibrary)

        _do_then_enumerate_library(lambda: User.create_user(username=u'harp4162', 
                                                            password=u'temp001'))

    @classmethod
    def tearDown(cls):
        # Must implement!
        # Clean up any side effects of these content packages being
        # registered
        def cleanup():
            del component.getUtility(IContentPackageLibrary).contentPackages
            try:
                del cls.__old_library.contentPackages
            except AttributeError:
                pass
            component.provideUtility(cls.__old_library, IContentPackageLibrary)
            User.delete_user('harp4162')
            component.getGlobalSiteManager().getUtility(ICourseCatalog).clear()
            components = component.getUtility(IComponents, name='platform.ou.edu')
            components.getUtility(ICourseCatalog).clear()

        _do_then_enumerate_library(cleanup)
        del cls.__old_library


class PersistentInstructedCourseApplicationTestLayer(ApplicationTestLayer):
    # A mix of new and old-style courses

    _library_path = 'PersistentLibrary'

    @classmethod
    def setUp(cls):
        # Must implement!
        cls.__old_library = component.getUtility(IContentPackageLibrary)
        component.provideUtility(LegacyInstructedCourseApplicationTestLayer._setup_library(
            cls), IContentPackageLibrary)
        _do_then_enumerate_library(lambda: User.create_user(username='harp4162', password='temp001'),
                                   sync_libs=True)

    @classmethod
    def tearDown(cls):
        # Must implement!
        # Clean up any side effects of these content packages being
        # registered
        def cleanup():
            del component.getUtility(IContentPackageLibrary).contentPackages
            try:
                del cls.__old_library.contentPackages
            except AttributeError:
                pass
            component.provideUtility(cls.__old_library, IContentPackageLibrary)
            User.delete_user('harp4162')
            component.getGlobalSiteManager().getUtility(ICourseCatalog).clear()
            components = component.getUtility(IComponents, name='platform.ou.edu')
            components.getUtility(ICourseCatalog).clear()

            from nti.site.site import get_site_for_site_names
            site = get_site_for_site_names(('platform.ou.edu',))
            cc = site.getSiteManager().getUtility(ICourseCatalog)
            for x in list(cc):
                del cc[x]

        _do_then_enumerate_library(cleanup)
        del cls.__old_library

# Export the new-style stuff as default
InstructedCourseApplicationTestLayer = PersistentInstructedCourseApplicationTestLayer


class SharedConfiguringTestLayer(ZopeComponentLayer,
                                 GCLayerMixin,
                                 ConfiguringLayerMixin,
                                 DSInjectorMixin):

    set_up_packages = ('nti.dataserver', 'nti.analytics', 'nti.app.analytics')

    @classmethod
    def setUp(cls):
        cls.setUpPackages()
        cls.old_data_dir = os.getenv('DATASERVER_DATA_DIR')
        cls.new_data_dir = tempfile.mkdtemp(dir="/tmp")
        os.environ['DATASERVER_DATA_DIR'] = cls.new_data_dir

    @classmethod
    def tearDown(cls):
        cls.tearDownPackages()
        zope.testing.cleanup.cleanUp()

    @classmethod
    def testSetUp(cls, test=None):
        cls.setUpTestDS(test)
        shutil.rmtree(cls.new_data_dir, True)
        os.environ['DATASERVER_DATA_DIR'] = cls.old_data_dir or '/tmp'

    @classmethod
    def testTearDown(cls):
        pass


class NTIAnalyticsTestCase(unittest.TestCase):
    layer = SharedConfiguringTestLayer
