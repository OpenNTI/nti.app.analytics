#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""


.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from nti.app.testing.application_webtest import ApplicationTestLayer
import os
import os.path

import ZODB
from nti.dataserver.tests.mock_dataserver import WithMockDS
from nti.dataserver.tests.mock_dataserver import mock_db_trans

from nti.dataserver import users
from zope.component.interfaces import IComponents
from nti.contenttypes.courses.interfaces import ICourseCatalog
from nti.contentlibrary.interfaces import IContentPackageLibrary

def publish_ou_course_entries():
	lib = component.getUtility(IContentPackageLibrary)
	try:
		del lib.contentPackages
	except AttributeError:
		pass

	lib.syncContentPackages()


def _do_then_enumerate_library(do, sync_libs=False):

	database = ZODB.DB( ApplicationTestLayer._storage_base,
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
	def _setup_library( cls, *args, **kwargs ):
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
					'CLC3403_LawAndJustice')) )
		return lib

	@classmethod
	def setUp(cls):
		# Must implement!
		cls.__old_library = component.getUtility(IContentPackageLibrary)
		component.provideUtility(cls._setup_library(cls), IContentPackageLibrary)

		_do_then_enumerate_library(lambda: users.User.create_user( username='harp4162', password='temp001') )

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
			users.User.delete_user('harp4162')
			component.getGlobalSiteManager().getUtility(ICourseCatalog).clear()
			component.getUtility(IComponents,name='platform.ou.edu').getUtility(ICourseCatalog).clear()

		_do_then_enumerate_library(cleanup)
		del cls.__old_library



class RestrictedInstructedCourseApplicationTestLayer(ApplicationTestLayer):

	_library_path = 'RestrictedLibrary'

	@classmethod
	def setUp(cls):
		# Must implement!
		cls.__old_library = component.getUtility(IContentPackageLibrary)
		component.provideUtility(LegacyInstructedCourseApplicationTestLayer._setup_library(cls), IContentPackageLibrary)

		_do_then_enumerate_library(lambda: users.User.create_user( username='harp4162', password='temp001') )

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
			users.User.delete_user('harp4162')
			component.getGlobalSiteManager().getUtility(ICourseCatalog).clear()
			component.getUtility(IComponents,name='platform.ou.edu').getUtility(ICourseCatalog).clear()

		_do_then_enumerate_library(cleanup)
		del cls.__old_library


class PersistentInstructedCourseApplicationTestLayer(ApplicationTestLayer):
	# A mix of new and old-style courses

	_library_path = 'PersistentLibrary'

	@classmethod
	def setUp(cls):
		# Must implement!
		cls.__old_library = component.getUtility(IContentPackageLibrary)
		component.provideUtility(LegacyInstructedCourseApplicationTestLayer._setup_library(cls), IContentPackageLibrary)
		_do_then_enumerate_library(lambda: users.User.create_user( username='harp4162', password='temp001'),
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
			users.User.delete_user('harp4162')
			component.getGlobalSiteManager().getUtility(ICourseCatalog).clear()
			component.getUtility(IComponents,name='platform.ou.edu').getUtility(ICourseCatalog).clear()

			from nti.site.site import get_site_for_site_names
			site = get_site_for_site_names(('platform.ou.edu',))
			cc = site.getSiteManager().getUtility(ICourseCatalog)
			for x in list(cc):
				del cc[x]

		_do_then_enumerate_library(cleanup)
		del cls.__old_library

# Export the new-style stuff as default
InstructedCourseApplicationTestLayer = PersistentInstructedCourseApplicationTestLayer

# Override id lookup for testing purposes.
from nti.analytics import identifier
from nti.analytics.tests import TestIdentifier
identifier._DSIdentifier.get_id = identifier._NtiidIdentifier.get_id \
= identifier.SessionId.get_id = TestIdentifier().get_id
