#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import csv
import time

from datetime import datetime
from datetime import timedelta

from io import BytesIO

from pyramid.view import view_config
from pyramid.view import view_defaults

from pyramid import httpexceptions as hexc

from zope import component
from zope import interface

from zope.container.contained import Contained

from zope.traversing.interfaces import IPathAdapter

from ZODB.POSException import POSError

from nti.analytics import get_factory
from nti.analytics import QUEUE_NAMES

from nti.analytics.assessments import get_self_assessments_for_course

from nti.analytics.boards import get_topic_views

from nti.analytics.interfaces import IUserResearchStatus

from nti.analytics.locations import update_missing_locations

from nti.analytics.resource_views import get_video_views_for_ntiid
from nti.analytics.resource_views import get_resource_views_for_ntiid

from nti.analytics.stats.utils import get_time_stats

from nti.app.analytics import ANALYTICS
from nti.app.analytics import VIEW_STATS

from nti.app.analytics.externalization import to_external_job

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.products.courseware.views import CourseAdminPathAdapter

from nti.common.maps import CaseInsensitiveDict

from nti.contentlibrary.interfaces import IContentUnit

from nti.contenttypes.courses.interfaces import ICourseCatalog
from nti.contenttypes.courses.interfaces import ICourseInstance
from nti.contenttypes.courses.interfaces import	ICourseCatalogEntry

from nti.contenttypes.presentation.interfaces import IAssetRef
from nti.contenttypes.presentation.interfaces import INTIVideo
from nti.contenttypes.presentation.interfaces import INTIVideoRef
from nti.contenttypes.presentation.interfaces import IPresentationAsset

from nti.dataserver.authorization import ACT_NTI_ADMIN

from nti.dataserver.contenttypes.forums.interfaces import IForum
from nti.dataserver.contenttypes.forums.interfaces import ITopic

from nti.dataserver.interfaces import IUser
from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout
from nti.dataserver.interfaces import IDataserverFolder
from nti.dataserver.interfaces import IUsernameSubstitutionPolicy

from nti.dataserver.users import User

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.ntiids.ntiids import find_object_with_ntiid

CLASS = StandardExternalFields.CLASS
ITEMS = StandardExternalFields.ITEMS
TOTAL = StandardExternalFields.TOTAL
LAST_MODIFIED = StandardExternalFields.LAST_MODIFIED

@interface.implementer(IPathAdapter)
class AnalyticsPathAdapter(Contained):

	__name__ = ANALYTICS

	def __init__(self, context, request):
		self.context = context
		self.request = request
		self.__parent__ = context

@view_config(route_name='objects.generic.traversal',
			 name='queue_info',
			 renderer='rest',
			 request_method='GET',
			 permission=ACT_NTI_ADMIN,
			 context=AnalyticsPathAdapter)
def queue_info(request):
	"""
	Report on the analytics queue sizes.
	"""

	result = LocatedExternalDict()
	factory = get_factory()

	for name in QUEUE_NAMES:
		queue_info = LocatedExternalDict()
		result[ name ] = queue_info
		queue = factory.get_queue(name)
		queue_info['queue_length'] = len(queue)
		queue_info['failed_length'] = len(queue.get_failed_queue())

	return result

@view_config(route_name='objects.generic.traversal',
			 name='empty_queue',
			 renderer='rest',
			 request_method='POST',
			 permission=ACT_NTI_ADMIN,
			 context=AnalyticsPathAdapter)
def empty_queue(request):
	"""
	Empty the analytics job queues, including the fail queues.
	"""

	result = LocatedExternalDict()
	factory = get_factory()
	queue_names = QUEUE_NAMES
	now = time.time()

	for name in queue_names:
		queue = factory.get_queue(name)
		queue_count = len(queue)
		queue.empty()

		fail_queue = queue.get_failed_queue()
		failed_count = len(fail_queue)
		fail_queue.empty()

		if queue_count or failed_count:
			logger.info('Emptied analytics processing queue (%s) (count=%s) (fail_count=%s)',
						name, queue_count, failed_count)

		queue_stat = LocatedExternalDict()
		queue_stat[TOTAL] = queue_count
		queue_stat['Failed total'] = failed_count
		result[ name ] = queue_stat

	elapsed = time.time() - now
	logger.info('Emptied analytics processing queue (time=%s)', elapsed)
	return result

@view_config(route_name='objects.generic.traversal',
			 name='queue_jobs',
			 renderer='rest',
			 request_method='GET',
			 permission=ACT_NTI_ADMIN,
			 context=AnalyticsPathAdapter)
def queue_jobs(request):
	"""
	Report on the analytics jobs.
	"""

	factory = get_factory()
	result = LocatedExternalDict()
	items = result[ITEMS] = dict()
	for name in QUEUE_NAMES:
		queue = factory.get_queue(name)
		items[name] = [to_external_job(x) for x in queue.all() or ()]
	return result

@view_config(route_name='objects.generic.traversal',
			 name='user_research_stats',
			 renderer='rest',
			 request_method='GET',
			 permission=ACT_NTI_ADMIN,
			 context=AnalyticsPathAdapter)
class UserResearchStatsView(AbstractAuthenticatedView):

	def __call__(self):
		result = LocatedExternalDict()

		dataserver = component.getUtility(IDataserver)
		users_folder = IShardLayout(dataserver).users_folder

		allow_count = deny_count = neither_count = 0

		now = datetime.utcnow()
		year_ago = now - timedelta(days=365)

		# This is pretty slow.
		for user in list(users_folder.values()): # TODO: Index?
			if not IUser.providedBy(user):
				continue

			try:
				research_status = IUserResearchStatus(user)
			except POSError:
				continue

			last_mod = research_status.lastModified
			if last_mod is not None:
				# First, find the year+ older entries; they are promptable.
				if datetime.utcfromtimestamp(last_mod) < year_ago:
					neither_count += 1
					continue

			if research_status.allow_research:
				allow_count += 1
			else:
				deny_count += 1

		result['DenyResearchCount'] = deny_count
		result['AllowResearchCount'] = allow_count
		result['ToBePromptedCount'] = neither_count
		return result

# Assessments
def replace_username(username):
	policy = component.queryUtility(IUsernameSubstitutionPolicy)
	if policy is not None:
		return policy.replace(username) or username
	return username

def _parse_catalog_entry(params, names=('ntiid', 'entry', 'course')):
	ntiid = None
	for name in names:
		ntiid = params.get(name)
		if ntiid:
			break
	if not ntiid:
		return None

	context = find_object_with_ntiid(ntiid)
	result = ICourseCatalogEntry(context, None)
	if result is None:
		try:
			catalog = component.getUtility(ICourseCatalog)
			result = catalog.getCatalogEntry(ntiid)
		except KeyError:
			pass
	return result

@view_config(context=IDataserverFolder)
@view_config(context=AnalyticsPathAdapter)
@view_config(context=CourseAdminPathAdapter)
@view_defaults(route_name='objects.generic.traversal',
				renderer='rest',
				permission=ACT_NTI_ADMIN,
				request_method='GET',
				name='CourseAssessmentsTakenCounts')
class UserCourseAssessmentsTakenCountsView(AbstractAuthenticatedView):
	"""
	For a course, return a CSV with the self assessment counts
	for each user, by assessment.
	"""

	def __call__(self):
		params = CaseInsensitiveDict(self.request.params)
		context = _parse_catalog_entry(params)
		if context is None:
			raise hexc.HTTPUnprocessableEntity("Invalid course NTIID")
		course = ICourseInstance(context)

		response = self.request.response
		response.content_encoding = str('identity')
		response.content_type = str('text/csv; charset=UTF-8')
		filename = context.ProviderUniqueID + '_self_assessment.csv'
		response.content_disposition = str('attachment; filename="%s"' % filename)

		stream = BytesIO()
		writer = csv.writer(stream)
		course_header = [ context.ProviderUniqueID ]
		writer.writerow(course_header)

		user_assessment_dict = {}
		user_assessments = get_self_assessments_for_course(course)

		for user_assessment in user_assessments:
			assessment_dict = user_assessment_dict.setdefault(user_assessment.AssessmentId, {})
			username = user_assessment.user.username

			prev_val = assessment_dict.setdefault(username, 0)
			assessment_dict[username] = prev_val + 1

		for assessment_id, users_vals in user_assessment_dict.items():
			assessment_header = [ assessment_id ]
			writer.writerow(())
			writer.writerow(assessment_header)
			writer.writerow([ 'Username', 'Username2', 'AssessmentCount' ])

			for username, user_count in users_vals.items():
				username2 = replace_username(username)
				user_row = [ username, username2, user_count ]
				writer.writerow(user_row)

		stream.flush()
		stream.seek(0)
		response.body_file = stream
		return response

@view_config(context=AnalyticsPathAdapter)
@view_defaults(route_name='objects.generic.traversal',
				renderer='rest',
				permission=ACT_NTI_ADMIN,
				request_method='POST',
				name='UpdateGeoLocations')
class UpdateGeoLocationsView(AbstractAuthenticatedView):
	"""
	Update locations missing a city/state in the db.
	"""

	def __call__(self):
		updated_count = update_missing_locations()
		logger.info('Updated %s missing geo locations', updated_count)
		return hexc.HTTPNoContent()

# XXX: Tests
class AbstractViewStatsView(AbstractAuthenticatedView):
	"""
	An abstract view_stats view that looks for `user` and
	`course` params, validating each.

	These views are currently only available as admin views,
	but may be opened up to instructors or others eventually.
	"""
	last_modified = None

	def _get_time_lengths(self, records):
		result = []
		last_mod = None
		for record in records or ():
			if record.Duration is not None:
				result.append(record.Duration)
			last_mod = record.timestamp if not last_mod else max(last_mod, record.timestamp)
		self.last_modified = last_mod
		return result

	def _build_time_stats(self, records):
		time_lengths = self._get_time_lengths(records)
		stats = get_time_stats(time_lengths)
		return stats

	def _get_kwargs(self):
		params = CaseInsensitiveDict(self.request.params)
		username = params.get('user') or params.get('username')
		result = {}
		if username is not None:
			user = User.get_user(username)
			if user is None:
				raise hexc.HTTPUnprocessableEntity(
									'Cannot find user %s' % username)
			result['user'] = user
		course_ntiid = params.get('course')
		if course_ntiid is not None:
			course = find_object_with_ntiid(course_ntiid)
			course = ICourseInstance(course, None)
			if course is None:
				raise hexc.HTTPUnprocessableEntity(
								'Cannot find course %s' % course_ntiid)
			result['course'] = course
		return result

	def __call__(self):
		result = LocatedExternalDict()
		kwargs = self._get_kwargs()
		records = self._get_records(**kwargs)
		result['Stats'] = self._build_time_stats(records)
		result[CLASS] = self.__class__.__name__
		result[LAST_MODIFIED] = self.last_modified
		return result

@view_config(context=IAssetRef)
@view_config(context=IContentUnit)
@view_config(context=IPresentationAsset)
@view_defaults(route_name='objects.generic.traversal',
			 	name=VIEW_STATS,
				renderer='rest',
				request_method='GET',
				permission=ACT_NTI_ADMIN)
class AssetViewStats(AbstractViewStatsView):

	def _get_context_ntiid(self):
		# XXX: Not sure if we need to try to aggregate everything here
		# (e.g. INTIRelatedWorkRefs pointing to content on disk).
		result = self.context.ntiid
		if IAssetRef.providedBy(self.context):
			result = self.context.target
		return result

	def _get_records(self, **kwargs):
		ntiid = self._get_context_ntiid()
		return get_resource_views_for_ntiid(ntiid, **kwargs)

@view_config(context=INTIVideo)
@view_config(context=INTIVideoRef)
@view_defaults(route_name='objects.generic.traversal',
			 	name=VIEW_STATS,
				renderer='rest',
				request_method='GET',
				permission=ACT_NTI_ADMIN)
class VideoViewStats(AbstractViewStatsView):

	def _get_context_ntiid(self):
		result = self.context.ntiid
		if INTIVideoRef.providedBy(self.context):
			result = self.context.target
		return result

	def _get_records(self, **kwargs):
		ntiid = self._get_context_ntiid()
		return get_video_views_for_ntiid(ntiid, **kwargs)

@view_config(route_name='objects.generic.traversal',
			 name=VIEW_STATS,
			 renderer='rest',
			 request_method='GET',
			 permission=ACT_NTI_ADMIN,
			 context=ITopic)
class TopicViewStats(AbstractViewStatsView):

	def _get_topic_records(self, topic, **kwargs):
		kwargs = dict(kwargs)
		kwargs['topic'] = topic
		return get_topic_views(**kwargs)

	def _get_records(self, **kwargs):
		return self._get_topic_records(self.context, **kwargs)

@view_config(route_name='objects.generic.traversal',
			 	name=VIEW_STATS,
			 	renderer='rest',
				request_method='GET',
				permission=ACT_NTI_ADMIN,
				context=IForum)
class ForumViewStats(TopicViewStats):
	"""
	For the contextual IForum, expose the view stats for
	each contained ITopic.
	"""

	def _get_stats(self, **kwargs):
		result = {}
		records = []
		for topic in self.context.values():
			topic_records = self._get_topic_records(topic, **kwargs)
			records.extend(topic_records)
			result[topic.title] = self._build_time_stats(topic_records)
		return result, records

	def __call__(self):
		result = LocatedExternalDict()
		kwargs = self._get_kwargs()
		stat_dict, all_records = self._get_stats(**kwargs)
		result['Stats'] = stat_dict
		result['AggregateForumStats'] = self._build_time_stats(all_records)
		result[CLASS] = self.__class__.__name__
		result[LAST_MODIFIED] = self.last_modified
		return result
