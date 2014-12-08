#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
from ZODB.interfaces import IBroken
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from datetime import datetime

from itertools import chain

from zope import component
from zope.event import notify
from zope.schema.interfaces import ValidationError

from pyramid.view import view_config
from pyramid import httpexceptions as hexc

from nti.app.base.abstract_views import AbstractAuthenticatedView
from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.analytics.model import delete_research_status
from nti.analytics.model import UserResearchStatusEvent

from nti.analytics.sessions import handle_new_session
from nti.analytics.sessions import handle_end_session
from nti.analytics.sessions import update_session

from nti.analytics.resolvers import recur_children_ntiid_for_unit
from nti.analytics.resolvers import get_course_by_container_id
from nti.analytics.resolvers import get_self_assessments_for_course
from nti.analytics.resolvers import get_assignments_for_course

from nti.analytics.resource_views import handle_events
from nti.analytics.resource_views import get_progress_for_ntiid

from nti.analytics.interfaces import IBatchResourceEvents
from nti.analytics.interfaces import IAnalyticsSessions
from nti.analytics.interfaces import IProgress
from nti.analytics.interfaces import IUserResearchStatus

from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode

from nti.dataserver import authorization as nauth
from nti.dataserver.interfaces import IUser

from nti.externalization import internalization
from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.externalization import to_external_object

from nti.ntiids import ntiids

from nti.utils.maps import CaseInsensitiveDict

from . import BATCH_EVENTS
from . import BATCH_EVENT_PARAMS
from . import ANALYTICS_SESSION
from . import END_ANALYTICS_SESSION
from . import ANALYTICS_SESSIONS

BATCH_EVENT_SIZE_NAME = 'RecommendedBatchEventsSize'
BATCH_EVENT_SIZE = 100
BATCH_EVENT_FREQUENCY_NAME = 'RecommendedBatchEventsSendFrequency'
# In seconds
BATCH_EVENT_FREQUENCY = 60

SET_RESEARCH_VIEW = 'SetUserResearch'

def _is_true(t):
	result = bool(t and str(t).lower() in ('1', 'y', 'yes', 't', 'true'))
	return result

@view_config(route_name='objects.generic.traversal',
			 name=BATCH_EVENTS,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class BatchEvents(	AbstractAuthenticatedView,
					ModeledContentUploadRequestUtilsMixin ):

	content_predicate = IBatchResourceEvents.providedBy

	def _do_call(self):
		external_input = self.readInput()
		# Ok, lets hand-internalize these objects one-by-one so that we
		# can exclude any malformed objects and process the proper events.
		batch_events = []
		malformed_count = 0
		events = external_input['events']
		total_count = len(events)

		for event in events:
			factory = internalization.find_factory_for(event)
			new_event = factory()
			try:
				internalization.update_from_external_object(new_event, event)
				batch_events.append( new_event )
			except ValidationError as e:
				# TODO Should we capture a more generic exception?
				# The app may resend events on error.
				logger.warn('Malformed events received (event=%s) (%s)', event, e)
				malformed_count += 1

		event_count = handle_events( batch_events )
		logger.info('Received batched analytic events (count=%s) (total_count=%s) (malformed=%s)',
					event_count, total_count, malformed_count )
		return event_count

@view_config(route_name='objects.generic.traversal',
			 name=BATCH_EVENT_PARAMS,
			 renderer='rest',
			 request_method='GET')
class BatchEventParams( AbstractAuthenticatedView ):

	def __call__(self):
		result = LocatedExternalDict()
		result[BATCH_EVENT_SIZE_NAME] = BATCH_EVENT_SIZE
		result[BATCH_EVENT_FREQUENCY_NAME] = BATCH_EVENT_FREQUENCY
		return result

@view_config(route_name='objects.generic.traversal',
			 name=ANALYTICS_SESSION,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class AnalyticsSession( AbstractAuthenticatedView ):

	def __call__(self):
		"""
		Create a new analytics session and place it in a cookie.
		"""
		request = self.request
		user = request.remote_user
		if user is not None:
			handle_new_session(user, request)
		return request.response

@view_config(route_name='objects.generic.traversal',
			 name=END_ANALYTICS_SESSION,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class EndAnalyticsSession(AbstractAuthenticatedView, ModeledContentUploadRequestUtilsMixin):

	def __call__(self):
		"""
		End the current analytics session.
		"""
		request = self.request
		user = request.remote_user
		handle_end_session( user, request )
		return hexc.HTTPNoContent()

@view_config(route_name='objects.generic.traversal',
			 name=ANALYTICS_SESSIONS,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class UpdateAnalyticsSessions(AbstractAuthenticatedView, ModeledContentUploadRequestUtilsMixin):

	content_predicate = IAnalyticsSessions.providedBy

	def __call__(self):
		"""
		Will accept one or many IAnalyticsSession objects, which we will synchronously
		resolve the session_id for before returning.  If there is already a session_id,
		we'll update the state (end_time) of the given session object.
		"""
		request = self.request
		user = request.remote_user

		external_input = self.readInput()
		factory = internalization.find_factory_for( external_input )
		sessions = factory()
		internalization.update_from_external_object( sessions, external_input )

		ip_addr = getattr( request, 'remote_addr' , None )
		user_agent = getattr( request, 'user_agent', None )

		results = []
		for session in sessions.sessions:
			try:
				result = update_session( session, user, user_agent=user_agent, ip_addr=ip_addr )
				results.append( result )
			except ValueError as e:
				# Append invalid session information.  We still return a 200 though.
				val = dict()
				val['Error'] = e.message
				results.append( val )
		return results

@view_config(route_name='objects.generic.traversal',
			 renderer='rest',
			 context=ICourseOutlineContentNode,
			 request_method='GET',
			 permission=nauth.ACT_READ,
			 name="Progress" )
class CourseOutlineNodeProgress(AbstractAuthenticatedView, ModeledContentUploadRequestUtilsMixin):
	"""
	For the given content outline node, return the progress we have for the user
	on each ntiid within the content node.  This will include self-assessments and
	assignments for the course.  On return, the 'LastModified' header will be set, allowing
	the client to specify the 'If-Modified-Since' header for future requests.  A 304 will be
	returned if there is the results have not changed.
	"""

	def __call__(self):
		# - Locally, this is quick. ~1s (much less when cached) to get
		# ntiids under node; ~.05s to get empty resource set.  Bumps up to ~.3s
		# once the user starts accumulating events.
		# If building the course ntiid cache (in analytics:resolvers.py), this call
		# is extremely slow (~25s locally with 15 courses).  This is a one-time hit.

		# TODO If these content nodes can be re-used in other courses, we should
		# probably accept a course param to distinguish progress between courses.
		user = self.getRemoteUser()

		ntiid = self.context.ContentNTIID
		content_unit = ntiids.find_object_with_ntiid( ntiid )
		node_ntiids = recur_children_ntiid_for_unit( content_unit )

		result = LocatedExternalDict()
		result[StandardExternalFields.ITEMS] = item_dict = {}

		node_last_modified = None
		def _get_last_mod( progress, max_last_mod ):
			result = max_last_mod

			if 		not max_last_mod \
				or 	( 	progress.last_modified and \
						progress.last_modified > max_last_mod ):
				result = progress.last_modified
			return result

		# Get progress for resource/videos
		for node_ntiid in node_ntiids:
			# Can we distinguish between video and other?
			node_progress = get_progress_for_ntiid( user, node_ntiid )

			if node_progress:
				item_dict[node_ntiid] = to_external_object( node_progress )
				node_last_modified = _get_last_mod( node_progress, node_last_modified )

		# Get progress for self-assessments and assignments
		# Expensive and slow if we're building our cache.
		try:
			course = get_course_by_container_id( ntiid )
		except TypeError:
			logger.warn( 'No course found for ntiid; cannot return progress (%s)', ntiid )
			course = None

		if course is not None:
			# Gathering all assignments/self-assessments for course.
			# May be cheaper than finding just for our unit.
			self_assessments = get_self_assessments_for_course( course )
			assignments = get_assignments_for_course( course )

			for assessment_ntiid in chain( assignments, self_assessments ):
				assessment_object = ntiids.find_object_with_ntiid( assessment_ntiid )
				progress = component.queryMultiAdapter( (user, assessment_object), IProgress )
				if progress:
					item_dict[progress.progress_id] = to_external_object( progress )
					node_last_modified = _get_last_mod( progress, node_last_modified )

		# TODO Summarize progress for node. This might be difficult, unless we assume
		# that every child ntiid contributes towards progress.  If we need to filter
		# out certain types of ntiids, that might be tough.

		# Setting this will enable the renderer to return a 304, if needed.
		self.request.response.last_modified = node_last_modified
		return result

@view_config( route_name='objects.generic.traversal',
			  renderer='rest',
			  context=IUser,
			  request_method='POST',
			  name=SET_RESEARCH_VIEW)
class UserResearchStudyView(AbstractAuthenticatedView,
							ModeledContentUploadRequestUtilsMixin ):
	"""
	Updates a user's research status.
	"""

	def __call__(self):
		values = CaseInsensitiveDict(self.readInput())
		allow_research = values.get('allow_research')
		allow_research = _is_true(allow_research)
		user = self.request.context

		research_status = IUserResearchStatus(user)
		if IBroken.providedBy(research_status):
			delete_research_status(user)
			research_status = IUserResearchStatus(user)
		research_status.updateLastMod()
		research_status.modified = datetime.utcnow()
		research_status.allow_research = allow_research

		logger.info('Setting research status for user (user=%s) (allow_research=%s)',
					user.username, allow_research )

		notify(UserResearchStatusEvent(user, allow_research))
		return hexc.HTTPNoContent()
