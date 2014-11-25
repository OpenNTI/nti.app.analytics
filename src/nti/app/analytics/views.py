#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope.schema.interfaces import ValidationError

from pyramid.view import view_config
from pyramid import httpexceptions as hexc

from nti.app.base.abstract_views import AbstractAuthenticatedView
from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.analytics.sessions import handle_new_session
from nti.analytics.sessions import handle_end_session
from nti.analytics.sessions import update_session

from nti.analytics.resource_views import handle_events
from nti.analytics.resource_views import get_progress_for_ntiid

from nti.analytics.interfaces import IBatchResourceEvents
from nti.analytics.interfaces import IAnalyticsSessions

from nti.contenttypes.courses.interfaces import ICourseOutlineContentNode

from nti.dataserver import authorization as nauth

from nti.externalization import internalization
from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.externalization import to_external_object

from nti.ntiids import ntiids

from . import BATCH_EVENTS
from . import ANALYTICS_SESSION
from . import END_ANALYTICS_SESSION
from . import ANALYTICS_SESSIONS

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

# This node is a ContentPackage
def recur_children_ntiid( node, accum ):
	#Get our embedded ntiids and recursively fetch our children's ntiids
	ntiid = node.ntiid
	accum.update( node.embeddedContainerNTIIDs )
	if ntiid:
		accum.add( ntiid )
	for n in node.children:
		recur_children_ntiid( n, accum )

@view_config(route_name='objects.generic.traversal',
			 renderer='rest',
			 context=ICourseOutlineContentNode,
			 request_method='GET',
			 permission=nauth.ACT_READ,
			 name="Progress" )
class CourseOutlineNodeProgress(AbstractAuthenticatedView, ModeledContentUploadRequestUtilsMixin):
	"""
	For the given content outline node, return the progress we have for the user
	on each ntiid within the content node.
	"""

	def __call__(self):
		user = self.request.remote_user
		# TODO Verify how expensive this is.
		# - I do not think we can do this efficiently (etag or lastMod) without
		# gathering all of the data we need. For this reason, maybe we need to send
		# progress updates over a socket.

		# TODO Do we update assignments/self-assess underneath this node?
		# - If not, we need another view to do so on-demand.

		# Do we want to check caching at the lesson level (harder to update, easier perhaps
		# with caching, also results in more efficient calls) or at the individual ntiid
		# level (easier to update, less efficient calls).

		# Could cache this in resolvers.py ( content_package -> ntiids ), if expensive.
		ntiid = self.context.ContentNTIID
		content_unit = ntiids.find_object_with_ntiid( ntiid )

		node_ntiids = set()
		recur_children_ntiid( content_unit, node_ntiids )
		result = LocatedExternalDict()
		result[StandardExternalFields.ITEMS] = item_dict = {}

		node_last_modified = None

		for node_ntiid in node_ntiids:
			# Can we distinguish between video and other?
			node_progress = get_progress_for_ntiid( user, node_ntiid )

			if node_progress:
				item_dict[node_ntiid] = to_external_object( node_progress )

				if 		not node_last_modified \
					or 	( 	node_progress.last_modified and \
							node_progress.last_modified > node_last_modified ):
					node_last_modified = node_progress.last_modified

		# Setting this will enable the rendered to return a 304, if needed.
		self.request.response.last_modified = node_last_modified
		return result
