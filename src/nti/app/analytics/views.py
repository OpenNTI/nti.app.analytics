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
from nti.analytics.sessions import handle_sessions

from nti.analytics.resource_views import handle_events
from nti.analytics.interfaces import IBatchResourceEvents
from nti.analytics.interfaces import IAnalyticsSessions

from nti.dataserver import authorization as nauth

from nti.externalization import internalization

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
class ResolveAnalyticsSessions(AbstractAuthenticatedView, ModeledContentUploadRequestUtilsMixin):

	content_predicate = IAnalyticsSessions.providedBy

	def __call__(self):
		"""
		Will accept one or many IAnalyticsSession objects, which we will synchronously
		resolve the session_id for before returning.
		"""
		request = self.request
		user = request.remote_user

		external_input = self.readInput()
		factory = internalization.find_factory_for( external_input )
		sessions = factory()
		internalization.update_from_external_object( sessions, external_input )

		ip_addr = getattr( request, 'remote_addr' , None )
		user_agent = getattr( request, 'user_agent', None )

		handle_sessions( sessions.sessions, user, user_agent=user_agent, ip_addr=ip_addr )
		return sessions
