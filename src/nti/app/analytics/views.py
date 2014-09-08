#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time

import pyramid.httpexceptions as hexc

from zope import component
from zope import interface
from zope.location.interfaces import IContained
from zope.container import contained as zcontained
from zope.traversing.interfaces import IPathAdapter
from zope.schema.interfaces import ValidationError

from pyramid.view import view_config

from nti.utils.maps import CaseInsensitiveDict

from nti.analytics import QUEUE_NAMES
from nti.analytics import get_factory

from nti.analytics.sessions import handle_new_session
from nti.analytics.sessions import handle_end_session

from nti.analytics.resource_views import handle_events

from nti.analytics.interfaces import IBatchResourceEvents

from nti.dataserver import authorization as nauth
from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout

from nti.app.base.abstract_views import AbstractAuthenticatedView
from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.externalization import internalization
from nti.externalization.interfaces import LocatedExternalDict

from . import ANALYTICS
from . import BATCH_EVENTS
from . import ANALYTICS_SESSION
from . import END_ANALYTICS_SESSION

@interface.implementer(IPathAdapter, IContained)
class AnalyticsPathAdapter(zcontained.Contained):

	__name__ = ANALYTICS

	def __init__(self, context, request):
		self.context = context
		self.request = request
		self.__parent__ = context

_view_defaults = dict(route_name='objects.generic.traversal',
					  renderer='rest',
					  permission=nauth.ACT_READ,
					  context=AnalyticsPathAdapter,
					  request_method='GET')
_post_view_defaults = _view_defaults.copy()
_post_view_defaults['request_method'] = 'POST'

def _make_min_max_btree_range(search_term):
	min_inclusive = search_term # start here
	max_exclusive = search_term[0:-1] + unichr(ord(search_term[-1]) + 1)
	return min_inclusive, max_exclusive

def username_search(search_term):
	min_inclusive, max_exclusive = _make_min_max_btree_range(search_term)
	dataserver = component.getUtility(IDataserver)
	_users = IShardLayout(dataserver).users_folder
	usernames = list(_users.iterkeys(min_inclusive, max_exclusive, excludemax=True))
	return usernames

@view_config(route_name='objects.generic.traversal',
			 name='queue_info',
			 renderer='rest',
			 request_method='GET',
			 permission=nauth.ACT_MODERATE)
def queue_info(request):
	result = LocatedExternalDict()
	factory = get_factory()

	for name in QUEUE_NAMES:
		queue_info = LocatedExternalDict()
		result[ name ] = queue_info
		queue = factory.get_queue( name )
		queue_info[ 'queue_length' ] = len(queue)
		queue_info[ 'failed_length' ] = len(queue.get_failed_queue())

	return result

@view_config(route_name='objects.generic.traversal',
			 name='empty_queue',
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_MODERATE)
def empty_queue(request):
	result = LocatedExternalDict()
	factory = get_factory()
	queue_names = QUEUE_NAMES
	now = time.time()

	for name in queue_names:
		queue = factory.get_queue( name )
		queue_count = len( queue )
		queue.empty()

		fail_queue = queue.get_failed_queue()
		failed_count = len( fail_queue )
		fail_queue.empty()

		if queue_count or failed_count:
			logger.info( 	'Emptied analytics processing queue (%s) (count=%s) (fail_count=%s)',
							name, queue_count, failed_count )

		queue_stat = LocatedExternalDict()
		queue_stat['Total'] = queue_count
		queue_stat['Failed total'] = failed_count
		result[ name ] = queue_stat

	elapsed = time.time() - now
	logger.info( 'Emptied analytics processing queue (time=%s)', elapsed )
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
		events = external_input['events']
		total_count = len( events )
		malformed_count = 0

		for event in events:
			factory = internalization.find_factory_for(event)
			new_event = factory()
			try:
				internalization.update_from_external_object(new_event, event)
				batch_events.append( new_event )
			except ValidationError as e:
				# TODO Should we capture a more generic exception?
				# The app may resend events on error.
				logger.warn( 'Malformed events received (event=%s) (%s)', event, e )
				malformed_count += 1

		event_count = handle_events( batch_events )
		logger.info( 	'Received batched analytic events (count=%s) (total_count=%s) (malformed=%s)',
						event_count, total_count, malformed_count )
		return event_count


@view_config(route_name='objects.generic.traversal',
			 name=ANALYTICS_SESSION,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class AnalyticsSession( AbstractAuthenticatedView ):

	def __call__(self):
		request = self.request
		user = request.remote_user
		handle_new_session( user, request )
		return hexc.HTTPNoContent()

@view_config(route_name='objects.generic.traversal',
			 name=END_ANALYTICS_SESSION,
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_READ)
class EndAnalyticsSession( AbstractAuthenticatedView, ModeledContentUploadRequestUtilsMixin ):

	def __call__(self):
		vals = {}
		if self.request.body:
			values = self.readInput()
			vals = CaseInsensitiveDict( values )

		session_id = vals.get( 'session_id' )

		request = self.request
		user = request.remote_user
		handle_end_session( user, session_id )
		return hexc.HTTPNoContent()

