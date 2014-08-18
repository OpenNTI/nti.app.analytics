#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time

from zope import component
from zope import interface
from zope.location.interfaces import IContained
from zope.container import contained as zcontained
from zope.traversing.interfaces import IPathAdapter

from pyramid.view import view_config

from nti.analytics import QUEUE_NAMES
from nti.analytics import get_factory

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
	queue = get_job_queue()
	result = LocatedExternalDict()
	factory = get_factory()

	for name in queue_names:
		queue = factory.get_queue( name )
		result[ name ] = len(queue)
	return result

@view_config(route_name='objects.generic.traversal',
			 name='empty_queue',
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_MODERATE)
def empty_queue(request):
	# TODO Need name or 'ALL'; no assumptions
	logger.info( 'Emptying analytics processing queue' )
	queue = get_job_queue()
	now = time.time()
	done = queue.empty()
	result = LocatedExternalDict()
	elapsed = time.time() - now
	result['Elapsed'] = elapsed
	result['Total'] = done
	logger.info( 'Emptied analytics processing queue (size=%s) (time=%s)', done, elapsed )
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
		factory = internalization.find_factory_for(external_input)
		batch_events = factory()
		internalization.update_from_external_object(batch_events, external_input)

		# TODO These calls may be associated with only a single user.
		# If so, we can attempt to get the session.
		event_count = handle_events( batch_events )
		logger.info( 'Received batched analytic events (size=%s)', event_count )
		return event_count


