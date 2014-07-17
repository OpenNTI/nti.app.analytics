#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
import transaction
import simplejson as json

from zope import component
from zope import interface
from zope.location.interfaces import IContained
from zope.container import contained as zcontained
from zope.traversing.interfaces import IPathAdapter

from pyramid.view import view_config

from nti.analytics import get_job_queue

from nti.analytics.utils import all_objects_iids

from nti.analytics.resource_views import handle_events

from nti.analytics.interfaces import IObjectProcessor
from nti.analytics.interfaces import IBatchResourceEvents

from nti.dataserver import authorization as nauth
from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout

from nti.app.base.abstract_views import AbstractAuthenticatedView
from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.externalization import internalization
from nti.externalization.interfaces import LocatedExternalDict

from nti.utils.maps import CaseInsensitiveDict

@interface.implementer(IPathAdapter, IContained)
class AnalyticsPathAdapter(zcontained.Contained):

	__name__ = 'analyticsdb'

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

def init( obj ):
	result = False
	for _, module in component.getUtilitiesFor(IObjectProcessor):
		result = module.init( obj ) or result
	return result

def init_db( usernames=() ):
	count = 0
	for _, obj in all_objects_iids(usernames):
		if init( obj ):
			count += 1
			if count % 10000 == 0:
				transaction.savepoint()
	return count

@view_config(route_name='objects.generic.traversal',
			 name='init_analytics_db',
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_MODERATE)
def init_analytics_db(request):
	values = json.loads(unicode(request.body, request.charset)) if request.body else {}
	values = CaseInsensitiveDict(values)
	# FIXME clean this up
	#usernames = values.get('usernames', values.get('username', None))
	usernames = 'josh.zuech@nextthought.com,student1'

	if usernames:
		usernames = usernames.split(',')
	else:
		usernames = ()

	now = time.time()
	total = init_db(usernames)
	elapsed = time.time() - now

	logger.info("Total objects processed %s(%s)", total, elapsed)

	result = LocatedExternalDict()
	result['Elapsed'] = elapsed
	result['Total'] = total
	return result

@view_config(route_name='objects.generic.traversal',
			 name='queue_info',
			 renderer='rest',
			 request_method='GET',
			 permission=nauth.ACT_MODERATE)
def queue_info(request):
	queue = get_job_queue()
	result = LocatedExternalDict()
	result['size'] = len(queue)
	return result

@view_config(route_name='objects.generic.traversal',
			 name='empty_queue',
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_MODERATE)
def empty_queue(request):
	queue = get_job_queue()
	now = time.time()
	done = queue.empty()
	result = LocatedExternalDict()
	result['Elapsed'] = time.time() - now
	result['Total'] = done
	return result


# TODO Permissioning?  These are batched, so I'm not sure what user we would have.
@view_config(route_name='objects.generic.traversal',
			 name='batch_events',
			 renderer='rest',
			 request_method='POST',
			 permission=nauth.ACT_MODERATE)
class BatchEvents(	AbstractAuthenticatedView,
					ModeledContentUploadRequestUtilsMixin ):

	content_predicate = IBatchResourceEvents.providedBy

	def _do_call(self):
		external_input = self.readInput()
		factory = internalization.find_factory_for(external_input)
		batch_events = factory()
		internalization.update_from_external_object(batch_events, external_input)

		# TODO If our events are batched, we won't have any session
		# information (because the user may be long gone).
		event_count = handle_events( batch_events )
		return event_count


